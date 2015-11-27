var util = require('util');
var AWS = require('aws-sdk');

function CloudwatchBackend(startupTime, config, emitter){
  var self = this;

  this.config = config.cloudwatch || {};
  AWS.config = this.config;

  function setEmitter() {
    self.cloudwatch = new AWS.CloudWatch(AWS.config);
    emitter.on('flush', function(timestamp, metrics) { self.flush(timestamp, metrics); });
  }

  // if iamRole is set attempt to fetch credentials from the Metadata Service
  if(this.config.iamRole) {
    if (this.config.iamRole == 'any') {
      // If the iamRole is set to any, then attempt to fetch any available credentials
      var ms = new AWS.EC2MetadataCredentials();
      ms.refresh(function(err) {
        if(err) { console.log('Failed to fetch IAM role credentials: '+err); }
        AWS.config.credentials = ms;
        setEmitter();
      });
    } else {
      // however if it's set to specify a role, query it specifically.
      ms = new AWS.MetadataService();
      ms.request('/latest/meta-data/iam/security-credentials/'+this.config.iamRole, function(err, rdata) {
        var data = JSON.parse(rdata);

        if(err) { console.log('Failed to fetch IAM role credentials: '+err); }
        AWS.config.credentials = new AWS.Credentials(data.AccessKeyId, data.SecretAccessKey, data.Token);
        setEmitter();
      });
    }
  } else {
    setEmitter();
  }
}

var processKey = function(key) {
    var parts = key.split('.');
    var appName = parts[0];
    var hostName = parts[1];
    var queryMethod, statusCode;
    if (parts.length > 2 && parseInt(parts[parts.length - 1])) {
      var sub = parts.splice(parts.length - 2);
      queryMethod = sub[0];
      statusCode = sub[1];
    }
    var metricName = parts.splice(2).join(".");

    var response = {
        metricName: metricName || key || 'unknown',
        dimensions: [
          {Name: 'application', 'Value': appName || 'unknown'},
          {Name: 'host', 'Value': hostName || 'unknown'}
        ]
    };
    if (queryMethod) {
      response.dimensions.push({Name: 'query', Value: queryMethod});
      response.dimensions.push({Name: 'status', Value: statusCode});
    }
  return response;
};

CloudwatchBackend.prototype.put_metric_data = function (key, value, unit, stats) {
  var names = processKey(key);

  var data = {
    MetricName: names.metricName,
    Dimensions:  names.dimensions,
    Unit: unit || 'None',
    Timestamp: new Date(timestamp*1000).toISOString()
  };

  if (stats) {
    data.StatisticValues = stats;
  } else {
    data.Value = value;
  }

  this.cloudwatch.putMetricData({
    MetricData : [data],
    Namespace  : "WritingStudio"
  }, callback);
};


CloudwatchBackend.prototype.flush = function(timestamp, metrics) {

  console.log('Flushing metrics at ' + new Date(timestamp*1000).toISOString());

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;

  var callback = function (err) {
    if (err)
      console.log(util.inspect(err));
  };


  for (var key in counters) {
      if (counters.hasOwnProperty(key)) {
        if (key.indexOf('statsd.') == 0)
          continue;

        this.put_metric_data(key, counters[key], 'Count');
      }

  }

  for (key in timers) {
    if (timers.hasOwnProperty(key) && timers[key].length > 0) {

      var values = timers[key].sort(function (a,b) { return a-b; });
      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var cumulativeValues = [min];
      for (var i = 1; i < count; i++) {
        cumulativeValues.push(values[i] + cumulativeValues[i-1]);
      }

      var sum = min;
      var mean = min;

      sum = cumulativeValues[count-1];
      mean = sum / count;

      this.put_metric_data(key, null, 'Milliseconds', {
        Minimum: min,
        Maximum: max,
        Sum: sum,
        SampleCount: count
      });
    }
  }

  for (key in gauges) {
    if (gauges.hasOwnProperty(key)) {
      this.put_metric_data(key, gauges[key]);
    }
  }

  for (key in sets) {
    if (sets.hasOwnProperty(key)) {
      this.put_metric_data(key, sets[key].values().length);
    }
  }
};

exports.init = function(startupTime, config, events) {
  var instance = new CloudwatchBackend(startupTime, config, events);
  return true;
};
