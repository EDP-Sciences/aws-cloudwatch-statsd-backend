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

CloudwatchBackend.prototype.processKey = function(key) {
	var parts = key.split(/[\.]/);
    var appName = parts[0];
    var hostName = parts[1];
    var metricName = parts.splice(2).join(".");

	return {
        appName: appName || 'unknown',
        hostName: hostName || 'unknown',
		metricName: metricName || key || 'unknown'
	};
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

  for (key in counters) {
	  if (key.indexOf('statsd.') == 0)
		  continue;

    var names = this.processKey(key);

    this.cloudwatch.putMetricData({
      MetricData : [{
        MetricName: names.metricName,
        Dimensions:  [
          {Name: 'application', Value: names.appName},
          {Name: 'hostname', Value: names.hostName}
        ],
        Unit: 'Count',
        Timestamp: new Date(timestamp*1000).toISOString(),
        Value: counters[key]
      }],
      Namespace  : "WritingStudio metrics"
    }, callback);
  }

  for (key in timers) {
    if (timers[key].length > 0) {

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

      names = this.processKey(key);

      this.cloudwatch.putMetricData({
        MetricData : [{
          MetricName: names.metricName,
          Dimensions:  [
            {Name: 'application', Value: names.appName},
            {Name: 'hostname', Value: names.hostName}
          ],
          Unit : 'Milliseconds',
          Timestamp: new Date(timestamp*1000).toISOString(),
          StatisticValues: {
            Minimum: min,
            Maximum: max,
            Sum: sum,
            SampleCount: count
          }
        }],
        Namespace  : "WritingStudio metrics"
      }, callback);
   	}
  }

  for (key in gauges) {

    names = this.processKey(key);

    this.cloudwatch.putMetricData({
      MetricData : [{
        MetricName: names.metricName,
        Dimensions:  [
          {Name: 'application', Value: names.appName},
          {Name: 'hostname', Value: names.hostName}
        ],
        Unit : 'None',
        Timestamp: new Date(timestamp*1000).toISOString(),
        Value : gauges[key]
      }],
      Namespace: "WritingStudio metrics"
    }, callback);
  }

  for (key in sets) {

    names = this.processKey(key);

    this.cloudwatch.putMetricData({
      MetricData : [{
        MetricName: names.metricName,
        Dimensions:  [
          {Name: 'application', Value: names.appName},
          {Name: 'hostname', Value: names.hostName}
        ],
        Unit : 'None',
        Timestamp: new Date(timestamp*1000).toISOString(),
        Value : sets[key].values().length
      }],
      Namespace: "WritingStudio metrics"
    }, callback);
  }
};

exports.init = function(startupTime, config, events) {
  var instance = new CloudwatchBackend(startupTime, config, events);
  return true;
};
