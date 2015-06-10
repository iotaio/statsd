/*
 * Send stats to iota.io (http://www.iota.io)
 *
 * This file was copied and modified from:
 *     https://github.com/hostedgraphite/statsdplugin
 * For more inforation on the original source see: http://www.hostedgraphite.com
 *
 * To enable this backend, include './backends/iotaio' in the backends
 * configuration array:
 *
 *   backends: ['./backends/iotaio']
 *
 * This backend supports the following config options:
 *
 *   iotaioApiKey: An iotaio API key.
 *   iotaioHost: The Iota.io url to store data to.
 *       eg. ts.{customer_id}.iota.io
 *   iotaioPort: The port to connect to iota.io on
 *       eg. 80 or 443
 *   iotaioTag: The parent tag to store data with.
 *
 *****************************************************
 * THIS MODULE IS IN BETA STATE AND UNDER DEVELOPMENT.
 *****************************************************
 */

var net = require('net'),
util = require('util'),
http = require('http');

var debug;
var flushInterval;
var ApiKey;
var storageHost;
var storagePort;
var dataTag;
var port;

var iotaioStats = {};

var post_stats = function iotaio_post_stats(statString) {
  var options = {
    hostname: storageHost,
    port: storagePort,
    path: '/',
    method: 'POST',
    headers: {
        'X-Auth-Token': ApiKey,
        'Content-Length': statString.length,
        'Content-Type': 'application/json'
    }
  };

  if (debug) {
      debug_log("Starting " + options.method + " connection to: " + options.hostname + ":" + options.port);
      debug_log("Sending headers: " + JSON.stringify(options.headers));
  }

  var req = http.request(options);

  req.on('response', function (res) {
    if (debug) {
        debug_log("HTTP reponse code: " + res.statusCode);
        debug_log("HTTP response: " + res.statusMessage);

        res.on('data', function(chunk) {
            debug_log("BODY: " + chunk);
        });
    }

    if (res.statusCode == 202) {
       iotaioStats.last_flush = Math.round(new Date().getTime() / 1000);
    }
  });

  req.on('error', function(e, i) {
    iotaioStats.last_exception = Math.round(new Date().getTime() / 1000);

    if (debug) {
       debug_log('HTTP Error: ' + e.message);
   }
  });

  req.write(statString);
  req.end();
}

var flush_stats = function iotaio_flush(ts, metrics) {
  var statString = '';
  var numStats = 0;
  var key;

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var pctThreshold = metrics.pctThreshold;

  var iotaioMetrics = [];
  var iotaioValues = [];
  var iotaioTimestamps = [];

  for (key in counters) {
    var value = counters[key];
    var valuePerSecond = value / (flushInterval / 1000); // calculate "per second" rate

    iotaioMetrics.push('stats.'        + key);
    iotaioValues.push(valuePerSecond);
    iotaioTimestamps.push(ts);

    iotaioMetrics.push('stats.counts.' + key);
    iotaioValues.push(value);
    iotaioTimestamps.push(ts);

    numStats += 1;
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
      var maxAtThreshold = max;

      var message = "";

      var key2;

      for (key2 in pctThreshold) {
        var pct = pctThreshold[key2];
        if (count > 1) {
          var thresholdIndex = Math.round(((100 - pct) / 100) * count);
          var numInThreshold = count - thresholdIndex;

          maxAtThreshold = values[numInThreshold - 1];
          sum = cumulativeValues[numInThreshold - 1];
          mean = sum / numInThreshold;
        }

        var clean_pct = '' + pct;

        iotaioMetrics.push('stats.timers.' + key + '.mean.'  + clean_pct);
        iotaioValues.push(mean);
        iotaioTimestamps.push(ts);

        iotaioMetrics.push('stats.timers.' + key + '.upper.' + clean_pct);
        iotaioValues.push(maxAtThreshold);
        iotaioTimestamps.push(ts);

        iotaioMetrics.push('stats.timers.' + key + '.sum.' + clean_pct);
        iotaioValues.push(sum);
        iotaioTimestamps.push(ts);
      }

      sum = cumulativeValues[count-1];
      mean = sum / count;

      iotaioMetrics.push('stats.timers.' + key + '.upper');
      iotaioValues.push(max);
      iotaioTimestamps.push(ts);

      iotaioMetrics.push('stats.timers.' + key + '.lower');
      iotaioValues.push(min);
      iotaioTimestamps.push(ts);

      iotaioMetrics.push('stats_timers.' + key + '.count')
      iotaioValues.push(count);
      iotaioTimestamps.push(ts);

      iotaioMetrics.push('stats_timers.' + key + '.sum')
      iotaioValues.push(sum);
      iotaioTimestamps.push(ts);

      iotaioMetrics.push('stats_timers.' + key + '.mean')
      iotaioValues.push(mean);
      iotaioTimestamps.push(ts);

      numStats += 1;
    }
  }

  for (key in gauges) {
    iotaioMetrics.push('stats.gauges.' + key);
    iotaioValues.push(gauges[key]);
    iotaioTimestamps.push(ts);

    numStats += 1;
  }

   iotaioMetrics.push('statsd.numStats');
   iotaioValues.push(numStats);
   iotaioTimestamps.push(ts);

   iotaio_data = {}
   iotaio_data['metrics'] = iotaioMetrics;
   iotaio_data['values'] = iotaioValues;
   iotaio_data['timestamps'] = iotaioTimestamps;
   iotaio_data['name'] = dataTag;

   json_data = "[ " + JSON.stringify(iotaio_data) + " ]";

   if (debug) {
       debug_log("JSON data: " + json_data);
   }

   post_stats(json_data);
};

var backend_status = function iotaio_status(writeCb) {
  for (stat in iotaioStats) {
    writeCb(null, 'iotaio', stat, iotaioStats[stat]);
  }
};

var debug_log = function debug_logger(message) {
    if (debug) {
        util.log('[iotaio] (DEBUG) ' + message)
    }
}

exports.init = function iotaio_init(startup_time, config, events) {
  debug = config.debug;

  ApiKey = config.iotaioApiKey;
  storageHost = config.iotaioHost;
  storagePort = config.iotaioPort;
  dataTag = config.iotaioTag;

  iotaioStats.last_flush = startup_time;
  iotaioStats.last_exception = startup_time;

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
