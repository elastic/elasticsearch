var Promise = require('bluebird');
var request = require('request');
var injectStats = require('./inject_stats');
var loadWatcher = require('./load_watcher');

module.exports = function (watcher, fixture) {

  return injectStats(fixture).then(function () {
    return loadWatcher(watcher);
  }).then(function () {
    return new Promise(function (resolve, reject) {
      var options = {
        method: 'POST',
        url: 'http://localhost:9800/_watcher/watch/' + watcher + '/_execute',
        json: true,
        body: {
          trigger_event: {
            schedule: {
              scheduled_time: 'now',
              triggered_time: 'now'
            }
          }
        }
      };

      request(options, function (err, resp, body) {
        if (err) return reject(err);
        if (resp.statusCode === 200) return resolve(body);
        var message = options.url + ' responed with ' + resp.statusCode;
        var error = new Error(body.error || message);
        error.body = body;
        error.resp = resp;
        error.code = resp.statusCode;
        error.options = options;
        reject(error);
      });
    });
  });

};
