var Promise = require('bluebird');
var request = require('request');
var path = require('path');

module.exports = function (name) {
  var watch = require(path.join(__dirname, '..', 'watches', name + '.json'));
  var options = {
    method: 'PUT',
    url: 'http://localhost:9800/_watcher/watch/' + name,
    json: true,
    body: watch
  };
  return new Promise(function (resolve, reject) {
    request(options, function (err, resp, body) {
      if (err) return reject(err);
      if (resp.statusCode <= 201) resolve({ resp: resp, body: body });
      var message = options.url + ' responded with ' + resp.statusCode;
      var error = new Error(body.error || message);
      error.body = body;
      error.resp = resp;
      error.code = resp.statusCode;
      error.options = options;
      reject(error);
    });
  });
};
