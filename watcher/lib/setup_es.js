var path = require('path');
var Promise = require('bluebird');
var libesvm = require('libesvm');
var createTemplate = require('./create_template');

function startEs() {
  var options = {
    version: '1.5.2',
    directory: path.join(__dirname, '..', 'esvm'),
    purge: true,
    plugins: [
      'elasticsearch/watcher/1.0.0-Beta1-5',
      'elasticsearch/license/latest'
    ],
    config: {
      'script.groovy.sandbox.enabled': true,
      'cluster.name': 'test',
      'network.host': '127.0.0.1',
      'http.port': 9800,
      'watcher.actions.email.service.account': {
        'local': {
          'email_defaults.from': 'admin@example.com',
          'smtp': {
            'host': 'localhost',
            'user': 'test',
            'password': 'test',
            'port': 5555
          }
        }
      }

    }
  };
  var cluster = libesvm.createCluster(options);
  cluster.on('log', function (log) {
    if (log.type === 'progress') return;
    if (process.env.DEBUG) console.log('%s %s %s %s', log.level, log.node, log.type, log.message);
  });
  return cluster.install()
  .then(function () {
    return cluster.installPlugins();
  })
  .then(function () {
    return cluster.start();
  })
  .then(function () {
    after(function () {
      this.timeout(60000);
      return cluster.shutdown();
    });
    return cluster;
  });
}

before(function () {
  var self = this;
  this.timeout(60000);
  return new Promise(function (resolve, reject) {
    startEs().then(function (cluster) {
      self.cluster = cluster;
      cluster.on('log', function (log) {
        if (/watch service has started/.test(log.message)) {
          createTemplate().then(function () {
            resolve(cluster);
          });
        }
      });
    }).catch(reject);
  });
});

