var _ = require('lodash');
var lib = require('requirefrom')('lib');
var expect = require('expect.js');
var moment = require('moment');
var executeWatcher = lib('execute_watcher');
var options = {
  indexPattern: '[.marvel-]YYYY.MM.DD',
  type: 'cluster_stats',
  watcher: 'cluster_status'
};
var testNoExecute = lib('test_no_execute').bind(null, options);
var client = lib('client');
lib('setup_es');
lib('setup_smtp_server');

describe('Marvel Watchers', function () {
  describe('Cluster Status', function () {

    describe('Red for 60 seconds', function () {
      var response;
      beforeEach(function () {
        this.timeout(5000);
        var workingDate = moment.utc();
        var rawData = _.times(12, function () {
          return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'red' };
        });
        var fixture = {
          indexPattern: '[.marvel-]YYYY.MM.DD',
          type: 'cluster_stats',
          dateField: 'timestamp',
          rawData: rawData
        };
        return executeWatcher('cluster_status', fixture).then(function (resp) {
          response = resp;
          if (process.env.DEBUG) console.log(JSON.stringify(resp, null, ' '));
          return resp;
        });
      });

      it('should meet the script condition', function () {
        expect(response.state).to.be('executed');
        expect(response.execution_result.condition.script.met).to.be(true);
      });

      it('should send an email', function () {
        expect(this.mailbox).to.have.length(1);
        var message = this.mailbox[0];
        expect(message.subject).to.contain('Watcher Notification - Cluster has been RED for the last 60 seconds');
        expect(message.text).to.contain('Your cluster has been red for the last 60 seconds.');
      });

    });

    testNoExecute('Red for 55 then Yellow for 60 seconds', function () {
      var workingDate = moment.utc();
      var rawData = _.times(11, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'red' };
      });
      rawData.concat(_.times(12, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'yellow' };
      }));
      return rawData;
    });

    testNoExecute('Red for 30 then Yellow for 60 seconds', function () {
      var workingDate = moment.utc();
      var rawData = _.times(6, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'red' };
      });
      rawData.concat(_.times(12, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'yellow' };
      }));
      return rawData;
    });

    testNoExecute('Red for 5 Yellow for 10 Red for 10 Green for 60', function () {
      var workingDate = moment.utc();
      var rawData = _.times(1, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'red' };
      });
      rawData.concat(_.times(2, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'yellow' };
      }));
      rawData.concat(_.times(2, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'red' };
      }));
      rawData.concat(_.times(12, function () {
        return { 'timestamp': workingDate.subtract(5, 's').format(), status: 'green' };
      }));
      return rawData;
    });

  });
});

