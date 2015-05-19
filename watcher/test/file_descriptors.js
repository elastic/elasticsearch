var lib = require('requirefrom')('lib');
var expect = require('expect.js');
var moment = require('moment');
var executeWatcher = lib('execute_watcher');
var client = lib('client');
var indexPattern = '[.marvel-]YYYY.MM.DD';
lib('setup_es');
lib('setup_smtp_server');

describe('Marvel Watchers', function () {
  describe('File Descriptors', function () {

    describe('above 80%', function () {
      var response;
      beforeEach(function () {
        this.timeout(5000);
        var fixture = {
          indexPattern: indexPattern,
          type: 'node_stats',
          duration: moment.duration(5, 's'),
          startDate: moment.utc().subtract(5, 'm'),
          data: [
            ['node.name', 'process.open_file_descriptors'],
            ['node-01', Math.round(65535*0.75)],
            ['node-02', Math.round(65535*0.81)],
            ['node-03', Math.round(65535*0.93)]
          ]
        };
        return executeWatcher('file_descriptors', fixture).then(function (resp) {
          response = resp;
          return resp;
        });
      });

      it('should meet the script condition', function () {
        expect(response.state).to.be('executed');
        expect(response.execution_result.condition.script.met).to.be(true);
      });

      it('should send an email with multiple hosts', function () {
        expect(this.mailbox).to.have.length(1);
        var message = this.mailbox[0];
        expect(message.text).to.contain('"node-02" - File Descriptors is at ' + Math.round(65535*0.81) + '.0 ('+ Math.round(((65535*0.81)/65535)*100) + '%)');
        expect(message.text).to.contain('"node-03" - File Descriptors is at ' + Math.round(65535*0.93) + '.0 ('+ Math.round(((65535*0.93)/65535)*100) + '%)');
      });

    });

    describe('below 80%', function () {
      var response;
      beforeEach(function () {
        var self = this;
        this.timeout(5000);
        var fixture = {
          indexPattern: indexPattern,
          type: 'node_stats',
          duration: moment.duration(5, 's'),
          startDate: moment.utc().subtract(5, 'm'),
          data: [
            ['node.name', 'process.open_file_descriptors'],
            ['node-01', Math.round(65535*0.05)],
            ['node-02', Math.round(65535*0.30)],
            ['node-03', Math.round(65535*0.23)]
          ]
        };
        return executeWatcher('file_descriptors', fixture).then(function (resp) {
          response = resp;
          return resp;
        });
      });

      it('should not send an email', function () {
        expect(response.state).to.be('execution_not_needed');
        expect(response.execution_result.condition.script.met).to.be(false);
        expect(this.mailbox).to.have.length(0);
      });

    });

  });
});

