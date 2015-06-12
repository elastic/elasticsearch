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
            ['node.name', 'indices.fielddata.memory_size_in_bytes'],
            ['node-01', 81000],
            ['node-02', 70000],
            ['node-03', 90000]
          ]
        };
        return executeWatcher('fielddata', fixture).then(function (resp) {
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
        expect(message.text).to.contain('"node-01" - Fielddata utilization is at 81000.0 bytes (81%)');
        expect(message.text).to.contain('"node-03" - Fielddata utilization is at 90000.0 bytes (90%)');
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
            ['node.name', 'indices.fielddata.memory_size_in_bytes'],
            ['node-01', 12039],
            ['node-02', 54393],
            ['node-03', 20302]
          ]
        };
        return executeWatcher('fielddata', fixture).then(function (resp) {
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

