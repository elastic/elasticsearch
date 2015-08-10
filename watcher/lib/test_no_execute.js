var lib = require('requirefrom')('lib');
var executeWatcher = lib('execute_watcher');
var expect = require('expect.js');
module.exports = function testNoExecute(options, message, generateRawData) {
  describe(message, function () {
    var response;
    beforeEach(function () {
      this.timeout(5000);
      var rawData = generateRawData();
      var fixture = {
        indexPattern: options.indexPattern,
        type: options.type,
        dateField: 'timestamp',
        rawData: rawData
      };
      return executeWatcher(options.watcher, fixture).then(function (resp) {
        response = resp;
        if (process.env.DEBUG) console.log(JSON.stringify(resp, null, ' '));
        return resp;
      });
    });
    it('should not meet the script condition', function () {
      expect(response.state).to.be('execution_not_needed');
      expect(response.execution_result.condition.script.met).to.be(false);
    });
  });
};

