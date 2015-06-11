var Promise = require('bluebird');
var SMTPServer = require('smtp-server').SMTPServer;
var MailParser = require('mailparser').MailParser;
var acceptAny = function (address, session, done) {
  return done();
};

var mailbox = [];

function startSMTP() {
  return new Promise(function (resolve, reject) {
    var server = new SMTPServer({
      logger: false,
      disabledCommands: ['STARTTLS'],
      onAuth: function (auth, session, done) {
        done(null, { user: 1 });
      },
      onMailFrom: acceptAny,
      onRcptTo: acceptAny,
      onData: function (stream, session, done) {
        var mailparser = new MailParser();
        mailparser.on('end', function (mailObj) {
          mailbox.push(mailObj);
          done();
        });
        stream.pipe(mailparser);
      }
    });

    server.listen(5555, function (err) {
      if (err) return reject(err);
      after(function (done) {
        server.close(done);
      });
      resolve(server);
    });
  });
}

before(function () {
  var self = this;
  return startSMTP().then(function (server) {
    this.smtp = server;
    return server;
  });
});

beforeEach(function () {
  this.mailbox = mailbox;
});

afterEach(function () {
  mailbox = [];
});

module.exports = mailbox;
