var client = require('./client');
var template = require('./template.json');

module.exports = function () {
  return client.indices.putTemplate({
    body: template,
    name: 'marvel'
  });
};


