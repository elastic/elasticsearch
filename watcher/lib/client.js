var Client = require('elasticsearch').Client;
module.exports = new Client({
  host: 'http://localhost:9800'
});
