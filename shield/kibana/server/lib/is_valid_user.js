const getAuthHeader = require('./get_auth_header');

module.exports = (client) => (username, password) => client.info({
  headers: getAuthHeader(username, password)
});