const _ = require('lodash');
const hapiAuthCookie = require('hapi-auth-cookie');
const getAuthHeader = require('./server/lib/get_auth_header');

module.exports = (kibana) => new kibana.Plugin({
  name: 'security',
  require: ['elasticsearch'],

  config(Joi) {
    return Joi.object({
      enabled: Joi.boolean().default(true),
      encryptionKey: Joi.string().default('secret'),
      sessionTimeout: Joi.number().default(30 * 60 * 1000)
    }).default()
  },

  uiExports: {
    apps: [{
      id: 'login',
      title: 'Login',
      main: 'plugins/security/login',
      hidden: true,
      autoload: kibana.autoload.styles
    }, {
      id: 'logout',
      title: 'Logout',
      main: 'plugins/security/login/logout',
      hidden: false,
      autoload: kibana.autoload.styles
    }]
  },

  init(server, options) {
    const isValidUser = require('./server/lib/is_valid_user')(server.plugins.elasticsearch.client);
    const config = server.config();

    server.register(hapiAuthCookie, (error) => {
      if (error != null) throw error;

      server.auth.strategy('session', 'cookie', 'required', {
        cookie: 'sid',
        password: config.get('security.encryptionKey'),
        ttl: config.get('security.sessionTimeout'),
        clearInvalid: true,
        keepAlive: true,
        isSecure: false, // TODO: Remove this
        redirectTo: '/login',
        validateFunc(request, session, callback) {
          const {username, password} = session;

          return isValidUser(username, password).then(() => {
            _.assign(request.headers, getAuthHeader(username, password));
            return callback(null, true);
          }, (error) => {
            return callback(error, false);
          });
        }
      });
    });

    require('./server/routes/authentication')(server, this);
  }
});