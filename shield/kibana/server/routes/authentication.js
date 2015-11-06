const Boom = require('boom');
const Joi = require('joi');

module.exports = (server, uiExports) => {
  const login = uiExports.apps.byId.login;
  const isValidUser = require('../lib/is_valid_user')(server.plugins.elasticsearch.client);

  server.route({
    method: 'GET',
    path: '/login',
    handler(request, reply) {
      return reply.renderApp(login);
    },
    config: {
      auth: false
    }
  });

  server.route({
    method: 'POST',
    path: '/login',
    handler(request, reply) {
      return isValidUser(request.payload.username, request.payload.password).then(() => {
        request.auth.session.set({username: request.payload.username, password: request.payload.password});
        return reply({
          statusCode: 200,
          payload: 'success'
        });
      }, (error) => {
        request.auth.session.clear();
        return reply(Boom.unauthorized(error));
      })
    },
    config: {
      auth: false,
      validate: {
        payload: {
          username: Joi.string().required(),
          password: Joi.string().required()
        }
      }
    }
  });

  server.route({
    method: 'GET',
    path: '/app/logout', // TODO: Change to /logout
    handler(request, reply) {
      request.auth.session.clear();
      return reply.redirect('/');
    }
  });
};