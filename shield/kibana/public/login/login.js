require('plugins/security/login/login.less');

const kibanaLogoUrl = require('ui/images/kibana-transparent-white.svg');

require('ui/chrome')
  .setVisible(false)
  .setRootTemplate(require('plugins/security/login/login.html'))
  .setRootController('login', ($http) => {
    var login = {
      loading: false,
      kibanaLogoUrl
    };

    login.submit = (username, password) => {
      login.loading = true;

      $http.post('/login', {
        username: username,
        password: password
      }).then(
        (response) => window.location.href = '/', // TODO: Redirect more intelligently
        (error) => login.error = true
      ).finally(() => login.loading = false);
    };

    return login;
  });