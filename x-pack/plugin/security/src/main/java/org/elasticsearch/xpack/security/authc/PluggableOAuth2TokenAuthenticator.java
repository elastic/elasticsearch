package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomTokenAuthenticator;

public class PluggableOAuth2TokenAuthenticator extends AbstractPluggableAuthenticator {

    private CustomTokenAuthenticator customOAuth2TokenAuthenticator;

    public PluggableOAuth2TokenAuthenticator(CustomTokenAuthenticator authenticator) {
        this.customOAuth2TokenAuthenticator = authenticator;
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        return customOAuth2TokenAuthenticator.extractCredentials(context.getBearerString());
    }

    @Override
    public CustomTokenAuthenticator getAuthenticator() {
        return customOAuth2TokenAuthenticator;
    }
}
