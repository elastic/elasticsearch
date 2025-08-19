package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.apikey.CustomTokenAuthenticator;

public abstract class AbstractPluggableAuthenticator implements Authenticator {

    @Override
    public String name() {
        return getAuthenticator().name();
    }

    public void authenticate(Authenticator.Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        getAuthenticator().authenticate(authenticationToken, ActionListener.wrap(response -> {
            if (response.isAuthenticated()) {
                listener.onResponse(response);
            } else if (response.getStatus() == AuthenticationResult.Status.TERMINATE) {
                final Exception ex = response.getException();
                if (ex == null) {
                    listener.onFailure(context.getRequest().authenticationFailed(authenticationToken));
                } else {
                    listener.onFailure(context.getRequest().exceptionProcessingRequest(ex, authenticationToken));
                }
            } else if (response.getStatus() == AuthenticationResult.Status.CONTINUE) {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        }, ex -> listener.onFailure(context.getRequest().exceptionProcessingRequest(ex, authenticationToken))));
    }

    public abstract CustomTokenAuthenticator getAuthenticator();
}
