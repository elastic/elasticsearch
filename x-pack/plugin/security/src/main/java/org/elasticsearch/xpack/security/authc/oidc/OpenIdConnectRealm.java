package org.elasticsearch.xpack.security.authc.oidc;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.user.User;

public class OpenIdConnectRealm extends Realm implements Releasable {

    private static final Logger logger = LogManager.getLogger(OpenIdConnectRealm.class);

    public OpenIdConnectRealm(RealmConfig config) {
        super(config);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public AuthenticationToken token(ThreadContext context) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult> listener) {

    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {

    }
}
