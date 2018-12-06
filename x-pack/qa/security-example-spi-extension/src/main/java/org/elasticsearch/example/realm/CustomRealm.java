/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

public class CustomRealm extends Realm {

    public static final String TYPE = "custom";

    public static final String USER_HEADER = "User";
    public static final String PW_HEADER = "Password";

    public static final String KNOWN_USER = "custom_user";
    public static final SecureString KNOWN_PW = new SecureString("x-pack-test-password".toCharArray());
    static final String[] ROLES = new String[] { "superuser" };

    public CustomRealm(RealmConfig config) {
        super(config);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    @Override
    public UsernamePasswordToken token(ThreadContext threadContext) {
        String user = threadContext.getHeader(USER_HEADER);
        if (user != null) {
            String password = threadContext.getHeader(PW_HEADER);
            if (password != null) {
                return new UsernamePasswordToken(user, new SecureString(password.toCharArray()));
            }
        }
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        UsernamePasswordToken token = (UsernamePasswordToken)authToken;
        final String actualUser = token.principal();
        if (KNOWN_USER.equals(actualUser)) {
            if (CharArrays.constantTimeEquals(token.credentials().getChars(), KNOWN_PW.getChars())) {
                listener.onResponse(AuthenticationResult.success(new User(actualUser, ROLES)));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("Invalid password for user " + actualUser, null));
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }
}
