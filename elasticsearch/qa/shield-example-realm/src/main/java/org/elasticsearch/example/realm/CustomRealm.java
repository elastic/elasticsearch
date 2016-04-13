/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;

public class CustomRealm extends Realm<UsernamePasswordToken> {

    public static final String TYPE = "custom";

    static final String USER_HEADER = "User";
    static final String PW_HEADER = "Password";

    static final String KNOWN_USER = "custom_user";
    static final String KNOWN_PW = "changeme";
    static final String[] ROLES = new String[] { "superuser" };

    public CustomRealm(RealmConfig config) {
        super(TYPE, config);
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
                return new UsernamePasswordToken(user, new SecuredString(password.toCharArray()));
            }
        }
        return null;
    }

    @Override
    public User authenticate(UsernamePasswordToken token) {
        final String actualUser = token.principal();
        if (KNOWN_USER.equals(actualUser) && SecuredString.constantTimeEquals(token.credentials(), KNOWN_PW)) {
            return new User(actualUser, ROLES);
        }
        return null;
    }

    @Override
    public User lookupUser(String username) {
        return null;
    }

    @Override
    public boolean userLookupSupported() {
        return false;
    }
}
