/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.HashMap;
import java.util.Map;

public final class DummyUsernamePasswordRealm extends UsernamePasswordRealm {

    private Map<String, Tuple<SecureString, User>> users;

    public DummyUsernamePasswordRealm(RealmConfig config) {
        super(config);
        setRealmRef(new Authentication.RealmRef(config.name(), config.type(), Node.NODE_NAME_SETTING.get(config.settings())));
        this.users = new HashMap<>();
    }

    public void defineUser(User user, SecureString password) {
        this.users.put(user.principal(), new Tuple<>(password, user));
    }

    public void defineUser(String username, SecureString password) {
        defineUser(new User(username), password);
    }

    @Override
    public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener) {
        if (token instanceof UsernamePasswordToken usernamePasswordToken) {
            User user = authenticate(usernamePasswordToken.principal(), usernamePasswordToken.credentials());
            if (user != null) {
                listener.onResponse(AuthenticationResult.success(user));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("Failed to authenticate " + usernamePasswordToken.principal(), null));
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    private User authenticate(String principal, SecureString credentials) {
        final Tuple<SecureString, User> tuple = users.get(principal);
        if (tuple.v1().equals(credentials)) {
            return tuple.v2();
        }
        return null;
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        listener.onResponse(null);
    }
}
