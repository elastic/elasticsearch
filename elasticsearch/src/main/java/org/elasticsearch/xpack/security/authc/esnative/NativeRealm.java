/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;

/**
 * User/password realm that is backed by an Elasticsearch index
 */
public class NativeRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "native";

    private final NativeUsersStore userStore;

    public NativeRealm(RealmConfig config, NativeUsersStore usersStore) {
        super(TYPE, config);
        this.userStore = usersStore;
    }

    @Override
    public boolean userLookupSupported() {
        return true;
    }

    @Override
    protected User doLookupUser(String username) {
        return userStore.getUser(username);
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        userStore.getUsers(new String[] {username}, ActionListener.wrap(c -> listener.onResponse(c.stream().findAny().orElse(null)),
                listener::onFailure));
    }

    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        return userStore.verifyPassword(token.principal(), token.credentials());
    }
}
