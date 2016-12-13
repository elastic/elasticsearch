/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;

import java.util.HashSet;
import java.util.Set;

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
    protected void doLookupUser(String username, ActionListener<User> listener) {
        userStore.getUser(username, listener);
    }

    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
        userStore.verifyPassword(token.principal(), token.credentials(), listener);
    }

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings() {
        return CachingUsernamePasswordRealm.getCachingSettings();
    }
}
