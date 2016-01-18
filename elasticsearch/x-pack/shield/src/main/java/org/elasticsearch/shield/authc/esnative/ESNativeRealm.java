/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esnative;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;

import java.util.List;

/**
 * User/password realm that is backed by an Elasticsearch index
 */
public class ESNativeRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "esnative";

    final ESNativeUsersStore userStore;

    public ESNativeRealm(RealmConfig config, ESNativeUsersStore usersStore) {
        super(TYPE, config);
        this.userStore = usersStore;
        usersStore.addListener(new Listener());
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
    protected User doAuthenticate(UsernamePasswordToken token) {
        return userStore.verifyPassword(token.principal(), token.credentials());
    }

    class Listener implements ESNativeUsersStore.ChangeListener {

        @Override
        public void onUsersChanged(List<String> usernames) {
            for (String username : usernames) {
                expire(username);
            }
        }
    }

    public static class Factory extends Realm.Factory<ESNativeRealm> {

        private final Settings settings;
        private final Environment env;
        private final ESNativeUsersStore userStore;

        @Inject
        public Factory(Settings settings, Environment env, ESNativeUsersStore userStore) {
            super(TYPE, true);
            this.settings = settings;
            this.env = env;
            this.userStore = userStore;
        }

        @Override
        public ESNativeRealm create(RealmConfig config) {
            return new ESNativeRealm(config, userStore);
        }

        @Override
        public ESNativeRealm createDefault(String name) {
            RealmConfig config = new RealmConfig(name, Settings.EMPTY, settings, env);
            return create(config);
        }
    }

}
