/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 *
 */
public class ESUsersRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "esusers";

    final FileUserPasswdStore userPasswdStore;
    final FileUserRolesStore userRolesStore;

    public ESUsersRealm(RealmConfig config, FileUserPasswdStore userPasswdStore, FileUserRolesStore userRolesStore) {
        super(TYPE, config);
        Listener listener = new Listener();
        this.userPasswdStore = userPasswdStore;
        userPasswdStore.addListener(listener);
        this.userRolesStore = userRolesStore;
        userRolesStore.addListener(listener);
    }

    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        if (!userPasswdStore.verifyPassword(token.principal(), token.credentials())) {
            return null;
        }
        String[] roles = userRolesStore.roles(token.principal());
        return new User.Simple(token.principal(), roles);
    }

    @Override
    public User doLookupUser(String username) {
        if (userPasswdStore.userExists(username)){
            String[] roles = userRolesStore.roles(username);
            return new User.Simple(username, roles);
        }
        return null;
    }

    @Override
    public boolean userLookupSupported() {
        return true;
    }

    class Listener implements RefreshListener {
        @Override
        public void onRefresh() {
            expireAll();
        }
    }

    public static class Factory extends Realm.Factory<ESUsersRealm> {

        private final Settings settings;
        private final Environment env;
        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(Settings settings, Environment env, ResourceWatcherService watcherService) {
            super(TYPE, true);
            this.settings = settings;
            this.env = env;
            this.watcherService = watcherService;
        }

        @Override
        public ESUsersRealm create(RealmConfig config) {
            FileUserPasswdStore userPasswdStore = new FileUserPasswdStore(config, watcherService);
            FileUserRolesStore userRolesStore = new FileUserRolesStore(config, watcherService);
            return new ESUsersRealm(config, userPasswdStore, userRolesStore);
        }

        @Override
        public ESUsersRealm createDefault(String name) {
            RealmConfig config = new RealmConfig(name, Settings.EMPTY, settings, env);
            return create(config);
        }
    }
}
