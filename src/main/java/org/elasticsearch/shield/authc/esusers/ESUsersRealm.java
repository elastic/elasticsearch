/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
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

    public ESUsersRealm(String name, Settings settings, FileUserPasswdStore userPasswdStore, FileUserRolesStore userRolesStore) {
        super(name, TYPE, settings);
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

    class Listener implements RefreshListener {
        @Override
        public void onRefresh() {
            expireAll();
        }
    }

    public static class Factory extends Realm.Factory<ESUsersRealm> {

        private final Environment env;
        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(Environment env, ResourceWatcherService watcherService, RestController restController) {
            super(TYPE, true);
            this.env = env;
            this.watcherService = watcherService;
            restController.registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
        }

        @Override
        public ESUsersRealm create(String name, Settings settings) {
            FileUserPasswdStore userPasswdStore = new FileUserPasswdStore(settings, env, watcherService);
            FileUserRolesStore userRolesStore = new FileUserRolesStore(settings, env, watcherService);
            return new ESUsersRealm(name, settings, userPasswdStore, userRolesStore);
        }

        @Override
        public ESUsersRealm createDefault(String name) {
            return create(name, ImmutableSettings.EMPTY);
        }
    }
}
