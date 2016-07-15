/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.RefreshListener;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Map;

/**
 *
 */
public class FileRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "file";

    final FileUserPasswdStore userPasswdStore;
    final FileUserRolesStore userRolesStore;

    public FileRealm(RealmConfig config, FileUserPasswdStore userPasswdStore, FileUserRolesStore userRolesStore) {
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

        return new User(token.principal(), roles);
    }

    @Override
    public User doLookupUser(String username) {
        if (userPasswdStore.userExists(username)) {
            String[] roles = userRolesStore.roles(username);
            return new User(username, roles);
        }
        return null;
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> stats = super.usageStats();
        // here we can determine the size based on the in mem user store
        stats.put("size", UserbaseSize.resolve(userPasswdStore.usersCount()));
        return stats;
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

    public static class Factory extends UsernamePasswordRealm.Factory<FileRealm> {

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
        public FileRealm create(RealmConfig config) {
            return new FileRealm(config, new FileUserPasswdStore(config, watcherService),
                    new FileUserRolesStore(config, watcherService));
        }

        @Override
        public FileRealm createDefault(String name) {
            RealmConfig config = new RealmConfig(name, Settings.EMPTY, settings, env);
            return create(config);
        }
    }
}
