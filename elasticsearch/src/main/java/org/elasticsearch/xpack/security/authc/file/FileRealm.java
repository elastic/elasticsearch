/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.file;

import java.util.Map;

import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.RefreshListener;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;

public class FileRealm extends CachingUsernamePasswordRealm {

    public static final String TYPE = "file";

    final FileUserPasswdStore userPasswdStore;
    final FileUserRolesStore userRolesStore;

    public FileRealm(RealmConfig config, ResourceWatcherService watcherService) {
        this(config, new FileUserPasswdStore(config, watcherService), new FileUserRolesStore(config, watcherService));
    }

    // pkg private for testing
    FileRealm(RealmConfig config, FileUserPasswdStore userPasswdStore, FileUserRolesStore userRolesStore) {
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
        stats.put("size", userPasswdStore.usersCount());
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
}
