/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Map;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * User/password realm that is backed by an Elasticsearch index
 */
public class NativeRealm extends CachingUsernamePasswordRealm {

    private final NativeUsersStore usersStore;

    public NativeRealm(RealmConfig config, NativeUsersStore usersStore, ThreadPool threadPool) {
        super(config, threadPool);
        this.usersStore = usersStore;
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        usersStore.getUser(username, listener);
    }

    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult> listener) {
        usersStore.verifyPassword(token.principal(), token.credentials(), listener);
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState) || isIndexDeleted(previousState, currentState)) {
            clearCache();
        }
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats ->
            usersStore.getUserCount(ActionListener.wrap(size -> {
                stats.put("size", size);
                listener.onResponse(stats);
            }, listener::onFailure))
        , listener::onFailure));
    }

    // method is used for testing to verify cache expiration since expireAll is final
    void clearCache() {
        expireAll();
    }

}
