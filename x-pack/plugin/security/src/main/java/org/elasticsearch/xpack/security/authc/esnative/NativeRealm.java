/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * User/password realm that is backed by an Elasticsearch index
 */
public class NativeRealm extends CachingUsernamePasswordRealm {

    private final NativeUsersStore userStore;

    public NativeRealm(RealmConfig config, NativeUsersStore usersStore, ThreadPool threadPool) {
        super(config, threadPool);
        this.userStore = usersStore;
    }

    @Override
    protected void doLookupUser(String username, ActionListener<User> listener) {
        userStore.getUser(username, listener);
    }

    @Override
    protected void doAuthenticate(UsernamePasswordToken token, ActionListener<AuthenticationResult<User>> listener) {
        userStore.verifyPassword(token.principal(), token.credentials(), listener);
    }

    @FixForMultiProject
    public void onSecurityIndexStateChange(
        ProjectId projectId,
        SecurityIndexManager.IndexState previousState,
        SecurityIndexManager.IndexState currentState
    ) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false) {
            clearCache();
        }
    }

    @Override
    public void usageStats(ActionListener<Map<String, Object>> listener) {
        super.usageStats(ActionListener.wrap(stats -> userStore.getUserCount(ActionListener.wrap(size -> {
            stats.put("size", size);
            listener.onResponse(stats);
        }, listener::onFailure)), listener::onFailure));
    }

    // method is used for testing to verify cache expiration since expireAll is final
    void clearCache() {
        expireAll();
    }

}
