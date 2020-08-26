/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;

/**
 * An example realm with specific behaviours:
 * (1) It only supports lookup (that is, "run-as" and "authorization_realms") but not authentication
 * (2) It performs role mapping to determine the roles for the looked-up user
 * (3) It caches the looked-up User objects
 */
public class CustomRoleMappingRealm extends Realm implements CachingRealm {

    public static final String TYPE = "custom_role_mapping";

    static final String USERNAME = "role_mapped_user";
    static final String USER_GROUP = "user_group";

    private final Cache<String, User> cache;
    private final UserRoleMapper roleMapper;

    public CustomRoleMappingRealm(RealmConfig config, UserRoleMapper roleMapper) {
        super(config);
        this.cache = CacheBuilder.<String, User>builder().build();
        this.roleMapper = roleMapper;
        this.roleMapper.refreshRealmOnChange(this);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return false;
    }

    @Override
    public UsernamePasswordToken token(ThreadContext threadContext) {
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult> listener) {
        listener.onResponse(AuthenticationResult.notHandled());
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        final User user = cache.get(username);
        if (user != null) {
            listener.onResponse(user);
            return;
        }
        if (USERNAME.equals(username)) {
            buildUser(username, ActionListener.wrap(
                u -> listener.onResponse(cache.computeIfAbsent(username, k -> u)),
                listener::onFailure
            ));
        } else {
            listener.onResponse(null);
        }
    }

    private void buildUser(String username, ActionListener<User> listener) {
        final UserRoleMapper.UserData data = new UserRoleMapper.UserData(username, null, List.of(USER_GROUP), Map.of(), super.config);
        roleMapper.resolveRoles(data, ActionListener.wrap(
            roles -> listener.onResponse(new User(username, roles.toArray(String[]::new))),
            listener::onFailure
        ));
    }

    @Override
    public void expire(String username) {
        this.cache.invalidate(username);
    }

    @Override
    public void expireAll() {
        this.cache.invalidateAll();
    }
}
