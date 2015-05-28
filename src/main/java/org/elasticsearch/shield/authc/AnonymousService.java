/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;

public class AnonymousService {

    public static final String SETTING_AUTHORIZATION_EXCEPTION_ENABLED = "shield.authc.anonymous.authz_exception";
    static final String ANONYMOUS_USERNAME = "_es_anonymous_user";


    @Nullable
    private final User anonymousUser;
    private final boolean authzExceptionEnabled;

    @Inject
    public AnonymousService(Settings settings) {
        anonymousUser = resolveAnonymousUser(settings);
        authzExceptionEnabled = settings.getAsBoolean(SETTING_AUTHORIZATION_EXCEPTION_ENABLED, true);
    }

    public boolean enabled() {
        return anonymousUser != null;
    }

    public boolean isAnonymous(User user) {
        if (enabled()) {
            return anonymousUser.equals(user);
        }
        return false;
    }

    public User anonymousUser() {
        return anonymousUser;
    }

    public boolean authorizationExceptionsEnabled() {
        return authzExceptionEnabled;
    }

    static User resolveAnonymousUser(Settings settings) {
        String[] roles = settings.getAsArray("shield.authc.anonymous.roles", null);
        if (roles == null) {
            return null;
        }
        String username = settings.get("shield.authc.anonymous.username", ANONYMOUS_USERNAME);
        return new User.Simple(username, roles);
    }
}
