/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A service account created through the API and stored in the security index, as opposed to one of the built-in
 * accounts declared in {@link ElasticServiceAccounts}.
 * <p>
 * Its privileges are the named roles in {@link #roles()}, which reach authorization as the roles of the {@link User}
 * built here. That routing is selected by {@link ServiceAccountSettings#USER_MANAGED_SERVICE_ACCOUNT_FIELD} in the
 * user's metadata: without the marker the authorization layer would look for a built-in account of the same name
 * instead, so every instance must set it.
 */
final class UserManagedServiceAccount implements ServiceAccount {

    private final ServiceAccountId id;
    private final List<String> roles;
    private final boolean enabled;
    private final User user;

    UserManagedServiceAccount(ServiceAccountId id, List<String> roles, boolean enabled) {
        this.id = Objects.requireNonNull(id, "service account id cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.enabled = enabled;
        this.user = new User(
            id.asPrincipal(),
            this.roles.toArray(String[]::new),
            "User-managed service account - " + id,
            null,
            Map.of(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD, true),
            enabled
        );
    }

    @Override
    public ServiceAccountId id() {
        return id;
    }

    @Override
    public User asUser() {
        return user;
    }

    List<String> roles() {
        return roles;
    }

    boolean enabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "UserManagedServiceAccount{id=" + id + ", roles=" + roles + ", enabled=" + enabled + '}';
    }
}
