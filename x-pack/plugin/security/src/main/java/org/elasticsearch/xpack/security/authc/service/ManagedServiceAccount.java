/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountAuthorization;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ManagedServiceAccount implements ServiceAccount {

    private final ServiceAccount.ServiceAccountId id;
    private final List<String> roles;
    private final boolean enabled;
    private final User user;

    ManagedServiceAccount(ServiceAccount.ServiceAccountId id, List<String> roles, boolean enabled) {
        this.id = Objects.requireNonNull(id, "service account id cannot be null");
        this.roles = List.copyOf(Objects.requireNonNull(roles, "roles cannot be null"));
        this.enabled = enabled;
        this.user = new User(
            id.asPrincipal(),
            roles.toArray(String[]::new),
            "Managed service account - " + id,
            null,
            Map.of(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD, true),
            enabled
        );
    }

    @Override
    public ServiceAccount.ServiceAccountId id() {
        return id;
    }

    @Override
    public ServiceAccountAuthorization authorization() {
        return new ServiceAccountAuthorization.AssignedRoles(roles);
    }

    List<String> roles() {
        return roles;
    }

    boolean enabled() {
        return enabled;
    }

    @Override
    public User asUser() {
        return user;
    }

    @Override
    public String toString() {
        return "ManagedServiceAccount{" + "id=" + id + ", roles=" + roles + ", enabled=" + enabled + '}';
    }
}
