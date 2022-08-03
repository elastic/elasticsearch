/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import java.util.Objects;
import java.util.Set;

/**
 * A unique identifier that can be associated to a Role. It can be used as cache key for role caching.
 */
public final class RoleKey {

    public static final String ROLES_STORE_SOURCE = "roles_stores";
    public static final RoleKey ROLE_KEY_EMPTY = new RoleKey(Set.of(), "__empty_role");
    public static final RoleKey ROLE_KEY_SUPERUSER = new RoleKey(
        Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()),
        RoleKey.ROLES_STORE_SOURCE
    );

    private final Set<String> names;
    private final String source;

    public RoleKey(Set<String> names, String source) {
        this.names = Objects.requireNonNull(names);
        this.source = Objects.requireNonNull(source);
    }

    public Set<String> getNames() {
        return names;
    }

    public String getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RoleKey roleKey = (RoleKey) o;
        return names.equals(roleKey.names) && source.equals(roleKey.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(names, source);
    }

    @Override
    public String toString() {
        return "RoleKey{" + "names=" + names + ", source='" + source + '\'' + '}';
    }
}
