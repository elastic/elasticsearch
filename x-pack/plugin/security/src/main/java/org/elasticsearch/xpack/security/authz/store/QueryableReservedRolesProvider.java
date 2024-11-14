/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class QueryableReservedRolesProvider implements QueryableRolesProvider {

    private final Supplier<QueryableRoles> roles;

    public QueryableReservedRolesProvider(final ReservedRolesStore reservedRolesStore) {
        this.roles = CachedSupplier.wrap(() -> {
            final Map<String, RoleDescriptor> roleDescriptors = ReservedRolesStore.roleDescriptorsMap();
            final String version = calculateHash(roleDescriptors.values()).get();
            final Map<String, String> rolesVersions = roleDescriptors.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> version));
            return new QueryableRoles(rolesVersions, roleDescriptors);
        });
    }

    @Override
    public QueryableRoles roles() {
        return roles.get();
    }

    @Override
    public void addListener(QueryableRolesChangedListener listener) {
        // Reserved roles are static and do not change, so we do not need to notify listeners.
    }

    private Supplier<String> calculateHash(final Collection<RoleDescriptor> roleDescriptors) {
        return CachedSupplier.wrap(() -> QueryableRolesUtils.calculateHash(roleDescriptors));
    }

}
