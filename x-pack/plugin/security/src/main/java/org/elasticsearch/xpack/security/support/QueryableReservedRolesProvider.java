/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A provider of the built-in reserved roles.
 * <p>
 * This provider fetches all reserved roles from the {@link ReservedRolesStore} and calculates their hashes lazily.
 * The reserved roles are static and do not change during runtime, hence this provider will never notify any listeners.
 * </p>
 */
public class QueryableReservedRolesProvider implements QueryableBuiltInRoles.Provider {

    private final Supplier<QueryableBuiltInRoles> reservedRolesSupplier;

    /**
     * Constructs a new reserved roles provider.
     *
     * @param reservedRolesStore the store to fetch the reserved roles from.
     *                           Having a store reference here is necessary to ensure that static fields are initialized.
     */
    public QueryableReservedRolesProvider(ReservedRolesStore reservedRolesStore) {
        this.reservedRolesSupplier = CachedSupplier.wrap(() -> {
            final Collection<RoleDescriptor> roleDescriptors = Collections.unmodifiableCollection(ReservedRolesStore.roleDescriptors());
            return new QueryableBuiltInRoles(
                roleDescriptors.stream()
                    .collect(Collectors.toUnmodifiableMap(RoleDescriptor::getName, QueryableBuiltInRolesUtils::calculateHash)),
                roleDescriptors
            );
        });
    }

    @Override
    public QueryableBuiltInRoles getRoles() {
        return reservedRolesSupplier.get();
    }

    @Override
    public void addListener(QueryableBuiltInRoles.Listener listener) {
        // no-op: reserved roles are static and do not change
    }
}
