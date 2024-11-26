/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.core.security.action.role.BulkRolesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;

/**
 * Wrapper around the {@link NativeRolesStore} that provides a way to create, update and delete built-in roles.
 */
public final class QueryableBuiltInRolesStore {

    private final NativeRolesStore nativeRolesStore;

    public QueryableBuiltInRolesStore(NativeRolesStore nativeRolesStore) {
        this.nativeRolesStore = nativeRolesStore;
    }

    public void putRoles(
        final SecurityIndexManager securityIndexManager,
        final Collection<RoleDescriptor> roles,
        final ActionListener<BulkRolesResponse> listener
    ) {
        assert roles.stream().allMatch(role -> (Boolean) role.getMetadata().get(MetadataUtils.RESERVED_METADATA_KEY));
        nativeRolesStore.putRoles(securityIndexManager, WriteRequest.RefreshPolicy.IMMEDIATE, roles, false, listener);
    }

    public void deleteRoles(
        final SecurityIndexManager securityIndexManager,
        final Collection<String> roleNames,
        final ActionListener<BulkRolesResponse> listener
    ) {
        nativeRolesStore.deleteRoles(securityIndexManager, roleNames, WriteRequest.RefreshPolicy.IMMEDIATE, false, listener);
    }

    public boolean isEnabled() {
        return nativeRolesStore.isEnabled();
    }

}
