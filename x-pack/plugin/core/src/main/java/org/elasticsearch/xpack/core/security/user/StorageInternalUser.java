/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Optional;

/**
 * "Storage" internal user - used when the indexing/storage subsystem needs to perform actions on specific indices
 * (that may not be permitted by the authenticated user)
 */
public class StorageInternalUser extends InternalUser {

    public static final String NAME = UsernamesField.STORAGE_USER_NAME;
    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.STORAGE_ROLE_NAME,
        new String[] {},
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("*")
                .privileges(RefreshAction.NAME + "*")
                .allowRestrictedIndices(true)
                .build() },
        new String[] {},
        MetadataUtils.DEFAULT_RESERVED_METADATA
    );
    public static final StorageInternalUser INSTANCE = new StorageInternalUser();

    private StorageInternalUser() {
        super(NAME);
        assert enabled();
        assert roles() != null && roles().length == 0;
    }

    @Override
    public Optional<RoleDescriptor> getLocalClusterRole() {
        return Optional.of(ROLE_DESCRIPTOR);
    }

    @Override
    public Optional<RoleDescriptor> getRemoteAccessRole() {
        return Optional.empty();
    }
}
