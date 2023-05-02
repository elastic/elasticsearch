/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Optional;

public class AsyncSearchUser extends InternalUser {

    public static final AsyncSearchUser INSTANCE = new AsyncSearchUser();

    public static final String NAME = UsernamesField.ASYNC_SEARCH_NAME;

    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.ASYNC_SEARCH_ROLE,
        new String[] { "cancel_task" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(XPackPlugin.ASYNC_RESULTS_INDEX + "*")
                .privileges("all")
                .allowRestrictedIndices(true)
                .build(), },
        null,
        null,
        null,
        MetadataUtils.DEFAULT_RESERVED_METADATA,
        null
    );

    private AsyncSearchUser() {
        super(NAME);
    }

    @Override
    public Optional<RoleDescriptor> getLocalClusterRole() {
        return Optional.of(ROLE_DESCRIPTOR);
    }

    /**
     * @return {@link Optional#empty()} because this user is not permitted to execute actions that originate from a remote cluster
     */
    @Override
    public Optional<RoleDescriptor> getRemoteAccessRole() {
        return Optional.empty();
    }

}
