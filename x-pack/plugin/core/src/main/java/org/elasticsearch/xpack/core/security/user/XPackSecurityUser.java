/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Map;
import java.util.Optional;

/**
 * internal user that manages xpack security. Has all cluster/indices permissions.
 */
public class XPackSecurityUser extends InternalUser {

    public static final XPackSecurityUser INSTANCE = new XPackSecurityUser();
    public static final String NAME = UsernamesField.XPACK_SECURITY_NAME;

    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.XPACK_SECURITY_ROLE,
        new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(true).build() },
        null,
        null,
        new String[] { "*" },
        MetadataUtils.DEFAULT_RESERVED_METADATA,
        Map.of()
    );

    private XPackSecurityUser() {
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
