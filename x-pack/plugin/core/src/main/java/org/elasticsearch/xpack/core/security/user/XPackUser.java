/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Optional;

/**
 * XPack internal user that manages xpack. Has all cluster/indices permissions for x-pack to operate excluding security permissions.
 */
public class XPackUser extends InternalUser {

    public static final String NAME = UsernamesField.XPACK_NAME;
    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.XPACK_ROLE,
        new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("/@&~(\\.security.*)&~(\\.async-search.*)/")
                .privileges("all")
                .allowRestrictedIndices(true)
                .build() },
        new String[] { "*" },
        MetadataUtils.DEFAULT_RESERVED_METADATA
    );
    public static final XPackUser INSTANCE = new XPackUser();

    private XPackUser() {
        super(NAME);
    }

    @Override
    public Optional<RoleDescriptor> getRoleDescriptor() {
        return Optional.of(ROLE_DESCRIPTOR);
    }

}
