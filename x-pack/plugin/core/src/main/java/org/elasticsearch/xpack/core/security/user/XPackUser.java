/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

/**
 * XPack internal user that manages xpack. Has all cluster/indices permissions for x-pack to operate excluding security permissions.
 */
public class XPackUser extends User {

    public static final String NAME = UsernamesField.XPACK_NAME;
    public static final String ROLE_NAME = UsernamesField.XPACK_ROLE;
    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(ROLE_NAME, new String[] { "all" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("/@&~(\\.security.*)&~(\\.async-search.*)/")
                .privileges("all")
                .allowRestrictedIndices(true)
                .build(),
            RoleDescriptor.IndicesPrivileges.builder().indices(IndexAuditTrailField.INDEX_NAME_PREFIX + "-*")
                .privileges("read").build()
        },
        new String[] { "*" },
        MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final XPackUser INSTANCE = new XPackUser();

    private XPackUser() {
        super(NAME, ROLE_NAME);
    }

    @Override
    public boolean equals(Object o) {
        return INSTANCE == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }

    public static boolean is(String principal) {
        return NAME.equals(principal);
    }
}
