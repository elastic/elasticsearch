/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

/**
 * XPack internal user that manages xpack. Has all cluster/indices permissions for x-pack to operate excluding security permissions.
 */
public class XPackUser extends User {

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
        super(NAME, Strings.EMPTY_ARRAY);
        // the following traits, and especially the run-as one, go with all the internal users
        // TODO abstract in a base `InternalUser` class
        assert enabled();
        assert roles() != null && roles().length == 0;
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

}
