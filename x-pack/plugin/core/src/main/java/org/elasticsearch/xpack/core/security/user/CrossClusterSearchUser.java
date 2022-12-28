/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

public class CrossClusterSearchUser extends User {
    public static final String NAME = UsernamesField.CROSS_CLUSTER_SEARCH_NAME;
    public static final CrossClusterSearchUser INSTANCE = new CrossClusterSearchUser();
    public static final RoleDescriptor ROLE_DESCRIPTOR = new RoleDescriptor(
        UsernamesField.CROSS_CLUSTER_SEARCH_ROLE,
        // monitor is required for cluster state retrieval
        new String[] { "monitor", "cancel_task" },
        // async search related
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

    private CrossClusterSearchUser() {
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
