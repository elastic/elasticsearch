/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.permission.ClusterPermission.Core;
import org.elasticsearch.xpack.security.support.MetadataUtils;

/**
 * A role for users of the reporting features in xpack
 */
public class ReportingUserRole extends Role {
    private static final RoleDescriptor.IndicesPrivileges[] INDICES_PRIVILEGES = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".reporting-*")
                    .privileges("read", "write")
                    .build()
    };

    public static final String NAME = "reporting_user";
    public static final RoleDescriptor DESCRIPTOR =
            new RoleDescriptor(NAME, null, INDICES_PRIVILEGES, null, MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final ReportingUserRole INSTANCE = new ReportingUserRole();

    private ReportingUserRole() {
        super(DESCRIPTOR.getName(),
                Core.NONE,
                new IndicesPermission.Core(Role.Builder.convertFromIndicesPrivileges(DESCRIPTOR.getIndicesPrivileges())),
                RunAsPermission.Core.NONE);
    }
}
