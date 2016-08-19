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
 * A built-in role that grants users the necessary privileges to use Monitoring. The user will also need the {@link KibanaUserRole}
 */
public class MonitoringUserRole extends Role {

    private static final RoleDescriptor.IndicesPrivileges[] INDICES_PRIVILEGES = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                    .indices(".marvel-es-*", ".monitoring-*")
                    .privileges("read")
                    .build() };

    public static final String NAME = "monitoring_user";
    public static final RoleDescriptor DESCRIPTOR =
            new RoleDescriptor(NAME, null, INDICES_PRIVILEGES, null, MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final MonitoringUserRole INSTANCE = new MonitoringUserRole();

    private MonitoringUserRole() {
        super(DESCRIPTOR.getName(),
                Core.NONE,
                new IndicesPermission.Core(Role.Builder.convertFromIndicesPrivileges(DESCRIPTOR.getIndicesPrivileges())),
                RunAsPermission.Core.NONE);
    }
}
