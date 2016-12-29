/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.Privilege.Name;
import org.elasticsearch.xpack.security.support.MetadataUtils;

public class LogstashSystemRole extends Role {

    private static final String[] CLUSTER_PRIVILEGES = new String[] { "monitor", MonitoringBulkAction.NAME};

    public static final String NAME = "logstash_system";
    public static final RoleDescriptor DESCRIPTOR =
            new RoleDescriptor(NAME, CLUSTER_PRIVILEGES, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final LogstashSystemRole INSTANCE = new LogstashSystemRole();

    private LogstashSystemRole() {
        super(DESCRIPTOR.getName(),
              new ClusterPermission.Core(ClusterPrivilege.get(new Name(DESCRIPTOR.getClusterPrivileges()))),
              new IndicesPermission.Core(Builder.convertFromIndicesPrivileges(DESCRIPTOR.getIndicesPrivileges())),
              RunAsPermission.Core.NONE);
    }
}
