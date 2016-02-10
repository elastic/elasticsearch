/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.Privilege.Name;

/**
 *
 */
public class KibanaRole extends Role {

    private static final String[] CLUSTER_PRIVILEGES = new String[] { "monitor" };
    private static final RoleDescriptor.IndicesPrivileges[] INDICES_PRIVILEGES = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices(".kibana" ).privileges("all").build() };

    public static final String NAME = "kibana";
    public static final RoleDescriptor DESCRIPTOR = new RoleDescriptor(NAME, CLUSTER_PRIVILEGES, INDICES_PRIVILEGES, null);
    public static final KibanaRole INSTANCE = new KibanaRole();

    private KibanaRole() {
        super(DESCRIPTOR.getName(),
                new ClusterPermission.Core(ClusterPrivilege.get(new Name(DESCRIPTOR.getClusterPrivileges()))),
                new IndicesPermission.Core(Role.Builder.convertFromIndicesPrivileges(DESCRIPTOR.getIndicesPrivileges())),
                RunAsPermission.Core.NONE);
    }
}
