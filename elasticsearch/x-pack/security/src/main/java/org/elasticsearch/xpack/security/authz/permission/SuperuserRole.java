/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.GeneralPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.Privilege.Name;

/**
 *
 */
public class SuperuserRole extends Role {

    public static final String NAME = "superuser";
    public static final RoleDescriptor DESCRIPTOR = new RoleDescriptor(NAME, new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build()},
            new String[] { "*" });
    public static final SuperuserRole INSTANCE = new SuperuserRole();

    private SuperuserRole() {
        super(DESCRIPTOR.getName(),
                new ClusterPermission.Core(ClusterPrivilege.get(new Name(DESCRIPTOR.getClusterPrivileges()))),
                new IndicesPermission.Core(Role.Builder.convertFromIndicesPrivileges(DESCRIPTOR.getIndicesPrivileges())),
                new RunAsPermission.Core(GeneralPrivilege.ALL));
    }
}
