/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.Privilege.Name;
import org.elasticsearch.xpack.security.support.MetadataUtils;

/**
 * Reserved role for the transport client
 */
public class TransportClientRole extends Role {

    public static final String NAME = "transport_client";
    private static final String[] CLUSTER_PRIVILEGES = new String[] { "transport_client" };

    public static final RoleDescriptor DESCRIPTOR =
            new RoleDescriptor(NAME, CLUSTER_PRIVILEGES, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA);
    public static final TransportClientRole INSTANCE = new TransportClientRole();

    private TransportClientRole() {
        super(DESCRIPTOR.getName(),
                new ClusterPermission.Core(ClusterPrivilege.get(new Name(DESCRIPTOR.getClusterPrivileges()))),
                IndicesPermission.Core.NONE, RunAsPermission.Core.NONE);
    }
}
