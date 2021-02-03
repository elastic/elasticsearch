/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * A {@link ClusterPrivilege} that has a name. The named cluster privileges can be referred simply by name within a
 * {@link RoleDescriptor#getClusterPrivileges()}.
 */
public interface NamedClusterPrivilege extends ClusterPrivilege {
    String name();
}
