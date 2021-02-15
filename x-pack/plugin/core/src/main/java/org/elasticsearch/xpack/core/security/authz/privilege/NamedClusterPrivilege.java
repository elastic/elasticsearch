/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;

/**
 * A {@link ClusterPrivilege} that has a name. The named cluster privileges can be referred simply by name within a
 * {@link RoleDescriptor#getClusterPrivileges()}.
 */
public interface NamedClusterPrivilege extends ClusterPrivilege {
    String name();

    /**
     * Returns a permission that represents this privilege only.
     * When building a role (or role-like object) that has many privileges, it is more efficient to build a shared permission using the
     * {@link #buildPermission(ClusterPermission.Builder)} method instead. This method is intended to allow callers to interrogate the
     * runtime permissions specifically granted by this privilege.
     * It is acceptable (and encouraged) for implementations of this method to cache (or precompute) the {@link ClusterPermission}
     * and return the same object on each call.
     * @see #buildPermission(ClusterPermission.Builder)
     */
    ClusterPermission permission();

}
