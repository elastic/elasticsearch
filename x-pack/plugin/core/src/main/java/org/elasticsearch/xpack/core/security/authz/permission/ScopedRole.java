/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Set;

/**
 * A {@link Role} scoped by another role.<br>
 * The effective permissions returned on {@link #authorize(String, Set, MetaData, FieldPermissionsCache)} call would be scoped by the
 * provided role.
 */
public class ScopedRole extends Role {
    private final Role scopedBy;

    ScopedRole(String[] names, ClusterPermission cluster, IndicesPermission indices, ApplicationPermission application,
            RunAsPermission runAs, Role scopedBy) {
        super(names, cluster, indices, application, runAs);
        this.scopedBy = scopedBy;
    }

    public Role scopedBy() {
        return scopedBy;
    }

    @Override
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData,
            FieldPermissionsCache fieldPermissionsCache) {
        IndicesAccessControl indicesAccessControl = super.authorize(action, requestedIndicesOrAliases, metaData, fieldPermissionsCache);
        IndicesAccessControl scopedByIndicesAccessControl = scopedBy.authorize(action, requestedIndicesOrAliases, metaData,
                fieldPermissionsCache);

        return IndicesAccessControl.scopedIndicesAccessControl(indicesAccessControl, scopedByIndicesAccessControl);
    }

    /**
     * Check if indices permissions allow for the given action
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkIndicesAction(String action) {
        boolean allowed = super.checkIndicesAction(action);
        if (allowed && scopedBy != null) {
            allowed = scopedBy.checkIndicesAction(action);
        }
        return allowed;
    }

    /**
     * Check if cluster permissions allow for the given action
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    public boolean checkClusterAction(String action, TransportRequest request) {
        boolean allowed = super.checkClusterAction(action, request);
        if (allowed && scopedBy != null) {
            allowed = scopedBy.checkClusterAction(action, request);
        }
        return allowed;
    }

    /**
     * Create a new role defined by given role and the scoped role.
     *
     * @param fromRole existing role {@link Role}
     * @param scopedByRole restrict the newly formed role to the permissions defined by this scoped {@link Role}
     * @return {@link ScopedRole}
     */
    public static ScopedRole createScopedRole(Role fromRole, Role scopedByRole) {
        return new ScopedRole(fromRole.names(), fromRole.cluster(), fromRole.indices(), fromRole.application(), fromRole.runAs(),
                scopedByRole);
    }
}
