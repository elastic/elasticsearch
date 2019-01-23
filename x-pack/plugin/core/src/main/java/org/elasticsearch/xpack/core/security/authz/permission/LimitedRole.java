/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A {@link Role} limited by another role.<br>
 * The effective permissions returned on {@link #authorize(String, Set, MetaData, FieldPermissionsCache)} call would be limited by the
 * provided role.
 */
public final class LimitedRole extends Role {
    private final Role limitedBy;

    LimitedRole(String[] names, ClusterPermission cluster, IndicesPermission indices, ApplicationPermission application,
            RunAsPermission runAs, Role limitedBy) {
        super(names, cluster, indices, application, runAs);
        assert limitedBy != null : "limiting role is required";
        this.limitedBy = limitedBy;
    }

    public Role limitedBy() {
        return limitedBy;
    }

    @Override
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData,
            FieldPermissionsCache fieldPermissionsCache) {
        IndicesAccessControl indicesAccessControl = super.authorize(action, requestedIndicesOrAliases, metaData, fieldPermissionsCache);
        IndicesAccessControl limitedByIndicesAccessControl = limitedBy.authorize(action, requestedIndicesOrAliases, metaData,
                fieldPermissionsCache);

        return indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
    }

    /**
     * @return A predicate that will match all the indices that this role and
     * the scoped by role has the privilege for executing the given action on.
     */
    public Predicate<String> allowedIndicesMatcher(String action) {
        Predicate<String> predicate = indices().allowedIndicesMatcher(action);
        predicate = predicate.and(limitedBy.indices().allowedIndicesMatcher(action));
        return predicate;
    }

    /**
     * Check if indices permissions allow for the given action,
     * also checks whether the scoped by role allows the given actions
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkIndicesAction(String action) {
        return super.checkIndicesAction(action) && limitedBy.checkIndicesAction(action);
    }

    /**
     * Check if cluster permissions allow for the given action,
     * also checks whether the scoped by role allows the given actions
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    public boolean checkClusterAction(String action, TransportRequest request) {
        return super.checkClusterAction(action, request) && limitedBy.checkClusterAction(action, request);
    }

    /**
     * Create a new role defined by given role and the limited role.
     *
     * @param fromRole existing role {@link Role}
     * @param limitedByRole restrict the newly formed role to the permissions defined by this limited {@link Role}
     * @return {@link LimitedRole}
     */
    public static LimitedRole createLimitedRole(Role fromRole, Role limitedByRole) {
        Objects.requireNonNull(limitedByRole, "limited by role is required to create limited role");
        return new LimitedRole(fromRole.names(), fromRole.cluster(), fromRole.indices(), fromRole.application(), fromRole.runAs(),
                limitedByRole);
    }
}
