/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    @Override
    public Predicate<String> allowedIndicesMatcher(String action) {
        Predicate<String> predicate = indices().allowedIndicesMatcher(action);
        predicate = predicate.and(limitedBy.indices().allowedIndicesMatcher(action));
        return predicate;
    }

    /**
     * Check if indices permissions allow for the given action,
     * also checks whether the limited by role allows the given actions
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkIndicesAction(String action) {
        return super.checkIndicesAction(action) && limitedBy.checkIndicesAction(action);
    }

    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcesPrivileges}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.<br>
     * This one takes intersection of resource privileges with the resource privileges from the limited-by role.
     *
     * @param forIndexPatterns set of index patterns
     * @param allowRestrictedIndices {@code true} whether restricted indices are allowed
     * @param forPrivileges set of index privileges
     * @return an instance of {@link ResourcesPrivileges}
     */
    @Override
    public ResourcesPrivileges getResourcePrivileges(List<String> forIndexPatterns, boolean allowRestrictedIndices,
                                                                List<String> forPrivileges) {
        ResourcesPrivileges resourcePrivilegesMap = super.indices().getResourcePrivileges(forIndexPatterns,
                allowRestrictedIndices, forPrivileges);
        ResourcesPrivileges resourcePrivilegesMapForLimitedRole = limitedBy.indices().getResourcePrivileges(forIndexPatterns,
                allowRestrictedIndices, forPrivileges);
        return intersect(forIndexPatterns, forPrivileges, resourcePrivilegesMap, resourcePrivilegesMapForLimitedRole);
    }

    /**
     * Check if cluster permissions allow for the given action,
     * also checks whether the limited by role allows the given actions
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkClusterAction(String action, TransportRequest request) {
        return super.checkClusterAction(action, request) && limitedBy.checkClusterAction(action, request);
    }

    /**
     * Check if cluster permissions allow for the given cluster privilege,
     * also checks whether the limited by role allows the given cluster privilege
     *
     * @param clusterPrivilege cluster privilege
     * @return {@code true} if cluster privilege is allowed else returns {@code false}
     */
    @Override
    public boolean checkClusterPrivilege(ClusterPrivilege clusterPrivilege) {
        return super.cluster().check(clusterPrivilege) && limitedBy.cluster().check(clusterPrivilege);
    }


    /**
     * For a given application, checks for the privileges for resources and returns an instance of {@link ResourcesPrivileges} holding a map
     * of resource to {@link ResourcePrivileges} where the resource is application resource and the map of application privilege to whether
     * it is allowed or not.<br>
     * This one takes intersection of resource privileges with the resource privileges from the limited-by role.
     *
     * @param applicationName application name
     * @param forResources list of application resources
     * @param forPrivilegeNames list of application privileges
     * @param storedPrivileges stored {@link ApplicationPrivilegeDescriptor} for the application
     * @return an instance of {@link ResourcesPrivileges}
     */
    @Override
    public ResourcesPrivileges getResourcePrivileges(final String applicationName, List<String> forResources,
                                                                 List<String> forPrivilegeNames,
                                                                 Collection<ApplicationPrivilegeDescriptor> storedPrivileges) {
        ResourcesPrivileges resourcePrivilegesMap = super.application().getResourcePrivileges(applicationName, forResources,
                forPrivilegeNames, storedPrivileges);
        ResourcesPrivileges resourcePrivilegesMapForLimitedRole = limitedBy.application().getResourcePrivileges(applicationName,
                forResources, forPrivilegeNames, storedPrivileges);
        return intersect(forResources, forPrivilegeNames, resourcePrivilegesMap, resourcePrivilegesMapForLimitedRole);
    }

    private ResourcesPrivileges intersect(List<String> forResources, List<String> forPrivilegeNames,
                                                      ResourcesPrivileges resourcePrivilegesMap,
                                                      ResourcesPrivileges resourcePrivilegesMapForLimitedRole) {
        final boolean allowAll;
        if (resourcePrivilegesMap.allAllowed() == resourcePrivilegesMapForLimitedRole.allAllowed()) {
            allowAll = resourcePrivilegesMap.allAllowed();
        } else {
            allowAll = false;
        }
        Map<String, ResourcePrivileges> result = new LinkedHashMap<>();
        for (String resource : forResources) {
            ResourcePrivileges resourcePrivileges = resourcePrivilegesMap.getResourceToResourcePrivileges().get(resource);
            ResourcePrivileges resourcePrivilegesForLimitedRole = resourcePrivilegesMapForLimitedRole.getResourceToResourcePrivileges()
                    .get(resource);
            Map<String, Boolean> privileges = new HashMap<>();
            for (String privilege : forPrivilegeNames) {
                if (resourcePrivileges.isAllowed(privilege)
                        && resourcePrivilegesForLimitedRole.isAllowed(privilege)) {
                    privileges.put(privilege, Boolean.TRUE);
                } else {
                    privileges.put(privilege, Boolean.FALSE);
                }
            }
            result.put(resource,new ResourcePrivileges(resource, privileges));
        }
        return new ResourcesPrivileges(allowAll, result);
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
