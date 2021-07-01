/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A {@link Role} limited by another role.<br>
 * The effective permissions returned on {@link #authorize(String, Set, Map, FieldPermissionsCache)} call would be limited by the
 * provided role.
 */
public final class LimitedRole extends Role {
    private final Role limitedBy;

    LimitedRole(ClusterPermission cluster, IndicesPermission indices, ApplicationPermission application, RunAsPermission runAs,
                Role limitedBy) {
        super(Objects.requireNonNull(limitedBy, "limiting role is required").names(), cluster, indices, application, runAs);
        this.limitedBy = limitedBy;
    }

    @Override
    public ClusterPermission cluster() {
        throw new UnsupportedOperationException("cannot retrieve cluster permission on limited role");
    }

    @Override
    public IndicesPermission indices() {
        throw new UnsupportedOperationException("cannot retrieve indices permission on limited role");
    }

    @Override
    public ApplicationPermission application() {
        throw new UnsupportedOperationException("cannot retrieve application permission on limited role");
    }

    @Override
    public RunAsPermission runAs() {
        throw new UnsupportedOperationException("cannot retrieve run_as permission on limited role");
    }

    @Override
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases,
                                          Map<String, IndexAbstraction> aliasAndIndexLookup,
                                          FieldPermissionsCache fieldPermissionsCache) {
        IndicesAccessControl indicesAccessControl =
            super.authorize(action, requestedIndicesOrAliases, aliasAndIndexLookup, fieldPermissionsCache);
        IndicesAccessControl limitedByIndicesAccessControl = limitedBy.authorize(action, requestedIndicesOrAliases, aliasAndIndexLookup,
                fieldPermissionsCache);

        return indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
    }

    /**
     * @return A predicate that will match all the indices that this role and the limited by role has the privilege for executing the given
     * action on.
     */
    @Override
    public Predicate<IndexAbstraction> allowedIndicesMatcher(String action) {
        Predicate<IndexAbstraction> predicate = super.indices().allowedIndicesMatcher(action);
        predicate = predicate.and(limitedBy.indices().allowedIndicesMatcher(action));
        return predicate;
    }

    @Override
    public Automaton allowedActionsMatcher(String index) {
        final Automaton allowedMatcher = super.allowedActionsMatcher(index);
        final Automaton limitedByMatcher = limitedBy.allowedActionsMatcher(index);
        return Automatons.intersectAndMinimize(allowedMatcher, limitedByMatcher);
    }

    /**
     * Check if indices permissions allow for the given action, also checks whether the limited by role allows the given actions
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkIndicesAction(String action) {
        return super.checkIndicesAction(action) && limitedBy.checkIndicesAction(action);
    }

    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcePrivilegesMap}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.<br>
     * This one takes intersection of resource privileges with the resource privileges from the limited-by role.
     *
     * @param checkForIndexPatterns check permission grants for the set of index patterns
     * @param allowRestrictedIndices if {@code true} then checks permission grants even for restricted indices by index matching
     * @param checkForPrivileges check permission grants for the set of index privileges
     * @return an instance of {@link ResourcePrivilegesMap}
     */
    @Override
    public ResourcePrivilegesMap checkIndicesPrivileges(Set<String> checkForIndexPatterns, boolean allowRestrictedIndices,
                                                        Set<String> checkForPrivileges) {
        ResourcePrivilegesMap resourcePrivilegesMap = super.indices().checkResourcePrivileges(checkForIndexPatterns, allowRestrictedIndices,
                checkForPrivileges);
        ResourcePrivilegesMap resourcePrivilegesMapForLimitedRole = limitedBy.indices().checkResourcePrivileges(checkForIndexPatterns,
                allowRestrictedIndices, checkForPrivileges);
        return ResourcePrivilegesMap.intersection(resourcePrivilegesMap, resourcePrivilegesMapForLimitedRole);
    }

    /**
     * Check if cluster permissions allow for the given action,
     * also checks whether the limited by role allows the given actions in the context of given
     * authentication.
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @param authentication {@link Authentication}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    @Override
    public boolean checkClusterAction(String action, TransportRequest request, Authentication authentication) {
        return super.checkClusterAction(action, request, authentication) && limitedBy.checkClusterAction(action, request, authentication);
    }

    /**
     * Check if cluster permissions grants the given cluster privilege, also checks whether the limited by role grants the given cluster
     * privilege
     *
     * @param clusterPrivilege cluster privilege
     * @return {@code true} if cluster privilege is allowed else returns {@code false}
     */
    @Override
    public boolean grants(ClusterPrivilege clusterPrivilege) {
        return super.grants(clusterPrivilege) && limitedBy.grants(clusterPrivilege);
    }

    /**
     * For a given application, checks for the privileges for resources and returns an instance of {@link ResourcePrivilegesMap} holding a
     * map of resource to {@link ResourcePrivileges} where the resource is application resource and the map of application privilege to
     * whether it is allowed or not.<br>
     * This one takes intersection of resource privileges with the resource privileges from the limited-by role.
     *
     * @param applicationName checks privileges for the provided application name
     * @param checkForResources check permission grants for the set of resources
     * @param checkForPrivilegeNames check permission grants for the set of privilege names
     * @param storedPrivileges stored {@link ApplicationPrivilegeDescriptor} for an application against which the access checks are
     * performed
     * @return an instance of {@link ResourcePrivilegesMap}
     */
    @Override
    public ResourcePrivilegesMap checkApplicationResourcePrivileges(final String applicationName, Set<String> checkForResources,
                                                                    Set<String> checkForPrivilegeNames,
                                                                    Collection<ApplicationPrivilegeDescriptor> storedPrivileges) {
        ResourcePrivilegesMap resourcePrivilegesMap = super.application().checkResourcePrivileges(applicationName, checkForResources,
                checkForPrivilegeNames, storedPrivileges);
        ResourcePrivilegesMap resourcePrivilegesMapForLimitedRole = limitedBy.application().checkResourcePrivileges(applicationName,
                checkForResources, checkForPrivilegeNames, storedPrivileges);
        return ResourcePrivilegesMap.intersection(resourcePrivilegesMap, resourcePrivilegesMapForLimitedRole);
    }

    @Override
    public boolean checkRunAs(String runAs) {
        return super.checkRunAs(runAs) && limitedBy.checkRunAs(runAs);
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
        return new LimitedRole(fromRole.cluster(), fromRole.indices(), fromRole.application(), fromRole.runAs(), limitedByRole);
    }
}
