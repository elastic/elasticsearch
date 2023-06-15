/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link Role} limited by another role.<br>
 * The effective permissions returned on {@link #authorize(String, Set, Map, FieldPermissionsCache)} call would be limited by the
 * provided role.
 */
public final class LimitedRole implements Role {

    private static final Logger logger = LogManager.getLogger(LimitedRole.class);
    private final Role baseRole;
    private final Role limitedByRole;

    /**
     * Create a new role defined by given role and the limited role.
     *
     * @param baseRole existing role {@link Role}
     * @param limitedByRole restrict the newly formed role to the permissions defined by this limited {@link Role}
     */
    public LimitedRole(Role baseRole, Role limitedByRole) {
        this.baseRole = Objects.requireNonNull(baseRole);
        this.limitedByRole = Objects.requireNonNull(limitedByRole, "limited by role is required to create limited role");
        assert false == limitedByRole.hasWorkflowsRestriction() : "limited-by role must not have workflows restriction";
    }

    @Override
    public String[] names() {
        // TODO: this is to retain existing behaviour, but it is not accurate
        return limitedByRole.names();
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
    public RemoteIndicesPermission remoteIndices() {
        throw new UnsupportedOperationException("cannot retrieve remote indices permission on limited role");
    }

    @Override
    public boolean hasWorkflowsRestriction() {
        return baseRole.hasWorkflowsRestriction() || limitedByRole.hasWorkflowsRestriction();
    }

    @Override
    public Role forWorkflow(String workflow) {
        Role baseRestricted = baseRole.forWorkflow(workflow);
        if (baseRestricted == EMPTY_RESTRICTED_BY_WORKFLOW) {
            return EMPTY_RESTRICTED_BY_WORKFLOW;
        }
        Role limitedByRestricted = limitedByRole.forWorkflow(workflow);
        if (limitedByRestricted == EMPTY_RESTRICTED_BY_WORKFLOW) {
            return EMPTY_RESTRICTED_BY_WORKFLOW;
        }
        return baseRestricted.limitedBy(limitedByRestricted);
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
    public boolean hasFieldOrDocumentLevelSecurity() {
        return baseRole.hasFieldOrDocumentLevelSecurity() || limitedByRole.hasFieldOrDocumentLevelSecurity();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LimitedRole that = (LimitedRole) o;
        return baseRole.equals(that.baseRole) && this.limitedByRole.equals(that.limitedByRole);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseRole, limitedByRole);
    }

    @Override
    public IndicesAccessControl authorize(
        String action,
        Set<String> requestedIndicesOrAliases,
        Map<String, IndexAbstraction> aliasAndIndexLookup,
        FieldPermissionsCache fieldPermissionsCache
    ) {
        IndicesAccessControl indicesAccessControl = baseRole.authorize(
            action,
            requestedIndicesOrAliases,
            aliasAndIndexLookup,
            fieldPermissionsCache
        );
        IndicesAccessControl limitedByIndicesAccessControl = limitedByRole.authorize(
            action,
            requestedIndicesOrAliases,
            aliasAndIndexLookup,
            fieldPermissionsCache
        );
        return indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
    }

    @Override
    public RoleDescriptorsIntersection getRoleDescriptorsIntersectionForRemoteCluster(final String remoteClusterAlias) {
        final RoleDescriptorsIntersection baseIntersection = baseRole.getRoleDescriptorsIntersectionForRemoteCluster(remoteClusterAlias);
        // Intersecting with empty descriptors list should result in an empty intersection.
        if (baseIntersection.roleDescriptorsList().isEmpty()) {
            logger.trace(
                () -> "Base role ["
                    + Strings.arrayToCommaDelimitedString(baseRole.names())
                    + "] does not define any role descriptors for remote cluster alias ["
                    + remoteClusterAlias
                    + "]"
            );
            return RoleDescriptorsIntersection.EMPTY;
        }
        final RoleDescriptorsIntersection limitedByIntersection = limitedByRole.getRoleDescriptorsIntersectionForRemoteCluster(
            remoteClusterAlias
        );
        if (limitedByIntersection.roleDescriptorsList().isEmpty()) {
            logger.trace(
                () -> "Limited-by role ["
                    + Strings.arrayToCommaDelimitedString(limitedByRole.names())
                    + "] does not define any role descriptors for remote cluster alias ["
                    + remoteClusterAlias
                    + "]"
            );
            return RoleDescriptorsIntersection.EMPTY;
        }
        final List<Set<RoleDescriptor>> mergedIntersection = new ArrayList<>(
            baseIntersection.roleDescriptorsList().size() + limitedByIntersection.roleDescriptorsList().size()
        );
        mergedIntersection.addAll(baseIntersection.roleDescriptorsList());
        mergedIntersection.addAll(limitedByIntersection.roleDescriptorsList());
        return new RoleDescriptorsIntersection(Collections.unmodifiableList(mergedIntersection));
    }

    /**
     * @return A predicate that will match all the indices that this role and the limited by role has the privilege for executing the given
     * action on.
     */
    @Override
    public IsResourceAuthorizedPredicate allowedIndicesMatcher(String action) {
        return baseRole.allowedIndicesMatcher(action).and(limitedByRole.allowedIndicesMatcher(action));
    }

    @Override
    public Automaton allowedActionsMatcher(String index) {
        final Automaton allowedMatcher = baseRole.allowedActionsMatcher(index);
        final Automaton limitedByMatcher = limitedByRole.allowedActionsMatcher(index);
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
        return baseRole.checkIndicesAction(action) && limitedByRole.checkIndicesAction(action);
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
     * @param resourcePrivilegesMapBuilder out-parameter for returning the details on which privilege over which resource is granted or not.
     *                                     Can be {@code null} when no such details are needed so the method can return early, after
     *                                     encountering the first privilege that is not granted over some resource.
     * @return {@code true} when all the privileges are granted over all the resources, or {@code false} otherwise
     */
    @Override
    public boolean checkIndicesPrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        boolean baseRoleCheck = baseRole.checkIndicesPrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            resourcePrivilegesMapBuilder
        );
        if (false == baseRoleCheck && null == resourcePrivilegesMapBuilder) {
            // short-circuit only if not interested in the detailed individual check results
            return false;
        }
        boolean limitedByRoleCheck = limitedByRole.checkIndicesPrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            resourcePrivilegesMapBuilder
        );
        return baseRoleCheck && limitedByRoleCheck;
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
        return baseRole.checkClusterAction(action, request, authentication)
            && limitedByRole.checkClusterAction(action, request, authentication);
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
        return baseRole.grants(clusterPrivilege) && limitedByRole.grants(clusterPrivilege);
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
     * @param resourcePrivilegesMapBuilder out-parameter for returning the details on which privilege over which resource is granted or not.
     *                                     Can be {@code null} when no such details are needed so the method can return early, after
     *                                     encountering the first privilege that is not granted over some resource.
     * @return {@code true} when all the privileges are granted over all the resources, or {@code false} otherwise
     */
    @Override
    public boolean checkApplicationResourcePrivileges(
        String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        boolean baseRoleCheck = baseRole.application()
            .checkResourcePrivileges(
                applicationName,
                checkForResources,
                checkForPrivilegeNames,
                storedPrivileges,
                resourcePrivilegesMapBuilder
            );
        if (false == baseRoleCheck && null == resourcePrivilegesMapBuilder) {
            // short-circuit only if not interested in the detailed individual check results
            return false;
        }
        boolean limitedByRoleCheck = limitedByRole.application()
            .checkResourcePrivileges(
                applicationName,
                checkForResources,
                checkForPrivilegeNames,
                storedPrivileges,
                resourcePrivilegesMapBuilder
            );
        return baseRoleCheck && limitedByRoleCheck;
    }

    @Override
    public boolean checkRunAs(String runAs) {
        return baseRole.checkRunAs(runAs) && limitedByRole.checkRunAs(runAs);
    }
}
