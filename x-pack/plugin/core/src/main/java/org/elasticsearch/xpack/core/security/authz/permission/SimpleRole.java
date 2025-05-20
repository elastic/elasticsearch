/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowsRestriction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SimpleRole implements Role {

    public static final Setting<Integer> CACHE_SIZE_SETTING = Setting.intSetting(
        "xpack.security.authz.store.roles.has_privileges.cache.max_size",
        1000,
        Setting.Property.NodeScope
    );

    private final String[] names;
    private final ClusterPermission cluster;
    private final IndicesPermission indices;
    private final ApplicationPermission application;
    private final RunAsPermission runAs;
    private final RemoteIndicesPermission remoteIndicesPermission;
    private final RemoteClusterPermissions remoteClusterPermissions;
    private final WorkflowsRestriction workflowsRestriction;

    SimpleRole(
        String[] names,
        ClusterPermission cluster,
        IndicesPermission indices,
        ApplicationPermission application,
        RunAsPermission runAs,
        RemoteIndicesPermission remoteIndicesPermission,
        RemoteClusterPermissions remoteClusterPermissions,
        WorkflowsRestriction workflowsRestriction
    ) {
        this.names = names;
        this.cluster = Objects.requireNonNull(cluster);
        this.indices = Objects.requireNonNull(indices);
        this.application = Objects.requireNonNull(application);
        this.runAs = Objects.requireNonNull(runAs);
        this.remoteIndicesPermission = Objects.requireNonNull(remoteIndicesPermission);
        this.remoteClusterPermissions = Objects.requireNonNull(remoteClusterPermissions);
        this.workflowsRestriction = Objects.requireNonNull(workflowsRestriction);
    }

    @Override
    public String[] names() {
        return names;
    }

    @Override
    public ClusterPermission cluster() {
        return cluster;
    }

    @Override
    public IndicesPermission indices() {
        return indices;
    }

    @Override
    public ApplicationPermission application() {
        return application;
    }

    @Override
    public RunAsPermission runAs() {
        return runAs;
    }

    @Override
    public RemoteIndicesPermission remoteIndices() {
        return remoteIndicesPermission;
    }

    @Override
    public RemoteClusterPermissions remoteCluster() {
        return remoteClusterPermissions;
    }

    @Override
    public boolean hasWorkflowsRestriction() {
        return workflowsRestriction.hasWorkflows();
    }

    @Override
    public Role forWorkflow(String workflow) {
        if (workflowsRestriction.isWorkflowAllowed(workflow)) {
            return this;
        } else {
            return EMPTY_RESTRICTED_BY_WORKFLOW;
        }
    }

    @Override
    public boolean hasFieldOrDocumentLevelSecurity() {
        return indices.hasFieldOrDocumentLevelSecurity();
    }

    @Override
    public IsResourceAuthorizedPredicate allowedIndicesMatcher(String action) {
        return indices.allowedIndicesMatcher(action);
    }

    @Override
    public Automaton allowedActionsMatcher(String index) {
        return indices.allowedActionsMatcher(index);
    }

    @Override
    public boolean checkRunAs(String runAsName) {
        return runAs.check(runAsName);
    }

    @Override
    public boolean checkIndicesAction(String action) {
        return indices.check(action);
    }

    @Override
    public boolean checkIndicesPrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        return indices.checkResourcePrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            resourcePrivilegesMapBuilder
        );
    }

    @Override
    public boolean checkClusterAction(String action, TransportRequest request, Authentication authentication) {
        return cluster.check(action, request, authentication);
    }

    @Override
    public boolean grants(ClusterPrivilege clusterPrivilege) {
        return cluster.implies(clusterPrivilege.buildPermission(ClusterPermission.builder()).build());
    }

    @Override
    public boolean checkApplicationResourcePrivileges(
        final String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        return application.checkResourcePrivileges(
            applicationName,
            checkForResources,
            checkForPrivilegeNames,
            storedPrivileges,
            resourcePrivilegesMapBuilder
        );
    }

    @Override
    public IndicesAccessControl authorize(
        String action,
        Set<String> requestedIndicesOrAliases,
        ProjectMetadata metadata,
        FieldPermissionsCache fieldPermissionsCache
    ) {
        return indices.authorize(action, requestedIndicesOrAliases, metadata, fieldPermissionsCache);
    }

    @Override
    public RoleDescriptorsIntersection getRoleDescriptorsIntersectionForRemoteCluster(
        final String remoteClusterAlias,
        TransportVersion remoteClusterVersion
    ) {
        final RemoteIndicesPermission remoteIndicesPermission = this.remoteIndicesPermission.forCluster(remoteClusterAlias);

        if (remoteIndicesPermission.remoteIndicesGroups().isEmpty()
            && remoteClusterPermissions.hasAnyPrivileges(remoteClusterAlias) == false) {
            return RoleDescriptorsIntersection.EMPTY;
        }

        final List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
        for (RemoteIndicesPermission.RemoteIndicesGroup remoteIndicesGroup : remoteIndicesPermission.remoteIndicesGroups()) {
            for (IndicesPermission.Group indicesGroup : remoteIndicesGroup.indicesPermissionGroups()) {
                indicesPrivileges.add(toIndicesPrivileges(indicesGroup));
            }
        }

        return new RoleDescriptorsIntersection(
            new RoleDescriptor(
                REMOTE_USER_ROLE_NAME,
                remoteClusterPermissions.collapseAndRemoveUnsupportedPrivileges(remoteClusterAlias, remoteClusterVersion),
                // The role descriptors constructed here may be cached in raw byte form, using a hash of their content as a
                // cache key; we therefore need deterministic order when constructing them here, to ensure cache hits for
                // equivalent role descriptors
                indicesPrivileges.stream().sorted().toArray(RoleDescriptor.IndicesPrivileges[]::new),
                null,
                null,
                null,
                null,
                null
            )
        );
    }

    private static Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> getFieldGrantExcludeGroups(IndicesPermission.Group group) {
        if (group.getFieldPermissions().hasFieldLevelSecurity()) {
            final List<FieldPermissionsDefinition> fieldPermissionsDefinitions = group.getFieldPermissions()
                .getFieldPermissionsDefinitions();
            assert fieldPermissionsDefinitions.size() == 1
                : "a simple role can only have up to one field permissions definition per remote indices privilege";
            final FieldPermissionsDefinition definition = fieldPermissionsDefinitions.get(0);
            return definition.getFieldGrantExcludeGroups();
        } else {
            return Collections.emptySet();
        }
    }

    private static RoleDescriptor.IndicesPrivileges toIndicesPrivileges(final IndicesPermission.Group indicesGroup) {
        final Set<BytesReference> queries = indicesGroup.getQuery();
        final Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldGrantExcludeGroups = getFieldGrantExcludeGroups(indicesGroup);
        assert queries == null || queries.size() <= 1
            : "translation from an indices permission group to indices privileges supports up to one DLS query but multiple queries found";
        assert fieldGrantExcludeGroups.size() <= 1
            : "translation from an indices permission group to indices privileges supports up to one FLS field-grant-exclude group"
                + " but multiple groups found";

        final BytesReference query = (queries == null || false == queries.iterator().hasNext()) ? null : queries.iterator().next();
        final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
            // Sort because these index privileges will be part of role descriptors that may be cached in raw byte form;
            // we need deterministic order to ensure cache hits for equivalent role descriptors
            .indices(Arrays.stream(indicesGroup.indices()).sorted().collect(Collectors.toList()))
            .privileges(indicesGroup.privilege().name().stream().sorted().collect(Collectors.toList()))
            .allowRestrictedIndices(indicesGroup.allowRestrictedIndices())
            .query(query);
        if (false == fieldGrantExcludeGroups.isEmpty()) {
            final FieldPermissionsDefinition.FieldGrantExcludeGroup fieldGrantExcludeGroup = fieldGrantExcludeGroups.iterator().next();
            builder.grantedFields(fieldGrantExcludeGroup.getGrantedFields()).deniedFields(fieldGrantExcludeGroup.getExcludedFields());
        }

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleRole that = (SimpleRole) o;
        return Arrays.equals(this.names, that.names)
            && this.cluster.equals(that.cluster)
            && this.indices.equals(that.indices)
            && this.application.equals(that.application)
            && this.runAs.equals(that.runAs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(cluster, indices, application, runAs);
        result = 31 * result + Arrays.hashCode(names);
        return result;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{" + String.join(",", names) + "}";
    }

    private final AtomicReference<Cache<PrivilegesToCheck, PrivilegesCheckResult>> hasPrivilegesCacheReference = new AtomicReference<>();

    public void cacheHasPrivileges(Settings settings, PrivilegesToCheck privilegesToCheck, PrivilegesCheckResult privilegesCheckResult)
        throws ExecutionException {
        Cache<PrivilegesToCheck, PrivilegesCheckResult> cache = hasPrivilegesCacheReference.get();
        if (cache == null) {
            final CacheBuilder<PrivilegesToCheck, PrivilegesCheckResult> cacheBuilder = CacheBuilder.builder();
            final int cacheSize = CACHE_SIZE_SETTING.get(settings);
            if (cacheSize >= 0) {
                cacheBuilder.setMaximumWeight(cacheSize);
            }
            hasPrivilegesCacheReference.compareAndSet(null, cacheBuilder.build());
            cache = hasPrivilegesCacheReference.get();
        }
        cache.computeIfAbsent(privilegesToCheck, ignore -> privilegesCheckResult);
    }

    public PrivilegesCheckResult checkPrivilegesWithCache(PrivilegesToCheck privilegesToCheck) {
        final Cache<PrivilegesToCheck, PrivilegesCheckResult> cache = hasPrivilegesCacheReference.get();
        if (cache == null) {
            return null;
        }
        return cache.get(privilegesToCheck);
    }

    // package private for testing
    Cache<PrivilegesToCheck, PrivilegesCheckResult> getHasPrivilegesCache() {
        return hasPrivilegesCacheReference.get();
    }
}
