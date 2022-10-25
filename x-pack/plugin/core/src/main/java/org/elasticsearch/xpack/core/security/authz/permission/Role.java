/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public interface Role {

    Role EMPTY = builder(new RestrictedIndices(Automatons.EMPTY)).build();

    String[] names();

    ClusterPermission cluster();

    IndicesPermission indices();

    ApplicationPermission application();

    RunAsPermission runAs();

    RemoteIndicesPermission remoteIndices();

    /**
     * Whether the Role has any field or document level security enabled index privileges
     * @return
     */
    boolean hasFieldOrDocumentLevelSecurity();

    /**
     * @return A predicate that will match all the indices that this role
     * has the privilege for executing the given action on.
     */
    Predicate<IndexAbstraction> allowedIndicesMatcher(String action);

    /**
     * Returns an {@link Automaton} that matches all action names allowed for the given index
     */
    Automaton allowedActionsMatcher(String index);

    /**
     * Check if the role is allowed to run-as the given username.
     * @param runAsName
     * @return
     */
    boolean checkRunAs(String runAsName);

    /**
     * Check if indices permissions allow for the given action
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    boolean checkIndicesAction(String action);

    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcePrivilegesMap}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.
     *
     * @param checkForIndexPatterns check permission grants for the set of index patterns
     * @param allowRestrictedIndices if {@code true} then checks permission grants even for restricted indices by index matching
     * @param checkForPrivileges check permission grants for the set of index privileges
     * @param resourcePrivilegesMapBuilder out-parameter for returning the details on which privilege over which resource is granted or not.
     *                                     Can be {@code null} when no such details are needed so the method can return early, after
     *                                     encountering the first privilege that is not granted over some resource.
     * @return {@code true} when all the privileges are granted over all the resources, or {@code false} otherwise
     */
    boolean checkIndicesPrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    );

    /**
     * Check if cluster permissions allow for the given action in the context of given
     * authentication.
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @param authentication {@link Authentication}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    boolean checkClusterAction(String action, TransportRequest request, Authentication authentication);

    /**
     * Check if cluster permissions grants the given cluster privilege
     *
     * @param clusterPrivilege cluster privilege
     * @return {@code true} if cluster privilege is allowed else returns {@code false}
     */
    boolean grants(ClusterPrivilege clusterPrivilege);

    /**
     * For a given application, checks for the privileges for resources and returns an instance of {@link ResourcePrivilegesMap} holding a
     * map of resource to {@link ResourcePrivileges} where the resource is application resource and the map of application privilege to
     * whether it is allowed or not.
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
    boolean checkApplicationResourcePrivileges(
        String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    );

    /**
     * Returns whether at least one group encapsulated by this indices permissions is authorized to execute the
     * specified action with the requested indices/aliases. At the same time if field and/or document level security
     * is configured for any group also the allowed fields and role queries are resolved.
     */
    IndicesAccessControl authorize(
        String action,
        Set<String> requestedIndicesOrAliases,
        Map<String, IndexAbstraction> aliasAndIndexLookup,
        FieldPermissionsCache fieldPermissionsCache
    );

    /***
     * Creates a {@link LimitedRole} that uses this Role as base and the given role as limited-by.
     */
    default LimitedRole limitedBy(Role role) {
        return new LimitedRole(this, role);
    }

    /**
     * @param restrictedIndices An automaton that can determine whether a string names
     *                          a restricted index. For simple unit tests, this can be
     *                          {@link Automatons#EMPTY}.
     * @param names Names of roles.
     * @return A builder for a role
     */
    static Builder builder(RestrictedIndices restrictedIndices, String... names) {
        return new Builder(restrictedIndices, names);
    }

    class Builder {

        private final String[] names;
        private ClusterPermission cluster = ClusterPermission.NONE;
        private RunAsPermission runAs = RunAsPermission.NONE;
        private final List<IndicesPermissionGroupDefinition> groups = new ArrayList<>();
        private final Map<Set<String>, List<IndicesPermissionGroupDefinition>> remoteGroups = new HashMap<>();
        private final List<Tuple<ApplicationPrivilege, Set<String>>> applicationPrivs = new ArrayList<>();
        private final RestrictedIndices restrictedIndices;

        private Builder(RestrictedIndices restrictedIndices, String[] names) {
            this.restrictedIndices = restrictedIndices;
            this.names = names;
        }

        public Builder cluster(Set<String> privilegeNames, Iterable<ConfigurableClusterPrivilege> configurableClusterPrivileges) {
            ClusterPermission.Builder builder = ClusterPermission.builder();
            if (privilegeNames.isEmpty() == false) {
                for (String name : privilegeNames) {
                    builder = ClusterPrivilegeResolver.resolve(name).buildPermission(builder);
                }
            }
            for (ConfigurableClusterPrivilege ccp : configurableClusterPrivileges) {
                builder = ccp.buildPermission(builder);
            }
            this.cluster = builder.build();
            return this;
        }

        public Builder runAs(Privilege privilege) {
            runAs = new RunAsPermission(privilege);
            return this;
        }

        public Builder add(IndexPrivilege privilege, String... indices) {
            groups.add(new IndicesPermissionGroupDefinition(privilege, FieldPermissions.DEFAULT, null, false, indices));
            return this;
        }

        public Builder add(
            FieldPermissions fieldPermissions,
            Set<BytesReference> query,
            IndexPrivilege privilege,
            boolean allowRestrictedIndices,
            String... indices
        ) {
            groups.add(new IndicesPermissionGroupDefinition(privilege, fieldPermissions, query, allowRestrictedIndices, indices));
            return this;
        }

        public Builder addRemoteGroup(
            final Set<String> remoteClusterAliases,
            final FieldPermissions fieldPermissions,
            final Set<BytesReference> query,
            final IndexPrivilege privilege,
            final boolean allowRestrictedIndices,
            final String... indices
        ) {
            remoteGroups.computeIfAbsent(remoteClusterAliases, k -> new ArrayList<>())
                .add(new IndicesPermissionGroupDefinition(privilege, fieldPermissions, query, allowRestrictedIndices, indices));
            return this;
        }

        public Builder addApplicationPrivilege(ApplicationPrivilege privilege, Set<String> resources) {
            applicationPrivs.add(new Tuple<>(privilege, resources));
            return this;
        }

        public SimpleRole build() {
            final IndicesPermission indices;
            if (groups.isEmpty()) {
                indices = IndicesPermission.NONE;
            } else {
                IndicesPermission.Builder indicesBuilder = new IndicesPermission.Builder(restrictedIndices);
                for (IndicesPermissionGroupDefinition group : groups) {
                    indicesBuilder.addGroup(
                        group.privilege,
                        group.fieldPermissions,
                        group.query,
                        group.allowRestrictedIndices,
                        group.indices
                    );
                }
                indices = indicesBuilder.build();
            }

            final RemoteIndicesPermission remoteIndices;
            if (remoteGroups.isEmpty()) {
                remoteIndices = RemoteIndicesPermission.NONE;
            } else {
                final RemoteIndicesPermission.Builder remoteIndicesBuilder = new RemoteIndicesPermission.Builder();
                for (final Map.Entry<Set<String>, List<IndicesPermissionGroupDefinition>> remoteGroupEntry : remoteGroups.entrySet()) {
                    final var clusterAlias = remoteGroupEntry.getKey();
                    for (IndicesPermissionGroupDefinition group : remoteGroupEntry.getValue()) {
                        remoteIndicesBuilder.addGroup(
                            clusterAlias,
                            group.privilege,
                            group.fieldPermissions,
                            group.query,
                            group.allowRestrictedIndices,
                            group.indices
                        );
                    }
                }
                remoteIndices = remoteIndicesBuilder.build();
            }

            final ApplicationPermission applicationPermission = applicationPrivs.isEmpty()
                ? ApplicationPermission.NONE
                : new ApplicationPermission(applicationPrivs);
            return new SimpleRole(names, cluster, indices, applicationPermission, runAs, remoteIndices);
        }

        private static class IndicesPermissionGroupDefinition {
            private final IndexPrivilege privilege;
            private final FieldPermissions fieldPermissions;
            private final @Nullable Set<BytesReference> query;
            private final boolean allowRestrictedIndices;
            private final String[] indices;

            private IndicesPermissionGroupDefinition(
                IndexPrivilege privilege,
                FieldPermissions fieldPermissions,
                @Nullable Set<BytesReference> query,
                boolean allowRestrictedIndices,
                String... indices
            ) {
                this.privilege = privilege;
                this.fieldPermissions = fieldPermissions;
                this.query = query;
                this.allowRestrictedIndices = allowRestrictedIndices;
                this.indices = indices;
            }
        }
    }

    static SimpleRole buildFromRoleDescriptor(
        final RoleDescriptor roleDescriptor,
        final FieldPermissionsCache fieldPermissionsCache,
        final RestrictedIndices restrictedIndices
    ) {
        // TODO handle this when we introduce remote index privileges for built-in users and roles. That's the only production code
        // using this builder
        assert false == roleDescriptor.hasRemoteIndicesPrivileges();
        Objects.requireNonNull(fieldPermissionsCache);

        final Builder builder = builder(restrictedIndices, roleDescriptor.getName());

        builder.cluster(
            Sets.newHashSet(roleDescriptor.getClusterPrivileges()),
            Arrays.asList(roleDescriptor.getConditionalClusterPrivileges())
        );

        for (RoleDescriptor.IndicesPrivileges indexPrivilege : roleDescriptor.getIndicesPrivileges()) {
            builder.add(
                fieldPermissionsCache.getFieldPermissions(
                    new FieldPermissionsDefinition(indexPrivilege.getGrantedFields(), indexPrivilege.getDeniedFields())
                ),
                indexPrivilege.getQuery() == null ? null : Collections.singleton(indexPrivilege.getQuery()),
                IndexPrivilege.get(Sets.newHashSet(indexPrivilege.getPrivileges())),
                indexPrivilege.allowRestrictedIndices(),
                indexPrivilege.getIndices()
            );
        }

        for (RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege : roleDescriptor.getApplicationPrivileges()) {
            builder.addApplicationPrivilege(
                new ApplicationPrivilege(
                    applicationPrivilege.getApplication(),
                    Sets.newHashSet(applicationPrivilege.getPrivileges()),
                    applicationPrivilege.getPrivileges()
                ),
                Sets.newHashSet(applicationPrivilege.getResources())
            );
        }

        final String[] rdRunAs = roleDescriptor.getRunAs();
        if (rdRunAs != null && rdRunAs.length > 0) {
            builder.runAs(new Privilege(Sets.newHashSet(rdRunAs), rdRunAs));
        }

        return builder.build();
    }
}
