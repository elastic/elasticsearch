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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public class Role {

    public static final Role EMPTY = Role.builder(Automatons.EMPTY, "__empty").build();

    private final String[] names;
    private final ClusterPermission cluster;
    private final IndicesPermission indices;
    private final ApplicationPermission application;
    private final RunAsPermission runAs;

    Role(String[] names, ClusterPermission cluster, IndicesPermission indices, ApplicationPermission application, RunAsPermission runAs) {
        this.names = names;
        this.cluster = Objects.requireNonNull(cluster);
        this.indices = Objects.requireNonNull(indices);
        this.application = Objects.requireNonNull(application);
        this.runAs = Objects.requireNonNull(runAs);
    }

    public String[] names() {
        return names;
    }

    public ClusterPermission cluster() {
        return cluster;
    }

    public IndicesPermission indices() {
        return indices;
    }

    public ApplicationPermission application() {
        return application;
    }

    public RunAsPermission runAs() {
        return runAs;
    }

    /**
     * @param restrictedIndices An automaton that can determine whether a string names
     *                          a restricted index. For simple unit tests, this can be
     *                          {@link Automatons#EMPTY}.
     * @param names Names of roles.
     * @return A builder for a role
     */
    public static Builder builder(Automaton restrictedIndices, String... names) {
        return new Builder(restrictedIndices, names);
    }

    public static Builder builder(RoleDescriptor rd, FieldPermissionsCache fieldPermissionsCache, Automaton restrictedIndices) {
        return new Builder(rd, fieldPermissionsCache, restrictedIndices);
    }

    /**
     * @return A predicate that will match all the indices that this role
     * has the privilege for executing the given action on.
     */
    public Predicate<IndexAbstraction> allowedIndicesMatcher(String action) {
        return indices.allowedIndicesMatcher(action);
    }

    public Automaton allowedActionsMatcher(String index) {
        return indices.allowedActionsMatcher(index);
    }

    public boolean checkRunAs(String runAsName) {
        return runAs.check(runAsName);
    }

    /**
     * Check if indices permissions allow for the given action
     *
     * @param action indices action
     * @return {@code true} if action is allowed else returns {@code false}
     */
    public boolean checkIndicesAction(String action) {
        return indices.check(action);
    }


    /**
     * For given index patterns and index privileges determines allowed privileges and creates an instance of {@link ResourcePrivilegesMap}
     * holding a map of resource to {@link ResourcePrivileges} where resource is index pattern and the map of index privilege to whether it
     * is allowed or not.
     *
     * @param checkForIndexPatterns check permission grants for the set of index patterns
     * @param allowRestrictedIndices if {@code true} then checks permission grants even for restricted indices by index matching
     * @param checkForPrivileges check permission grants for the set of index privileges
     * @return an instance of {@link ResourcePrivilegesMap}
     */
    public ResourcePrivilegesMap checkIndicesPrivileges(Set<String> checkForIndexPatterns, boolean allowRestrictedIndices,
                                                        Set<String> checkForPrivileges) {
        return indices.checkResourcePrivileges(checkForIndexPatterns, allowRestrictedIndices, checkForPrivileges);
    }

    /**
     * Check if cluster permissions allow for the given action in the context of given
     * authentication.
     *
     * @param action cluster action
     * @param request {@link TransportRequest}
     * @param authentication {@link Authentication}
     * @return {@code true} if action is allowed else returns {@code false}
     */
    public boolean checkClusterAction(String action, TransportRequest request, Authentication authentication) {
        return cluster.check(action, request, authentication);
    }

    /**
     * Check if cluster permissions grants the given cluster privilege
     *
     * @param clusterPrivilege cluster privilege
     * @return {@code true} if cluster privilege is allowed else returns {@code false}
     */
    public boolean grants(ClusterPrivilege clusterPrivilege) {
        return cluster.implies(clusterPrivilege.buildPermission(ClusterPermission.builder()).build());
    }

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
     * @return an instance of {@link ResourcePrivilegesMap}
     */
    public ResourcePrivilegesMap checkApplicationResourcePrivileges(final String applicationName, Set<String> checkForResources,
                                                                    Set<String> checkForPrivilegeNames,
                                                                    Collection<ApplicationPrivilegeDescriptor> storedPrivileges) {
        return application.checkResourcePrivileges(applicationName, checkForResources, checkForPrivilegeNames, storedPrivileges);
    }

    /**
     * Returns whether at least one group encapsulated by this indices permissions is authorized to execute the
     * specified action with the requested indices/aliases. At the same time if field and/or document level security
     * is configured for any group also the allowed fields and role queries are resolved.
     */
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases,
                                          Map<String, IndexAbstraction> aliasAndIndexLookup,
                                          FieldPermissionsCache fieldPermissionsCache) {
        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = indices.authorize(
            action, requestedIndicesOrAliases, aliasAndIndexLookup, fieldPermissionsCache
        );

        // At least one role / indices permission set need to match with all the requested indices/aliases:
        boolean granted = true;
        for (Map.Entry<String, IndicesAccessControl.IndexAccessControl> entry : indexPermissions.entrySet()) {
            if (entry.getValue().isGranted() == false) {
                granted = false;
                break;
            }
        }
        return new IndicesAccessControl(granted, indexPermissions);
    }

    public static class Builder {

        private final String[] names;
        private ClusterPermission cluster = ClusterPermission.NONE;
        private RunAsPermission runAs = RunAsPermission.NONE;
        private final List<IndicesPermissionGroupDefinition> groups = new ArrayList<>();
        private final List<Tuple<ApplicationPrivilege, Set<String>>> applicationPrivs = new ArrayList<>();
        private final Automaton restrictedNamesAutomaton;

        private Builder(Automaton restrictedNamesAutomaton, String[] names) {
            this.restrictedNamesAutomaton = restrictedNamesAutomaton;
            this.names = names;
        }

        private Builder(RoleDescriptor rd, @Nullable FieldPermissionsCache fieldPermissionsCache, Automaton restrictedNamesAutomaton) {
            this.names = new String[] { rd.getName() };
            cluster(Sets.newHashSet(rd.getClusterPrivileges()), Arrays.asList(rd.getConditionalClusterPrivileges()));
            groups.addAll(convertFromIndicesPrivileges(rd.getIndicesPrivileges(), fieldPermissionsCache));

            final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = rd.getApplicationPrivileges();
            for (RoleDescriptor.ApplicationResourcePrivileges applicationPrivilege : applicationPrivileges) {
                applicationPrivs.add(convertApplicationPrivilege(applicationPrivilege));
            }

            String[] rdRunAs = rd.getRunAs();
            if (rdRunAs != null && rdRunAs.length > 0) {
                this.runAs(new Privilege(Sets.newHashSet(rdRunAs), rdRunAs));
            }
            this.restrictedNamesAutomaton = restrictedNamesAutomaton;
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

        public Builder add(FieldPermissions fieldPermissions, Set<BytesReference> query, IndexPrivilege privilege,
                boolean allowRestrictedIndices, String... indices) {
            groups.add(new IndicesPermissionGroupDefinition(privilege, fieldPermissions, query, allowRestrictedIndices, indices));
            return this;
        }

        public Builder addApplicationPrivilege(ApplicationPrivilege privilege, Set<String> resources) {
            applicationPrivs.add(new Tuple<>(privilege, resources));
            return this;
        }

        public Role build() {
            final IndicesPermission indices;
            if (groups.isEmpty()) {
                indices = IndicesPermission.NONE;
            } else {
                IndicesPermission.Builder indicesBuilder = new IndicesPermission.Builder(restrictedNamesAutomaton);
                for (IndicesPermissionGroupDefinition group : groups) {
                    indicesBuilder.addGroup(group.privilege, group.fieldPermissions, group.query, group.allowRestrictedIndices,
                            group.indices);
                }
                indices = indicesBuilder.build();
            }
            final ApplicationPermission applicationPermission
                = applicationPrivs.isEmpty() ? ApplicationPermission.NONE : new ApplicationPermission(applicationPrivs);
            return new Role(names, cluster, indices, applicationPermission, runAs);
        }

        static List<IndicesPermissionGroupDefinition> convertFromIndicesPrivileges(RoleDescriptor.IndicesPrivileges[] indicesPrivileges,
                                                                          @Nullable FieldPermissionsCache fieldPermissionsCache) {
            List<IndicesPermissionGroupDefinition> list = new ArrayList<>(indicesPrivileges.length);
            for (RoleDescriptor.IndicesPrivileges privilege : indicesPrivileges) {
                final FieldPermissions fieldPermissions;
                if (fieldPermissionsCache != null) {
                    fieldPermissions = fieldPermissionsCache.getFieldPermissions(privilege.getGrantedFields(), privilege.getDeniedFields());
                } else {
                    fieldPermissions = new FieldPermissions(
                        new FieldPermissionsDefinition(privilege.getGrantedFields(), privilege.getDeniedFields()));
                }
                final Set<BytesReference> query = privilege.getQuery() == null ? null : Collections.singleton(privilege.getQuery());
                list.add(new IndicesPermissionGroupDefinition(IndexPrivilege.get(Sets.newHashSet(privilege.getPrivileges())),
                    fieldPermissions, query, privilege.allowRestrictedIndices(), privilege.getIndices()));
            }
            return list;
        }

        static Tuple<ApplicationPrivilege, Set<String>> convertApplicationPrivilege(RoleDescriptor.ApplicationResourcePrivileges arp) {
            return new Tuple<>(new ApplicationPrivilege(arp.getApplication(),
                Sets.newHashSet(arp.getPrivileges()),
                arp.getPrivileges()
            ), Sets.newHashSet(arp.getResources()));
        }

        private static class IndicesPermissionGroupDefinition {
            private final IndexPrivilege privilege;
            private final FieldPermissions fieldPermissions;
            private final @Nullable Set<BytesReference> query;
            private final boolean allowRestrictedIndices;
            private final String[] indices;

            private IndicesPermissionGroupDefinition(IndexPrivilege privilege,
                                                     FieldPermissions fieldPermissions,
                                                     @Nullable Set<BytesReference> query,
                                                     boolean allowRestrictedIndices,
                                                     String... indices) {
                this.privilege = privilege;
                this.fieldPermissions = fieldPermissions;
                this.query = query;
                this.allowRestrictedIndices = allowRestrictedIndices;
                this.indices = indices;
            }
        }
    }

}
