/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class Role {

    public static final Role EMPTY = Role.builder("__empty").build();

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

    public static Builder builder(String... names) {
        return new Builder(names);
    }

    public static Builder builder(RoleDescriptor rd, FieldPermissionsCache fieldPermissionsCache) {
        return new Builder(rd, fieldPermissionsCache);
    }

    /**
     * Returns whether at least one group encapsulated by this indices permissions is authorized to execute the
     * specified action with the requested indices/aliases. At the same time if field and/or document level security
     * is configured for any group also the allowed fields and role queries are resolved.
     */
    public IndicesAccessControl authorize(String action, Set<String> requestedIndicesOrAliases, MetaData metaData,
                                          FieldPermissionsCache fieldPermissionsCache) {
        Map<String, IndicesAccessControl.IndexAccessControl> indexPermissions = indices.authorize(
            action, requestedIndicesOrAliases, metaData, fieldPermissionsCache
        );

        // At least one role / indices permission set need to match with all the requested indices/aliases:
        boolean granted = true;
        for (Map.Entry<String, IndicesAccessControl.IndexAccessControl> entry : indexPermissions.entrySet()) {
            if (!entry.getValue().isGranted()) {
                granted = false;
                break;
            }
        }
        return new IndicesAccessControl(granted, indexPermissions);
    }

    public static class Builder {

        private final String[] names;
        private ClusterPermission cluster = ClusterPermission.SimpleClusterPermission.NONE;
        private RunAsPermission runAs = RunAsPermission.NONE;
        private List<IndicesPermission.Group> groups = new ArrayList<>();
        private List<Tuple<ApplicationPrivilege, Set<String>>> applicationPrivs = new ArrayList<>();

        private Builder(String[] names) {
            this.names = names;
        }

        private Builder(RoleDescriptor rd, @Nullable FieldPermissionsCache fieldPermissionsCache) {
            this.names = new String[] { rd.getName() };
            cluster(Sets.newHashSet(rd.getClusterPrivileges()), Arrays.asList(rd.getConditionalClusterPrivileges()));
            groups.addAll(convertFromIndicesPrivileges(rd.getIndicesPrivileges(), fieldPermissionsCache));

            final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = rd.getApplicationPrivileges();
            for (int i = 0; i < applicationPrivileges.length; i++) {
                applicationPrivs.add(convertApplicationPrivilege(rd.getName(), i, applicationPrivileges[i]));
            }

            String[] rdRunAs = rd.getRunAs();
            if (rdRunAs != null && rdRunAs.length > 0) {
                this.runAs(new Privilege(Sets.newHashSet(rdRunAs), rdRunAs));
            }
        }

        public Builder cluster(Set<String> privilegeNames, Iterable<ConditionalClusterPrivilege> conditionalClusterPrivileges) {
            List<ClusterPermission> clusterPermissions = new ArrayList<>();
            if (privilegeNames.isEmpty() == false) {
                clusterPermissions.add(new ClusterPermission.SimpleClusterPermission(ClusterPrivilege.get(privilegeNames)));
            }
            for (ConditionalClusterPrivilege ccp : conditionalClusterPrivileges) {
                clusterPermissions.add(new ClusterPermission.ConditionalClusterPermission(ccp));
            }
            if (clusterPermissions.isEmpty()) {
                this.cluster = ClusterPermission.SimpleClusterPermission.NONE;
            } else if (clusterPermissions.size() == 1) {
                this.cluster = clusterPermissions.get(0);
            } else {
                this.cluster = new ClusterPermission.CompositeClusterPermission(clusterPermissions);
            }
            return this;
        }

        /**
         * @deprecated Use {@link #cluster(Set, Iterable)}
         */
        @Deprecated
        public Builder cluster(ClusterPrivilege privilege) {
            cluster = new ClusterPermission.SimpleClusterPermission(privilege);
            return this;
        }

        public Builder runAs(Privilege privilege) {
            runAs = new RunAsPermission(privilege);
            return this;
        }

        public Builder add(IndexPrivilege privilege, String... indices) {
            groups.add(new IndicesPermission.Group(privilege, FieldPermissions.DEFAULT, null, indices));
            return this;
        }

        public Builder add(FieldPermissions fieldPermissions, Set<BytesReference> query, IndexPrivilege privilege, String... indices) {
            groups.add(new IndicesPermission.Group(privilege, fieldPermissions, query, indices));
            return this;
        }

        public Builder addApplicationPrivilege(ApplicationPrivilege privilege, Set<String> resources) {
            applicationPrivs.add(new Tuple<>(privilege, resources));
            return this;
        }

        public Role build() {
            IndicesPermission indices = groups.isEmpty() ? IndicesPermission.NONE :
                new IndicesPermission(groups.toArray(new IndicesPermission.Group[groups.size()]));
            final ApplicationPermission applicationPermission
                = applicationPrivs.isEmpty() ? ApplicationPermission.NONE : new ApplicationPermission(applicationPrivs);
            return new Role(names, cluster, indices, applicationPermission, runAs);
        }

        static List<IndicesPermission.Group> convertFromIndicesPrivileges(RoleDescriptor.IndicesPrivileges[] indicesPrivileges,
                                                                          @Nullable FieldPermissionsCache fieldPermissionsCache) {
            List<IndicesPermission.Group> list = new ArrayList<>(indicesPrivileges.length);
            for (RoleDescriptor.IndicesPrivileges privilege : indicesPrivileges) {
                final FieldPermissions fieldPermissions;
                if (fieldPermissionsCache != null) {
                    fieldPermissions = fieldPermissionsCache.getFieldPermissions(privilege.getGrantedFields(), privilege.getDeniedFields());
                } else {
                    fieldPermissions = new FieldPermissions(
                        new FieldPermissionsDefinition(privilege.getGrantedFields(), privilege.getDeniedFields()));
                }
                final Set<BytesReference> query = privilege.getQuery() == null ? null : Collections.singleton(privilege.getQuery());
                list.add(new IndicesPermission.Group(IndexPrivilege.get(Sets.newHashSet(privilege.getPrivileges())),
                    fieldPermissions,
                    query,
                    privilege.getIndices()));

            }
            return list;
        }

        static Tuple<ApplicationPrivilege, Set<String>> convertApplicationPrivilege(String role, int index,
                                                                                    RoleDescriptor.ApplicationResourcePrivileges arp) {
            return new Tuple<>(new ApplicationPrivilege(arp.getApplication(),
                Sets.newHashSet(arp.getPrivileges()),
                arp.getPrivileges()
            ), Sets.newHashSet(arp.getResources()));
        }
    }
}
