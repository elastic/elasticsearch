/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.support.MetadataUtils.RESERVED_METADATA_KEY;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtils.determineRolesToDelete;
import static org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtils.determineRolesToUpsert;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class QueryableBuiltInRolesUtilsTests extends ESTestCase {

    @BeforeClass
    public static void setupReservedRolesStore() {
        new ReservedRolesStore(); // initialize the store
    }

    public void testCalculateHash() {
        assertThat(
            QueryableBuiltInRolesUtils.calculateHash(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR),
            equalTo("bWEFdFo4WX229wdhdecfiz5QHMYEssh3ex8hizRgg+Q=")
        );
    }

    public void testEmptyOrNullRolesToUpsertOrDelete() {
        // test empty roles and index digests
        final QueryableBuiltInRoles emptyRoles = new QueryableBuiltInRoles(Map.of(), Set.of());
        assertThat(determineRolesToDelete(emptyRoles, Map.of()), is(empty()));
        assertThat(determineRolesToUpsert(emptyRoles, Map.of()), is(empty()));

        // test empty roles and null indexed digests
        assertThat(determineRolesToDelete(emptyRoles, null), is(empty()));
        assertThat(determineRolesToUpsert(emptyRoles, null), is(empty()));
    }

    public void testNoRolesToUpsertOrDelete() {
        {
            QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
                Set.of(
                    ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                    ReservedRolesStore.roleDescriptor("viewer"),
                    ReservedRolesStore.roleDescriptor("editor")
                )
            );

            // no roles to delete or upsert since the built-in roles are the same as the indexed roles
            assertThat(determineRolesToDelete(currentBuiltInRoles, currentBuiltInRoles.rolesDigest()), is(empty()));
            assertThat(determineRolesToUpsert(currentBuiltInRoles, currentBuiltInRoles.rolesDigest()), is(empty()));
        }
        {
            QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
                Set.of(
                    ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                    ReservedRolesStore.roleDescriptor("viewer"),
                    ReservedRolesStore.roleDescriptor("editor"),
                    supermanRole("monitor", "read")
                )
            );

            Map<String, String> digests = buildDigests(
                Set.of(
                    ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                    ReservedRolesStore.roleDescriptor("viewer"),
                    ReservedRolesStore.roleDescriptor("editor"),
                    supermanRole("monitor", "read")
                )
            );

            // no roles to delete or upsert since the built-in roles are the same as the indexed roles
            assertThat(determineRolesToDelete(currentBuiltInRoles, digests), is(empty()));
            assertThat(determineRolesToUpsert(currentBuiltInRoles, digests), is(empty()));
        }
        {
            final RoleDescriptor randomRole = RoleDescriptorTestHelper.randomRoleDescriptor();
            final QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(Set.of(randomRole));
            final Map<String, String> digests = buildDigests(
                Set.of(
                    new RoleDescriptor(
                        randomRole.getName(),
                        randomRole.getClusterPrivileges(),
                        randomRole.getIndicesPrivileges(),
                        randomRole.getApplicationPrivileges(),
                        randomRole.getConditionalClusterPrivileges(),
                        randomRole.getRunAs(),
                        randomRole.getMetadata(),
                        randomRole.getTransientMetadata(),
                        randomRole.getRemoteIndicesPrivileges(),
                        randomRole.getRemoteClusterPermissions(),
                        randomRole.getRestriction(),
                        randomRole.getDescription()
                    )
                )
            );

            assertThat(determineRolesToDelete(currentBuiltInRoles, digests), is(empty()));
            assertThat(determineRolesToUpsert(currentBuiltInRoles, digests), is(empty()));
        }
    }

    public void testRolesToDeleteOnly() {
        Map<String, String> indexedDigests = buildDigests(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor"),
                supermanRole("monitor", "read", "view_index_metadata", "read_cross_cluster")
            )
        );

        QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor")
            )
        );

        // superman is the only role that needs to be deleted since it is not in a current built-in role
        assertThat(determineRolesToDelete(currentBuiltInRoles, indexedDigests), containsInAnyOrder("superman"));
        assertThat(determineRolesToUpsert(currentBuiltInRoles, indexedDigests), is(empty()));

        // passing empty built-in roles should result in all indexed roles needing to be deleted
        QueryableBuiltInRoles emptyBuiltInRoles = new QueryableBuiltInRoles(Map.of(), Set.of());
        assertThat(
            determineRolesToDelete(emptyBuiltInRoles, indexedDigests),
            containsInAnyOrder("superman", "viewer", "editor", "superuser")
        );
        assertThat(determineRolesToUpsert(emptyBuiltInRoles, indexedDigests), is(empty()));
    }

    public void testRolesToUpdateOnly() {
        Map<String, String> indexedDigests = buildDigests(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor"),
                supermanRole("monitor", "read", "write")
            )
        );

        RoleDescriptor updatedSupermanRole = supermanRole("monitor", "read", "view_index_metadata", "read_cross_cluster");
        QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor"),
                updatedSupermanRole
            )
        );

        // superman is the only role that needs to be updated since its definition has changed
        assertThat(determineRolesToDelete(currentBuiltInRoles, indexedDigests), is(empty()));
        assertThat(determineRolesToUpsert(currentBuiltInRoles, indexedDigests), containsInAnyOrder(updatedSupermanRole));
        assertThat(currentBuiltInRoles.rolesDigest().get("superman"), is(not(equalTo(indexedDigests.get("superman")))));
    }

    public void testRolesToCreateOnly() {
        Map<String, String> indexedDigests = buildDigests(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor")
            )
        );

        RoleDescriptor newSupermanRole = supermanRole("monitor", "read", "view_index_metadata", "read_cross_cluster");
        QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor"),
                newSupermanRole
            )
        );

        // superman is the only role that needs to be created since it is not in the indexed roles
        assertThat(determineRolesToDelete(currentBuiltInRoles, indexedDigests), is(empty()));
        assertThat(determineRolesToUpsert(currentBuiltInRoles, indexedDigests), containsInAnyOrder(newSupermanRole));

        // passing empty indexed roles should result in all roles needing to be created
        assertThat(determineRolesToDelete(currentBuiltInRoles, Map.of()), is(empty()));
        assertThat(
            determineRolesToUpsert(currentBuiltInRoles, Map.of()),
            containsInAnyOrder(currentBuiltInRoles.roleDescriptors().toArray(new RoleDescriptor[0]))
        );
    }

    public void testRolesToUpsertAndDelete() {
        Map<String, String> indexedDigests = buildDigests(
            Set.of(
                ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
                ReservedRolesStore.roleDescriptor("viewer"),
                ReservedRolesStore.roleDescriptor("editor")
            )
        );

        RoleDescriptor newSupermanRole = supermanRole("monitor");
        QueryableBuiltInRoles currentBuiltInRoles = buildQueryableBuiltInRoles(
            Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR, newSupermanRole)
        );

        // superman is the only role that needs to be updated since its definition has changed
        assertThat(determineRolesToDelete(currentBuiltInRoles, indexedDigests), containsInAnyOrder("viewer", "editor"));
        assertThat(determineRolesToUpsert(currentBuiltInRoles, indexedDigests), containsInAnyOrder(newSupermanRole));
    }

    private static RoleDescriptor supermanRole(String... indicesPrivileges) {
        return new RoleDescriptor(
            "superman",
            new String[] { "all" },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(false).build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges(indicesPrivileges)
                    .allowRestrictedIndices(true)
                    .build() },
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build() },
            null,
            new String[] { "*" },
            randomlyOrderedSupermanMetadata(),
            Collections.emptyMap(),
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                new RoleDescriptor.RemoteIndicesPrivileges(
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").allowRestrictedIndices(false).build(),
                    "*"
                ),
                new RoleDescriptor.RemoteIndicesPrivileges(
                    RoleDescriptor.IndicesPrivileges.builder()
                        .indices("*")
                        .privileges(indicesPrivileges)
                        .allowRestrictedIndices(true)
                        .build(),
                    "*"
                ) },
            new RemoteClusterPermissions().addGroup(
                new RemoteClusterPermissionGroup(
                    RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]),
                    new String[] { "*" }
                )
            ),
            null,
            "Grants full access to cluster management and data indices."
        );
    }

    private static Map<String, Object> randomlyOrderedSupermanMetadata() {
        final LinkedHashMap<String, Object> metadata = new LinkedHashMap<>();
        if (randomBoolean()) {
            metadata.put("foo", "bar");
            metadata.put("baz", "qux");
            metadata.put(RESERVED_METADATA_KEY, true);
        } else {
            metadata.put(RESERVED_METADATA_KEY, true);
            metadata.put("foo", "bar");
            metadata.put("baz", "qux");
        }
        return metadata;
    }

    public static QueryableBuiltInRoles buildQueryableBuiltInRoles(Set<RoleDescriptor> roles) {
        final Map<String, String> digests = buildDigests(roles);
        return new QueryableBuiltInRoles(digests, roles);
    }

    public static Map<String, String> buildDigests(Set<RoleDescriptor> roles) {
        final Map<String, String> digests = new HashMap<>();
        for (RoleDescriptor role : roles) {
            digests.put(role.getName(), QueryableBuiltInRolesUtils.calculateHash(role));
        }
        return digests;
    }
}
