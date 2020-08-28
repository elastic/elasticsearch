/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;

public class AuthorizedIndicesTests extends ESTestCase {

    public void testAuthorizedIndicesUserWithoutRoles() {
        List<String> authorizedIndices =
            RBACEngine.resolveAuthorizedIndicesFromRole(Role.EMPTY, getRequestInfo(""), Metadata.EMPTY_METADATA.getIndicesLookup());
        assertTrue(authorizedIndices.isEmpty());
    }

    public void testAuthorizedIndicesUserWithSomeRoles() {
        RoleDescriptor aStarRole = new RoleDescriptor("a_star", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() }, null);
        RoleDescriptor bRole = new RoleDescriptor("b", null,
                new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() }, null);
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        Metadata metadata = Metadata.builder()
                .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("b")
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder("ab").build())
                        .putAlias(new AliasMetadata.Builder("ba").build())
                        .build(), true)
                .put(new IndexMetadata.Builder(internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        Role roles = future.actionGet();
        List<String> list =
            RBACEngine.resolveAuthorizedIndicesFromRole(roles, getRequestInfo(SearchAction.NAME), metadata.getIndicesLookup());
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertFalse(list.contains("bbbbb"));
        assertFalse(list.contains("ba"));
        assertThat(list, not(contains(internalSecurityIndex)));
        assertThat(list, not(contains(RestrictedIndicesNames.SECURITY_MAIN_ALIAS)));
    }

    public void testAuthorizedIndicesUserWithSomeRolesEmptyMetadata() {
        Role role = Role.builder("role").add(IndexPrivilege.ALL, "*").build();
        List<String> authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(SearchAction.NAME),
            Metadata.EMPTY_METADATA.getIndicesLookup());
        assertTrue(authorizedIndices.isEmpty());
    }

    public void testSecurityIndicesAreRemovedFromRegularUser() {
        Role role = Role.builder("user_role").add(IndexPrivilege.ALL, "*").cluster(Set.of("all"), Set.of()).build();
        List<String> authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(SearchAction.NAME),
            Metadata.EMPTY_METADATA.getIndicesLookup());
        assertTrue(authorizedIndices.isEmpty());
    }

    public void testSecurityIndicesAreRestrictedForDefaultRole() {
        Role role = Role.builder(randomFrom("user_role", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))
                .add(IndexPrivilege.ALL, "*")
                .cluster(Set.of("all"), Set.of())
                .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        Metadata metadata = Metadata.builder()
                .put(new IndexMetadata.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder(
                    internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();

        List<String> authorizedIndices =
            RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(SearchAction.NAME), metadata.getIndicesLookup());
        assertThat(authorizedIndices, containsInAnyOrder("an-index", "another-index"));
        assertThat(authorizedIndices, not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices, not(contains(RestrictedIndicesNames.SECURITY_MAIN_ALIAS)));
    }

    public void testSecurityIndicesAreNotRemovedFromUnrestrictedRole() {
        Role role = Role.builder(randomAlphaOfLength(8))
                .add(FieldPermissions.DEFAULT, null, IndexPrivilege.ALL, true, "*")
                .cluster(Set.of("all"), Set.of())
                .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        Metadata metadata = Metadata.builder()
                .put(new IndexMetadata.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder(internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();

        List<String> authorizedIndices =
            RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(SearchAction.NAME), metadata.getIndicesLookup());
        assertThat(authorizedIndices, containsInAnyOrder(
            "an-index", "another-index", RestrictedIndicesNames.SECURITY_MAIN_ALIAS, internalSecurityIndex));

        List<String> authorizedIndicesSuperUser =
            RBACEngine.resolveAuthorizedIndicesFromRole(role, getRequestInfo(SearchAction.NAME), metadata.getIndicesLookup());
        assertThat(authorizedIndicesSuperUser, containsInAnyOrder(
            "an-index", "another-index", RestrictedIndicesNames.SECURITY_MAIN_ALIAS, internalSecurityIndex));
    }

    public void testDataStreamsAreNotIncludedInAuthorizedIndices() {
        RoleDescriptor aStarRole = new RoleDescriptor("a_star", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() }, null);
        RoleDescriptor bRole = new RoleDescriptor("b", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() }, null);
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("b")
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder("ab").build())
                .putAlias(new AliasMetadata.Builder("ba").build())
                .build(), true)
            .put(new IndexMetadata.Builder(internalSecurityIndex)
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                .build(), true)
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new DataStream("adatastream1", createTimestampField("@timestamp"),
                List.of(new Index(DataStream.getDefaultBackingIndexName("adatastream1", 1), "_na_"))))
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        Role roles = future.actionGet();
        List<String> list =
            RBACEngine.resolveAuthorizedIndicesFromRole(roles, getRequestInfo(SearchAction.NAME), metadata.getIndicesLookup());
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertFalse(list.contains("bbbbb"));
        assertFalse(list.contains("ba"));
        assertFalse(list.contains("adatastream1"));
        assertThat(list, not(contains(internalSecurityIndex)));
        assertThat(list, not(contains(RestrictedIndicesNames.SECURITY_MAIN_ALIAS)));
    }

    public void testDataStreamsAreIncludedInAuthorizedIndices() {
        RoleDescriptor aStarRole = new RoleDescriptor("a_star", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() }, null);
        RoleDescriptor bRole = new RoleDescriptor("b", null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() }, null);
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("b")
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder("ab").build())
                .putAlias(new AliasMetadata.Builder("ba").build())
                .build(), true)
            .put(new IndexMetadata.Builder(internalSecurityIndex)
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                .build(), true)
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new DataStream("adatastream1", createTimestampField("@timestamp"),
                List.of(new Index(DataStream.getDefaultBackingIndexName("adatastream1", 1), "_na_"))))
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(descriptors, new FieldPermissionsCache(Settings.EMPTY), null, future);
        Role roles = future.actionGet();
        TransportRequest request = new ResolveIndexAction.Request(new String[]{"a*"});
        AuthorizationEngine.RequestInfo requestInfo = new AuthorizationEngine.RequestInfo(null, request, SearchAction.NAME);
        List<String> list =
            RBACEngine.resolveAuthorizedIndicesFromRole(roles, requestInfo, metadata.getIndicesLookup());
        assertThat(list, containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab", "adatastream1", backingIndex));
        assertFalse(list.contains("bbbbb"));
        assertFalse(list.contains("ba"));
        assertThat(list, not(contains(internalSecurityIndex)));
        assertThat(list, not(contains(RestrictedIndicesNames.SECURITY_MAIN_ALIAS)));
    }

    public static AuthorizationEngine.RequestInfo getRequestInfo(String action) {
        return getRequestInfo(TransportRequest.Empty.INSTANCE, action);
    }

    public static AuthorizationEngine.RequestInfo getRequestInfo(TransportRequest request, String action) {
        return new AuthorizationEngine.RequestInfo(null, request, action);
    }
}
