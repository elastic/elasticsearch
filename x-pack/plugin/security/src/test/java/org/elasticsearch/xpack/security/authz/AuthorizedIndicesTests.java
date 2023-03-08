/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class AuthorizedIndicesTests extends ESTestCase {

    public void testAuthorizedIndicesUserWithoutRoles() {
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            Role.EMPTY,
            getRequestInfo(""),
            Metadata.EMPTY_METADATA.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all().get().isEmpty());
    }

    public void testAuthorizedIndicesUserWithSomeRoles() {
        RoleDescriptor aStarRole = new RoleDescriptor(
            "a_star",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() },
            null
        );
        RoleDescriptor bRole = new RoleDescriptor(
            "b",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() },
            null
        );
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder("b").settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder("ab").build())
                    .putAlias(new AliasMetadata.Builder("ba").build())
                    .build(),
                true
            )
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(
            descriptors,
            new FieldPermissionsCache(Settings.EMPTY),
            null,
            RESTRICTED_INDICES,
            future
        );
        Role roles = future.actionGet();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            getRequestInfo(SearchAction.NAME),
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get(), containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertThat(authorizedIndices.all().get(), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb"), is(false));
        assertThat(authorizedIndices.all().get(), not(contains("ba")));
        assertThat(authorizedIndices.check("ba"), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(false));
    }

    public void testAuthorizedIndicesUserWithSomeRolesEmptyMetadata() {
        Role role = Role.builder(RESTRICTED_INDICES, "role").add(IndexPrivilege.ALL, "*").build();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(SearchAction.NAME),
            Metadata.EMPTY_METADATA.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all().get().isEmpty());
    }

    public void testSecurityIndicesAreRemovedFromRegularUser() {
        Role role = Role.builder(RESTRICTED_INDICES, "user_role").add(IndexPrivilege.ALL, "*").cluster(Set.of("all"), Set.of()).build();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(SearchAction.NAME),
            Metadata.EMPTY_METADATA.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all().get().isEmpty());
    }

    public void testSecurityIndicesAreRestrictedForDefaultRole() {
        Role role = Role.builder(RESTRICTED_INDICES, randomFrom("user_role", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))
            .add(IndexPrivilege.ALL, "*")
            .cluster(Set.of("all"), Set.of())
            .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .build();

        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(SearchAction.NAME),
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get(), containsInAnyOrder("an-index", "another-index"));
        assertThat(authorizedIndices.check("an-index"), is(true));
        assertThat(authorizedIndices.check("another-index"), is(true));
        assertThat(authorizedIndices.all().get(), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(false));
    }

    public void testSecurityIndicesAreNotRemovedFromUnrestrictedRole() {
        Role role = Role.builder(RESTRICTED_INDICES, randomAlphaOfLength(8))
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.ALL, true, "*")
            .cluster(Set.of("all"), Set.of())
            .build();
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("an-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("another-index").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .build();

        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(SearchAction.NAME),
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(
            authorizedIndices.all().get(),
            containsInAnyOrder("an-index", "another-index", SecuritySystemIndices.SECURITY_MAIN_ALIAS, internalSecurityIndex)
        );

        AuthorizedIndices authorizedIndicesSuperUser = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(SearchAction.NAME),
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(
            authorizedIndicesSuperUser.all().get(),
            containsInAnyOrder("an-index", "another-index", SecuritySystemIndices.SECURITY_MAIN_ALIAS, internalSecurityIndex)
        );
    }

    public void testDataStreamsAreNotIncludedInAuthorizedIndices() {
        RoleDescriptor aStarRole = new RoleDescriptor(
            "a_star",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() },
            null
        );
        RoleDescriptor bRole = new RoleDescriptor(
            "b",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() },
            null
        );
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder("b").settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder("ab").build())
                    .putAlias(new AliasMetadata.Builder("ba").build())
                    .build(),
                true
            )
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(DataStream.getDefaultBackingIndexName("adatastream1", 1), "_na_"))
                )
            )
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(
            descriptors,
            new FieldPermissionsCache(Settings.EMPTY),
            null,
            RESTRICTED_INDICES,
            future
        );
        Role roles = future.actionGet();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            getRequestInfo(SearchAction.NAME),
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get(), containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        for (String resource : List.of("a1", "a2", "aaaaaa", "b", "ab")) {
            assertThat(authorizedIndices.check(resource), is(true));
        }
        assertThat(authorizedIndices.all().get(), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb"), is(false));
        assertThat(authorizedIndices.all().get(), not(contains("ba")));
        assertThat(authorizedIndices.check("ba"), is(false));
        // due to context, datastreams are excluded from wildcard expansion
        assertThat(authorizedIndices.all().get(), not(contains("adatastream1")));
        // but they are authorized when explicitly tested (they are not "unavailable" for the Security filter)
        assertThat(authorizedIndices.check("adatastream1"), is(true));
        assertThat(authorizedIndices.all().get(), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(false));
    }

    public void testDataStreamsAreIncludedInAuthorizedIndices() {
        RoleDescriptor aStarRole = new RoleDescriptor(
            "a_star",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("all").build() },
            null
        );
        RoleDescriptor bRole = new RoleDescriptor(
            "b",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("b").privileges("READ").build() },
            null
        );
        Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder("b").settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder("ab").build())
                    .putAlias(new AliasMetadata.Builder("ba").build())
                    .build(),
                true
            )
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(DataStream.getDefaultBackingIndexName("adatastream1", 1), "_na_"))
                )
            )
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aStarRole, bRole);
        CompositeRolesStore.buildRoleFromDescriptors(
            descriptors,
            new FieldPermissionsCache(Settings.EMPTY),
            null,
            RESTRICTED_INDICES,
            future
        );
        Role roles = future.actionGet();
        TransportRequest request = new ResolveIndexAction.Request(new String[] { "a*" });
        AuthorizationEngine.RequestInfo requestInfo = getRequestInfo(request, SearchAction.NAME);
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            requestInfo,
            metadata.getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all().get(), containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab", "adatastream1", backingIndex));
        assertThat(authorizedIndices.all().get(), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb"), is(false));
        assertThat(authorizedIndices.all().get(), not(contains("ba")));
        assertThat(authorizedIndices.check("ba"), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex), is(false));
        assertThat(authorizedIndices.all().get(), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(false));
    }

    public static AuthorizationEngine.RequestInfo getRequestInfo(String action) {
        return getRequestInfo(TransportRequest.Empty.INSTANCE, action);
    }

    public static AuthorizationEngine.RequestInfo getRequestInfo(TransportRequest request, String action) {
        final Authentication.RealmRef realm = new Authentication.RealmRef(
            randomAlphaOfLength(6),
            randomAlphaOfLength(4),
            "node0" + randomIntBetween(1, 9)
        );
        return new AuthorizationEngine.RequestInfo(
            AuthenticationTestHelper.builder().user(new User(randomAlphaOfLength(8))).realmRef(realm).build(false),
            request,
            action,
            null
        );
    }
}
