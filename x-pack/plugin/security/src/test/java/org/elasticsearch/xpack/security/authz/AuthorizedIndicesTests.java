/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.EmptyRequest;
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
            emptyProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all(IndexComponentSelector.DATA).isEmpty());
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
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("ba")));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
    }

    public void testAuthorizedIndicesUserWithSomeRolesEmptyMetadata() {
        Role role = Role.builder(RESTRICTED_INDICES, "role").add(IndexPrivilege.ALL, "*").build();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(TransportSearchAction.TYPE.name()),
            emptyProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all(IndexComponentSelector.DATA).isEmpty());
    }

    public void testSecurityIndicesAreRemovedFromRegularUser() {
        Role role = Role.builder(RESTRICTED_INDICES, "user_role").add(IndexPrivilege.ALL, "*").cluster(Set.of("all"), Set.of()).build();
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(TransportSearchAction.TYPE.name()),
            emptyProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertTrue(authorizedIndices.all(IndexComponentSelector.DATA).isEmpty());
    }

    public void testSecurityIndicesAreRestrictedForDefaultRole() {
        Role role = Role.builder(RESTRICTED_INDICES, randomFrom("user_role", ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))
            .add(IndexPrivilege.ALL, "*")
            .cluster(Set.of("all"), Set.of())
            .build();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), containsInAnyOrder("an-index", "another-index"));
        assertThat(authorizedIndices.check("an-index", IndexComponentSelector.DATA), is(true));
        assertThat(authorizedIndices.check("another-index", IndexComponentSelector.DATA), is(true));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
    }

    public void testSecurityIndicesAreNotRemovedFromUnrestrictedRole() {
        Role role = Role.builder(RESTRICTED_INDICES, randomAlphaOfLength(8))
            .add(FieldPermissions.DEFAULT, null, IndexPrivilege.ALL, true, "*")
            .cluster(Set.of("all"), Set.of())
            .build();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(
            authorizedIndices.all(IndexComponentSelector.DATA),
            containsInAnyOrder("an-index", "another-index", SecuritySystemIndices.SECURITY_MAIN_ALIAS, internalSecurityIndex)
        );

        AuthorizedIndices authorizedIndicesSuperUser = RBACEngine.resolveAuthorizedIndicesFromRole(
            role,
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(
            authorizedIndicesSuperUser.all(IndexComponentSelector.DATA),
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
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            .put(DataStreamTestHelper.newInstance("adatastream1", List.of(new Index(backingIndex, "_na_"))))
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab"));
        for (String resource : List.of("a1", "a2", "aaaaaa", "b", "ab")) {
            assertThat(authorizedIndices.check(resource, IndexComponentSelector.DATA), is(true));
        }
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("ba")));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        // due to context, datastreams are excluded from wildcard expansion
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("adatastream1")));
        // but they are authorized when explicitly tested (they are not "unavailable" for the Security filter)
        assertThat(authorizedIndices.check("adatastream1", IndexComponentSelector.DATA), is(true));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
    }

    public void testDataStreamsAreNotIncludedInAuthorizedIndicesWithFailuresSelectorAndAllPrivilege() {
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
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        String failureIndex = DataStream.getDefaultFailureStoreName("adatastream1", 1, 1);
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
            .put(new IndexMetadata.Builder(failureIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(backingIndex, "_na_")),
                    List.of(new Index(failureIndex, "_na_"))
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.DATA, "a1", "a2", "aaaaaa", "b", "ab");
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.FAILURES);

        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.FAILURES), is(false));

        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.FAILURES), is(false));

        // data are authorized when explicitly tested (they are not "unavailable" for the Security filter)
        assertThat(authorizedIndices.check("adatastream1", IndexComponentSelector.DATA), is(true));
        assertThat(authorizedIndices.check("adatastream1", IndexComponentSelector.FAILURES), is(true));

        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.FAILURES), is(false));
    }

    public void testDataStreamsAreIncludedInAuthorizedIndicesWithFailuresSelectorAndAllPrivilege() {
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
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        String failureIndex = DataStream.getDefaultFailureStoreName("adatastream1", 1, 1);
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
            .put(new IndexMetadata.Builder(failureIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(backingIndex, "_na_")),
                    List.of(new Index(failureIndex, "_na_"))
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
        AuthorizationEngine.RequestInfo requestInfo = getRequestInfo(request, TransportSearchAction.TYPE.name());
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            requestInfo,
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertAuthorizedFor(
            authorizedIndices,
            IndexComponentSelector.DATA,
            "a1",
            "a2",
            "aaaaaa",
            "b",
            "ab",
            "adatastream1",
            backingIndex,
            failureIndex
        );
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.FAILURES, "adatastream1", failureIndex);
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.FAILURES), is(false));
    }

    public void testDataStreamsAreIncludedInAuthorizedIndicesWithFailuresSelector() {
        RoleDescriptor aReadFailuresStarRole = new RoleDescriptor(
            "a_read_failure_store",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("read_failure_store").build() },
            null
        );
        RoleDescriptor aReadRole = new RoleDescriptor(
            "a_read",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("read").build() },
            null
        );
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        String failureIndex = DataStream.getDefaultFailureStoreName("adatastream1", 1, 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder(failureIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(backingIndex, "_na_")),
                    List.of(new Index(failureIndex, "_na_"))
                )
            )
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aReadFailuresStarRole, aReadRole);
        CompositeRolesStore.buildRoleFromDescriptors(
            descriptors,
            new FieldPermissionsCache(Settings.EMPTY),
            null,
            RESTRICTED_INDICES,
            future
        );
        Role roles = future.actionGet();
        TransportRequest request = new ResolveIndexAction.Request(new String[] { "a*" });
        AuthorizationEngine.RequestInfo requestInfo = getRequestInfo(request, TransportSearchAction.TYPE.name());
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            requestInfo,
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertAuthorizedFor(
            authorizedIndices,
            IndexComponentSelector.DATA,
            "a1",
            "a2",
            "aaaaaa",
            "adatastream1",
            backingIndex,
            failureIndex
        );
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.FAILURES, "adatastream1", failureIndex);
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.FAILURES), is(false));
    }

    public void testDataStreamsAreNotIncludedInAuthorizedIndicesWithFailuresSelector() {
        RoleDescriptor aReadFailuresStarRole = new RoleDescriptor(
            "a_read_failure_store",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("read_failure_store").build() },
            null
        );
        RoleDescriptor aReadRole = new RoleDescriptor(
            "a_read",
            null,
            new IndicesPrivileges[] { IndicesPrivileges.builder().indices("a*").privileges("read").build() },
            null
        );
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        String backingIndex = DataStream.getDefaultBackingIndexName("adatastream1", 1);
        String failureIndex = DataStream.getDefaultFailureStoreName("adatastream1", 1, 1);
        Metadata metadata = Metadata.builder()
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .put(new IndexMetadata.Builder(backingIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder(failureIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                DataStreamTestHelper.newInstance(
                    "adatastream1",
                    List.of(new Index(backingIndex, "_na_")),
                    List.of(new Index(failureIndex, "_na_"))
                )
            )
            .build();
        final PlainActionFuture<Role> future = new PlainActionFuture<>();
        final Set<RoleDescriptor> descriptors = Sets.newHashSet(aReadFailuresStarRole, aReadRole);
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
            getRequestInfo(TransportSearchAction.TYPE.name()),
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.DATA, "a1", "a2", "aaaaaa");
        assertAuthorizedFor(authorizedIndices, IndexComponentSelector.FAILURES);

        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.FAILURES), is(false));

        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.FAILURES), is(false));

        // data are authorized when explicitly tested (they are not "unavailable" for the Security filter)
        assertThat(authorizedIndices.check("adatastream1", IndexComponentSelector.DATA), is(true));
        assertThat(authorizedIndices.check("adatastream1", IndexComponentSelector.FAILURES), is(true));

        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.FAILURES), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.FAILURES), is(false));
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
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            .put(DataStreamTestHelper.newInstance("adatastream1", List.of(new Index(backingIndex, "_na_"))))
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
        AuthorizationEngine.RequestInfo requestInfo = getRequestInfo(request, TransportSearchAction.TYPE.name());
        AuthorizedIndices authorizedIndices = RBACEngine.resolveAuthorizedIndicesFromRole(
            roles,
            requestInfo,
            metadata.getProject().getIndicesLookup(),
            () -> ignore -> {}
        );
        assertThat(
            authorizedIndices.all(IndexComponentSelector.DATA),
            containsInAnyOrder("a1", "a2", "aaaaaa", "b", "ab", "adatastream1", backingIndex)
        );
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("bbbbb")));
        assertThat(authorizedIndices.check("bbbbb", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains("ba")));
        assertThat(authorizedIndices.check("ba", IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(internalSecurityIndex)));
        assertThat(authorizedIndices.check(internalSecurityIndex, IndexComponentSelector.DATA), is(false));
        assertThat(authorizedIndices.all(IndexComponentSelector.DATA), not(contains(SecuritySystemIndices.SECURITY_MAIN_ALIAS)));
        assertThat(authorizedIndices.check(SecuritySystemIndices.SECURITY_MAIN_ALIAS, IndexComponentSelector.DATA), is(false));
    }

    public static AuthorizationEngine.RequestInfo getRequestInfo(String action) {
        return getRequestInfo(new EmptyRequest(), action);
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

    private static void assertAuthorizedFor(
        AuthorizedIndices authorizedIndices,
        IndexComponentSelector selector,
        String... expectedIndices
    ) {
        assertThat(authorizedIndices.all(selector), containsInAnyOrder(expectedIndices));
        for (String resource : expectedIndices) {
            assertThat(authorizedIndices.check(resource, selector), is(true));
        }
    }
}
