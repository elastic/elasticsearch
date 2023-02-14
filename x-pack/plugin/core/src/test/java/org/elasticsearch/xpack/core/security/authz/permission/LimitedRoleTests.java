/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LimitedRoleTests extends ESTestCase {

    private static final RestrictedIndices EMPTY_RESTRICTED_INDICES = new RestrictedIndices(Automatons.EMPTY);

    List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors;

    @Before
    public void setup() {
        applicationPrivilegeDescriptors = new ArrayList<>();
    }

    public void testRoleConstructorWithLimitedRole() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role").build();
        Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").build();
        Role role = fromRole.limitedBy(limitedByRole);
        assertNotNull(role);
        assertThat(role.names(), is(limitedByRole.names()));

        NullPointerException npe = expectThrows(NullPointerException.class, () -> fromRole.limitedBy(null));
        assertThat(npe.getMessage(), containsString("limited by role is required to create limited role"));
    }

    public void testAuthorize() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            )
            .putAlias(AliasMetadata.builder("_alias"));
        IndexMetadata.Builder imbBuilder1 = IndexMetadata.builder("_index1")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            )
            .putAlias(AliasMetadata.builder("_alias1"));
        Metadata md = Metadata.builder().put(imbBuilder).put(imbBuilder1).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role")
            .cluster(Collections.singleton("manage_security"), Collections.emptyList())
            .add(IndexPrivilege.ALL, "_index")
            .add(IndexPrivilege.CREATE_INDEX, "_index1")
            .build();

        IndicesAccessControl iac = fromRole.authorize(
            SearchAction.NAME,
            Sets.newHashSet("_index", "_alias1"),
            md.getIndicesLookup(),
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(false));
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("_index"), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
        assertThat(iac.hasIndexPermissions("_index1"), is(false));
        iac = fromRole.authorize(
            CreateIndexAction.NAME,
            Sets.newHashSet("_index", "_index1"),
            md.getIndicesLookup(),
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("_index"), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("_index1"), is(true));

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role")
                .cluster(Collections.singleton("all"), Collections.emptyList())
                .add(IndexPrivilege.READ, "_index")
                .add(IndexPrivilege.NONE, "_index1")
                .build();
            iac = limitedByRole.authorize(
                SearchAction.NAME,
                Sets.newHashSet("_index", "_alias1"),
                md.getIndicesLookup(),
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));
            iac = limitedByRole.authorize(
                DeleteIndexAction.NAME,
                Sets.newHashSet("_index", "_alias1"),
                md.getIndicesLookup(),
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));
            iac = limitedByRole.authorize(
                CreateIndexAction.NAME,
                Sets.newHashSet("_index", "_alias1"),
                md.getIndicesLookup(),
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));

            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            iac = role.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(), fieldPermissionsCache);
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));
            iac = role.authorize(
                DeleteIndexAction.NAME,
                Sets.newHashSet("_index", "_alias1"),
                md.getIndicesLookup(),
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));
            iac = role.authorize(
                CreateIndexAction.NAME,
                Sets.newHashSet("_index", "_index1"),
                md.getIndicesLookup(),
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index"), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(nullValue()));
            assertThat(iac.hasIndexPermissions("_index1"), is(false));
        }
    }

    public void testCheckClusterAction() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role")
            .cluster(Collections.singleton("manage_security"), Collections.emptyList())
            .build();
        Authentication authentication = AuthenticationTestHelper.builder().build();
        assertThat(fromRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(true));
        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role")
                .cluster(Collections.singleton("all"), Collections.emptyList())
                .build();
            assertThat(
                limitedByRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication),
                is(true)
            );
            assertThat(limitedByRole.checkClusterAction("cluster:other-action", mock(TransportRequest.class), authentication), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(true));
            assertThat(role.checkClusterAction("cluster:other-action", mock(TransportRequest.class), authentication), is(false));
        }
        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role")
                .cluster(Collections.singleton("monitor"), Collections.emptyList())
                .build();
            assertThat(limitedByRole.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class), authentication), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class), authentication), is(false));
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(false));
        }
    }

    public void testCheckIndicesAction() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role").add(IndexPrivilege.READ, "ind-1").build();
        assertThat(fromRole.checkIndicesAction(SearchAction.NAME), is(true));
        assertThat(fromRole.checkIndicesAction(CreateIndexAction.NAME), is(false));

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.ALL, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(limitedByRole.checkIndicesAction(CreateIndexAction.NAME), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.NONE, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(false));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(false));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
    }

    public void testAllowedIndicesMatcher() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role").add(IndexPrivilege.READ, "ind-1*").build();
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(false));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(false));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));
        }
        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.READ, "ind-*").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));
        }
    }

    public void testAllowedIndicesMatcherWithNestedRole() {
        Role role = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role").add(IndexPrivilege.READ, "ind-1*").build();
        assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
        assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(true));
        assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));

        final int depth = randomIntBetween(2, 4);
        boolean index11Excluded = false;
        for (int i = 0; i < depth; i++) {
            final boolean excludeIndex11 = randomBoolean();
            final String[] indexNames = excludeIndex11 ? new String[] { "ind-1", "ind-2" } : new String[] { "ind-*" };
            index11Excluded = index11Excluded || excludeIndex11;
            final Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role-" + i)
                .add(IndexPrivilege.READ, indexNames)
                .build();
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(role);
            } else {
                role = role.limitedBy(limitedByRole);
            }
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(false == index11Excluded));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));
        }
    }

    public void testAllowedActionsMatcher() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "fromRole")
            .add(IndexPrivilege.WRITE, "ind*")
            .add(IndexPrivilege.READ, "ind*")
            .add(IndexPrivilege.READ, "other*")
            .build();
        Automaton fromRoleAutomaton = fromRole.allowedActionsMatcher("index1");
        Predicate<String> fromRolePredicate = Automatons.predicate(fromRoleAutomaton);
        assertThat(fromRolePredicate.test(SearchAction.NAME), is(true));
        assertThat(fromRolePredicate.test(BulkAction.NAME), is(true));

        Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limitedRole").add(IndexPrivilege.READ, "index1", "index2").build();
        Automaton limitedByRoleAutomaton = limitedByRole.allowedActionsMatcher("index1");
        Predicate<String> limitedByRolePredicated = Automatons.predicate(limitedByRoleAutomaton);
        assertThat(limitedByRolePredicated.test(SearchAction.NAME), is(true));
        assertThat(limitedByRolePredicated.test(BulkAction.NAME), is(false));
        Role role;
        if (randomBoolean()) {
            role = limitedByRole.limitedBy(fromRole);
        } else {
            role = fromRole.limitedBy(limitedByRole);
        }

        Automaton roleAutomaton = role.allowedActionsMatcher("index1");
        Predicate<String> rolePredicate = Automatons.predicate(roleAutomaton);
        assertThat(rolePredicate.test(SearchAction.NAME), is(true));
        assertThat(rolePredicate.test(BulkAction.NAME), is(false));

        roleAutomaton = role.allowedActionsMatcher("index2");
        rolePredicate = Automatons.predicate(roleAutomaton);
        assertThat(rolePredicate.test(SearchAction.NAME), is(true));
        assertThat(rolePredicate.test(BulkAction.NAME), is(false));

        roleAutomaton = role.allowedActionsMatcher("other");
        rolePredicate = Automatons.predicate(roleAutomaton);
        assertThat(rolePredicate.test(SearchAction.NAME), is(false));
        assertThat(rolePredicate.test(BulkAction.NAME), is(false));
    }

    public void testCheckClusterPrivilege() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role")
            .cluster(Collections.singleton("manage_security"), Collections.emptyList())
            .build();
        assertThat(fromRole.grants(ClusterPrivilegeResolver.ALL), is(false));
        assertThat(fromRole.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "scoped-role")
                .cluster(Collections.singleton("all"), Collections.emptyList())
                .build();
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.ALL), is(true));
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));
        }
        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "scoped-role")
                .cluster(Collections.singleton("monitor"), Collections.emptyList())
                .build();
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.MONITOR), is(true));
            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }
            assertThat(role.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MONITOR), is(false));
        }
    }

    public void testHasPrivilegesForIndexPatterns() {
        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role").add(IndexPrivilege.READ, "ind-1*").build();

        verifyResourcesPrivileges(
            fromRole,
            Set.of("ind-1-1-*"),
            true,
            Set.of("read", "write"),
            false,
            new ResourcePrivilegesMap(
                Map.of("ind-1-1-*", ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", true).addPrivilege("write", false).build())
            )
        );

        verifyResourcesPrivileges(
            fromRole,
            Set.of("ind-*"),
            true,
            Set.of("read", "write"),
            false,
            new ResourcePrivilegesMap(
                Map.of("ind-*", ResourcePrivileges.builder("ind-*").addPrivilege("read", false).addPrivilege("write", false).build())
            )
        );

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();

            verifyResourcesPrivileges(
                limitedByRole,
                Set.of("ind-1"),
                true,
                Set.of("read"),
                true,
                new ResourcePrivilegesMap(Map.of("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build()))
            );

            verifyResourcesPrivileges(
                limitedByRole,
                Set.of("ind-1-1-*"),
                true,
                Set.of("read"),
                false,
                new ResourcePrivilegesMap(Map.of("ind-1-1-*", ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", false).build()))
            );

            verifyResourcesPrivileges(
                limitedByRole,
                Set.of("ind-*"),
                true,
                Set.of("read"),
                false,
                new ResourcePrivilegesMap(Map.of("ind-*", ResourcePrivileges.builder("ind-*").addPrivilege("read", false).build()))
            );

            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }

            verifyResourcesPrivileges(
                role,
                Set.of("ind-1"),
                true,
                Set.of("read"),
                true,
                new ResourcePrivilegesMap(Map.of("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build()))
            );

            verifyResourcesPrivileges(
                role,
                Set.of("ind-1-1-*", "ind-1"),
                true,
                Set.of("read"),
                false,
                new ResourcePrivilegesMap(
                    mapBuilder().put("ind-1-1-*", ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", false).build())
                        .put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                        .map()
                )
            );
        }
        {
            fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "a-role")
                .add(FieldPermissions.DEFAULT, Collections.emptySet(), IndexPrivilege.READ, true, "ind-1*", ".security")
                .build();

            verifyResourcesPrivileges(
                fromRole,
                Set.of("ind-1", ".security"),
                true,
                Set.of("read"),
                true,
                new ResourcePrivilegesMap(
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                        .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", true).build())
                        .map()
                )
            );

            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();

            verifyResourcesPrivileges(
                limitedByRole,
                Set.of("ind-1", "ind-2", ".security"),
                true,
                Set.of("read"),
                false,
                new ResourcePrivilegesMap(
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                        .put("ind-2", ResourcePrivileges.builder("ind-2").addPrivilege("read", true).build())
                        .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", false).build())
                        .map()
                )
            );

            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }

            verifyResourcesPrivileges(
                role,
                Set.of("ind-1", "ind-2", ".security"),
                true,
                Set.of("read"),
                false,
                new ResourcePrivilegesMap(
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                        .put("ind-2", ResourcePrivileges.builder("ind-2").addPrivilege("read", false).build())
                        .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", false).build())
                        .map()
                )
            );
        }
    }

    public void testGetApplicationPrivilegesByResource() {
        final ApplicationPrivilege app1Read = defineApplicationPrivilege("app1", "read", "data:read/*");
        final ApplicationPrivilege app1All = defineApplicationPrivilege("app1", "all", "*");
        final ApplicationPrivilege app2Read = defineApplicationPrivilege("app2", "read", "data:read/*");
        final ApplicationPrivilege app2Write = defineApplicationPrivilege("app2", "write", "data:write/*");

        Role fromRole = Role.builder(EMPTY_RESTRICTED_INDICES, "test-role")
            .addApplicationPrivilege(app1Read, Collections.singleton("foo/*"))
            .addApplicationPrivilege(app1All, Collections.singleton("foo/bar/baz"))
            .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
            .addApplicationPrivilege(app2Write, Collections.singleton("*/bar/*"))
            .build();

        Set<String> forPrivilegeNames = Sets.newHashSet("read", "write", "all");

        verifyResourcesPrivileges(
            fromRole,
            "app1",
            Set.of("*"),
            forPrivilegeNames,
            applicationPrivilegeDescriptors,
            false,
            new ResourcePrivilegesMap(
                Map.of(
                    "*",
                    ResourcePrivileges.builder("*")
                        .addPrivilege("read", false)
                        .addPrivilege("write", false)
                        .addPrivilege("all", false)
                        .build()
                )
            )
        );

        verifyResourcesPrivileges(
            fromRole,
            "app1",
            Set.of("foo/x/y"),
            forPrivilegeNames,
            applicationPrivilegeDescriptors,
            false,
            new ResourcePrivilegesMap(
                Map.of(
                    "foo/x/y",
                    ResourcePrivileges.builder("foo/x/y")
                        .addPrivilege("read", true)
                        .addPrivilege("write", false)
                        .addPrivilege("all", false)
                        .build()
                )
            )
        );

        verifyResourcesPrivileges(
            fromRole,
            "app2",
            Set.of("foo/bar/a"),
            forPrivilegeNames,
            applicationPrivilegeDescriptors,
            false,
            new ResourcePrivilegesMap(
                Map.of(
                    "foo/bar/a",
                    ResourcePrivileges.builder("foo/bar/a")
                        .addPrivilege("read", true)
                        .addPrivilege("write", true)
                        .addPrivilege("all", false)
                        .build()
                )
            )
        );

        verifyResourcesPrivileges(
            fromRole,
            "app2",
            Set.of("moon/bar/a"),
            forPrivilegeNames,
            applicationPrivilegeDescriptors,
            false,
            new ResourcePrivilegesMap(
                Map.of(
                    "moon/bar/a",
                    ResourcePrivileges.builder("moon/bar/a")
                        .addPrivilege("read", false)
                        .addPrivilege("write", true)
                        .addPrivilege("all", false)
                        .build()
                )
            )
        );

        {
            Role limitedByRole = Role.builder(EMPTY_RESTRICTED_INDICES, "test-role-scoped")
                .addApplicationPrivilege(app1Read, Collections.singleton("foo/scoped/*"))
                .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
                .addApplicationPrivilege(app2Write, Collections.singleton("moo/bar/*"))
                .build();

            verifyResourcesPrivileges(
                limitedByRole,
                "app1",
                Set.of("*"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "*",
                        ResourcePrivileges.builder("*")
                            .addPrivilege("read", false)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                limitedByRole,
                "app1",
                Set.of("foo/x/y"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "foo/x/y",
                        ResourcePrivileges.builder("foo/x/y")
                            .addPrivilege("read", false)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                limitedByRole,
                "app2",
                Set.of("foo/bar/a"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "foo/bar/a",
                        ResourcePrivileges.builder("foo/bar/a")
                            .addPrivilege("read", true)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                limitedByRole,
                "app2",
                Set.of("moon/bar/a"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "moon/bar/a",
                        ResourcePrivileges.builder("moon/bar/a")
                            .addPrivilege("read", false)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            Role role;
            if (randomBoolean()) {
                role = limitedByRole.limitedBy(fromRole);
            } else {
                role = fromRole.limitedBy(limitedByRole);
            }

            verifyResourcesPrivileges(
                role,
                "app2",
                Set.of("foo/bar/a"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "foo/bar/a",
                        ResourcePrivileges.builder("foo/bar/a")
                            .addPrivilege("read", true)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                role,
                "app2",
                Set.of("moon/bar/a"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "moon/bar/a",
                        ResourcePrivileges.builder("moon/bar/a")
                            .addPrivilege("read", false)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                role,
                "unknown",
                Set.of("moon/bar/a"),
                forPrivilegeNames,
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "moon/bar/a",
                        ResourcePrivileges.builder("moon/bar/a")
                            .addPrivilege("read", false)
                            .addPrivilege("write", false)
                            .addPrivilege("all", false)
                            .build()
                    )
                )
            );

            verifyResourcesPrivileges(
                role,
                "app2",
                Set.of("moo/bar/a"),
                Set.of("read", "write", "all", "unknown"),
                applicationPrivilegeDescriptors,
                false,
                new ResourcePrivilegesMap(
                    Map.of(
                        "moo/bar/a",
                        ResourcePrivileges.builder("moo/bar/a")
                            .addPrivilege("read", false)
                            .addPrivilege("write", true)
                            .addPrivilege("all", false)
                            .addPrivilege("unknown", false)
                            .build()
                    )
                )
            );
        }
    }

    private void verifyResourcesPrivileges(
        Role role,
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        boolean expectedCheckResult,
        ResourcePrivilegesMap expectedAppPrivsByResource
    ) {
        // call "check indices privileges" twice, with and without details, in random order
        ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder = randomBoolean() ? ResourcePrivilegesMap.builder() : null;
        boolean privilegesCheck = role.checkIndicesPrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            resourcePrivilegesMapBuilder
        );
        assertThat(privilegesCheck, is(expectedCheckResult));

        if (resourcePrivilegesMapBuilder == null) {
            resourcePrivilegesMapBuilder = ResourcePrivilegesMap.builder();
            privilegesCheck = role.checkIndicesPrivileges(
                checkForIndexPatterns,
                allowRestrictedIndices,
                checkForPrivileges,
                resourcePrivilegesMapBuilder
            );
            assertThat(privilegesCheck, is(expectedCheckResult));
        } else {
            privilegesCheck = role.checkIndicesPrivileges(checkForIndexPatterns, allowRestrictedIndices, checkForPrivileges, null);
            assertThat(privilegesCheck, is(expectedCheckResult));
        }
        assertThat(resourcePrivilegesMapBuilder.build(), equalTo(expectedAppPrivsByResource));
    }

    private void verifyResourcesPrivileges(
        Role role,
        String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        boolean expectedCheckResult,
        ResourcePrivilegesMap expectedAppPrivsByResource
    ) {
        ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder = randomBoolean() ? ResourcePrivilegesMap.builder() : null;
        boolean privilegesCheck = role.checkApplicationResourcePrivileges(
            applicationName,
            checkForResources,
            checkForPrivilegeNames,
            storedPrivileges,
            resourcePrivilegesMapBuilder
        );
        assertThat(privilegesCheck, is(expectedCheckResult));

        if (resourcePrivilegesMapBuilder == null) {
            resourcePrivilegesMapBuilder = ResourcePrivilegesMap.builder();
            privilegesCheck = role.checkApplicationResourcePrivileges(
                applicationName,
                checkForResources,
                checkForPrivilegeNames,
                storedPrivileges,
                resourcePrivilegesMapBuilder
            );
            assertThat(privilegesCheck, is(expectedCheckResult));
        } else {
            privilegesCheck = role.checkApplicationResourcePrivileges(
                applicationName,
                checkForResources,
                checkForPrivilegeNames,
                storedPrivileges,
                null
            );
            assertThat(privilegesCheck, is(expectedCheckResult));
        }
        assertThat(resourcePrivilegesMapBuilder.build(), equalTo(expectedAppPrivsByResource));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(
            randomFrom(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        return mock;
    }

    private ApplicationPrivilege defineApplicationPrivilege(String app, String name, String... actions) {
        applicationPrivilegeDescriptors.add(
            new ApplicationPrivilegeDescriptor(app, name, Sets.newHashSet(actions), Collections.emptyMap())
        );
        return ApplicationPrivilegeTests.createPrivilege(app, name, actions);
    }

    private static MapBuilder<String, ResourcePrivileges> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }

}
