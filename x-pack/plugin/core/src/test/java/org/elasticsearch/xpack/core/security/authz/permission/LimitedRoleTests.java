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
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LimitedRoleTests extends ESTestCase {
    List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors;

    @Before
    public void setup() {
        applicationPrivilegeDescriptors = new ArrayList<>();
    }

    public void testRoleConstructorWithLimitedRole() {
        Role fromRole = Role.builder("a-role").build();
        Role limitedByRole = Role.builder("limited-role").build();
        Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
        assertNotNull(role);
        assertThat(role.names(), is(limitedByRole.names()));

        NullPointerException npe = expectThrows(NullPointerException.class, () -> LimitedRole.createLimitedRole(fromRole, null));
        assertThat(npe.getMessage(), containsString("limited by role is required to create limited role"));
    }

    public void testAuthorize() {
        IndexMetadata.Builder imbBuilder = IndexMetadata
                .builder("_index").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .putAlias(AliasMetadata.builder("_alias"));
        IndexMetadata.Builder imbBuilder1 = IndexMetadata
                .builder("_index1").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .putAlias(AliasMetadata.builder("_alias1"));
        Metadata md = Metadata.builder().put(imbBuilder).put(imbBuilder1).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton("manage_security"), Collections.emptyList())
                .add(IndexPrivilege.ALL, "_index").add(IndexPrivilege.CREATE_INDEX, "_index1").build();

        IndicesAccessControl iac = fromRole.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
            fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
        iac = fromRole.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_index1"), md.getIndicesLookup(),
            fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index1").isGranted(), is(true));

        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton("all"), Collections.emptyList()).add(IndexPrivilege.READ, "_index")
                    .add(IndexPrivilege.NONE, "_index1").build();
            iac = limitedByRole.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = limitedByRole.authorize(DeleteIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = limitedByRole.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            iac = role.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = role.authorize(DeleteIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = role.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_index1"), md.getIndicesLookup(),
                fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
        }
    }

    public void testCheckClusterAction() {
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton("manage_security"), Collections.emptyList())
            .build();
        Authentication authentication = mock(Authentication.class);
        assertThat(fromRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(true));
        {
            Role limitedByRole = Role.builder("limited-role")
                .cluster(Collections.singleton("all"), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication),
                is(true));
            assertThat(limitedByRole.checkClusterAction("cluster:other-action", mock(TransportRequest.class), authentication), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(true));
            assertThat(role.checkClusterAction("cluster:other-action", mock(TransportRequest.class), authentication), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role")
                .cluster(Collections.singleton("monitor"), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class), authentication), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class), authentication), is(false));
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class), authentication), is(false));
        }
    }

    public void testCheckIndicesAction() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1").build();
        assertThat(fromRole.checkIndicesAction(SearchAction.NAME), is(true));
        assertThat(fromRole.checkIndicesAction(CreateIndexAction.NAME), is(false));

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.ALL, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(limitedByRole.checkIndicesAction(CreateIndexAction.NAME), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(true));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.NONE, "ind-1").build();
            assertThat(limitedByRole.checkIndicesAction(SearchAction.NAME), is(false));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkIndicesAction(SearchAction.NAME), is(false));
            assertThat(role.checkIndicesAction(CreateIndexAction.NAME), is(false));
        }
    }

    public void testAllowedIndicesMatcher() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1*").build();
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(false));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-11")), is(false));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-*").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-1")), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("ind-2")), is(false));
        }
    }

    public void testAllowedActionsMatcher() {
        Role fromRole = Role.builder("fromRole")
                .add(IndexPrivilege.WRITE, "ind*")
                .add(IndexPrivilege.READ, "ind*")
                .add(IndexPrivilege.READ, "other*")
                .build();
        Automaton fromRoleAutomaton = fromRole.allowedActionsMatcher("index1");
        Predicate<String> fromRolePredicate = Automatons.predicate(fromRoleAutomaton);
        assertThat(fromRolePredicate.test(SearchAction.NAME), is(true));
        assertThat(fromRolePredicate.test(BulkAction.NAME), is(true));

        Role limitedByRole = Role.builder("limitedRole")
                .add(IndexPrivilege.READ, "index1", "index2")
                .build();
        Automaton limitedByRoleAutomaton = limitedByRole.allowedActionsMatcher("index1");
        Predicate<String> limitedByRolePredicated = Automatons.predicate(limitedByRoleAutomaton);
        assertThat(limitedByRolePredicated.test(SearchAction.NAME), is(true));
        assertThat(limitedByRolePredicated.test(BulkAction.NAME), is(false));
        Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);

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
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton("manage_security"), Collections.emptyList())
                .build();
        assertThat(fromRole.grants(ClusterPrivilegeResolver.ALL), is(false));
        assertThat(fromRole.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));

        {
            Role limitedByRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton("all"), Collections.emptyList()).build();
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.ALL), is(true));
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(true));
        }
        {
            Role limitedByRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton("monitor"), Collections.emptyList()).build();
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(limitedByRole.grants(ClusterPrivilegeResolver.MONITOR), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.grants(ClusterPrivilegeResolver.ALL), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MANAGE_SECURITY), is(false));
            assertThat(role.grants(ClusterPrivilegeResolver.MONITOR), is(false));
        }
    }

    public void testGetPrivilegesForIndexPatterns() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1*").build();
        ResourcePrivilegesMap resourcePrivileges = fromRole.checkIndicesPrivileges(Collections.singleton("ind-1-1-*"), true,
                Sets.newHashSet("read", "write"));
        ResourcePrivilegesMap expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("ind-1-1-*",
                ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", true).addPrivilege("write", false).build()));
        verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

        resourcePrivileges = fromRole.checkIndicesPrivileges(Collections.singleton("ind-*"), true, Sets.newHashSet("read", "write"));
        expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("ind-*",
                ResourcePrivileges.builder("ind-*").addPrivilege("read", false).addPrivilege("write", false).build()));
        verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            resourcePrivileges = limitedByRole.checkIndicesPrivileges(Collections.singleton("ind-1"), true, Collections.singleton("read"));
            expectedAppPrivsByResource = new ResourcePrivilegesMap(true,
                    Collections.singletonMap("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = limitedByRole.checkIndicesPrivileges(Collections.singleton("ind-1-1-*"), true,
                    Collections.singleton("read"));
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    Collections.singletonMap("ind-1-1-*", ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", false).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = limitedByRole.checkIndicesPrivileges(Collections.singleton("ind-*"), true, Collections.singleton("read"));
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    Collections.singletonMap("ind-*", ResourcePrivileges.builder("ind-*").addPrivilege("read", false).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            resourcePrivileges = role.checkIndicesPrivileges(Collections.singleton("ind-1"), true, Collections.singleton("read"));
            expectedAppPrivsByResource = new ResourcePrivilegesMap(true,
                    Collections.singletonMap("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = role.checkIndicesPrivileges(Sets.newHashSet("ind-1-1-*", "ind-1"), true, Collections.singleton("read"));
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    mapBuilder().put("ind-1-1-*", ResourcePrivileges.builder("ind-1-1-*").addPrivilege("read", false).build())
                            .put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build()).map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);
        }
        {
            fromRole = Role.builder("a-role")
                    .add(FieldPermissions.DEFAULT, Collections.emptySet(), IndexPrivilege.READ, true, "ind-1*", ".security").build();
            resourcePrivileges = fromRole.checkIndicesPrivileges(Sets.newHashSet("ind-1", ".security"), true,
                    Collections.singleton("read"));
            // Map<String, ResourcePrivileges> expectedResourceToResourcePrivs = new HashMap<>();
            ;
            expectedAppPrivsByResource = new ResourcePrivilegesMap(true,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                            .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", true).build()).map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            resourcePrivileges = limitedByRole.checkIndicesPrivileges(Sets.newHashSet("ind-1", "ind-2", ".security"), true,
                    Collections.singleton("read"));

            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                            .put("ind-2", ResourcePrivileges.builder("ind-2").addPrivilege("read", true).build())
                            .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", false).build()).map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            resourcePrivileges = role.checkIndicesPrivileges(Sets.newHashSet("ind-1", "ind-2", ".security"), true,
                    Collections.singleton("read"));

            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder("ind-1").addPrivilege("read", true).build())
                            .put("ind-2", ResourcePrivileges.builder("ind-2").addPrivilege("read", false).build())
                            .put(".security", ResourcePrivileges.builder(".security").addPrivilege("read", false).build()).map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);
        }
    }

    public void testGetApplicationPrivilegesByResource() {
        final ApplicationPrivilege app1Read = defineApplicationPrivilege("app1", "read", "data:read/*");
        final ApplicationPrivilege app1All = defineApplicationPrivilege("app1", "all", "*");
        final ApplicationPrivilege app2Read = defineApplicationPrivilege("app2", "read", "data:read/*");
        final ApplicationPrivilege app2Write = defineApplicationPrivilege("app2", "write", "data:write/*");

        Role fromRole = Role.builder("test-role").addApplicationPrivilege(app1Read, Collections.singleton("foo/*"))
                .addApplicationPrivilege(app1All, Collections.singleton("foo/bar/baz"))
                .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
                .addApplicationPrivilege(app2Write, Collections.singleton("*/bar/*")).build();

        Set<String> forPrivilegeNames = Sets.newHashSet("read", "write", "all");
        ResourcePrivilegesMap appPrivsByResource = fromRole.checkApplicationResourcePrivileges("app1", Collections.singleton("*"),
                forPrivilegeNames, applicationPrivilegeDescriptors);
        ResourcePrivilegesMap expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("*", ResourcePrivileges
                .builder("*").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.checkApplicationResourcePrivileges("app1", Collections.singleton("foo/x/y"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("foo/x/y", ResourcePrivileges
                .builder("foo/x/y").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.checkApplicationResourcePrivileges("app2", Collections.singleton("foo/bar/a"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges
                .builder("foo/bar/a").addPrivilege("read", true).addPrivilege("write", true).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.checkApplicationResourcePrivileges("app2", Collections.singleton("moon/bar/a"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("moon/bar/a", ResourcePrivileges
                .builder("moon/bar/a").addPrivilege("read", false).addPrivilege("write", true).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        {
            Role limitedByRole = Role.builder("test-role-scoped").addApplicationPrivilege(app1Read, Collections.singleton("foo/scoped/*"))
                    .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
                    .addApplicationPrivilege(app2Write, Collections.singleton("moo/bar/*")).build();
            appPrivsByResource = limitedByRole.checkApplicationResourcePrivileges("app1", Collections.singleton("*"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("*", ResourcePrivileges.builder("*")
                    .addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.checkApplicationResourcePrivileges("app1", Collections.singleton("foo/x/y"),
                    forPrivilegeNames, applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("foo/x/y", ResourcePrivileges
                    .builder("foo/x/y").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.checkApplicationResourcePrivileges("app2", Collections.singleton("foo/bar/a"),
                    forPrivilegeNames, applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges
                    .builder("foo/bar/a").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.checkApplicationResourcePrivileges("app2", Collections.singleton("moon/bar/a"),
                    forPrivilegeNames, applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("moon/bar/a", ResourcePrivileges
                    .builder("moon/bar/a").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            appPrivsByResource = role.checkApplicationResourcePrivileges("app2", Collections.singleton("foo/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges
                    .builder("foo/bar/a").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.checkApplicationResourcePrivileges("app2", Collections.singleton("moon/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("moon/bar/a", ResourcePrivileges
                    .builder("moon/bar/a").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.checkApplicationResourcePrivileges("unknown", Collections.singleton("moon/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false, Collections.singletonMap("moon/bar/a", ResourcePrivileges
                    .builder("moon/bar/a").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.checkApplicationResourcePrivileges("app2", Collections.singleton("moo/bar/a"),
                    Sets.newHashSet("read", "write", "all", "unknown"), applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcePrivilegesMap(false,
                    Collections.singletonMap("moo/bar/a", ResourcePrivileges.builder("moo/bar/a").addPrivilege("read", false)
                            .addPrivilege("write", true).addPrivilege("all", false).addPrivilege("unknown", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);
        }
    }

    private void verifyResourcesPrivileges(ResourcePrivilegesMap resourcePrivileges, ResourcePrivilegesMap expectedAppPrivsByResource) {
        assertThat(resourcePrivileges, equalTo(expectedAppPrivsByResource));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
                IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
        return mock;
    }

    private ApplicationPrivilege defineApplicationPrivilege(String app, String name, String... actions) {
        applicationPrivilegeDescriptors
                .add(new ApplicationPrivilegeDescriptor(app, name, Sets.newHashSet(actions), Collections.emptyMap()));
        return new ApplicationPrivilege(app, name, actions);
    }

    private static MapBuilder<String, ResourcePrivileges> mapBuilder() {
        return MapBuilder.newMapBuilder();
    }

}
