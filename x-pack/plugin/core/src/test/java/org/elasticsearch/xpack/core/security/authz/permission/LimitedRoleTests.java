/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.security.user.privileges.Role.ClusterPrivilegeName;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

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

        NullPointerException npe = expectThrows(NullPointerException.class, () -> LimitedRole.createLimitedRole(fromRole, null));
        assertThat(npe.getMessage(), containsString("limited by role is required to create limited role"));
    }

    public void testAuthorize() {
        IndexMetaData.Builder imbBuilder = IndexMetaData.builder("_index")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetaData.builder("_alias"));
        IndexMetaData.Builder imbBuilder1 = IndexMetaData.builder("_index1")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetaData.builder("_alias1"));
        MetaData md = MetaData.builder().put(imbBuilder).put(imbBuilder1).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton(ClusterPrivilegeName.MANAGE_SECURITY), Collections.emptyList())
                .add(IndexPrivilege.ALL, "_index")
                .add(IndexPrivilege.CREATE_INDEX, "_index1")
                .build();

        IndicesAccessControl iac = fromRole.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
        iac = fromRole.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_index1"), md, fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
        assertThat(iac.getIndexPermissions("_index1").isGranted(), is(true));

        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.ALL), Collections.emptyList())
                    .add(IndexPrivilege.READ, "_index")
                    .add(IndexPrivilege.NONE, "_index1")
                    .build();
            iac = limitedByRole.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = limitedByRole.authorize(DeleteIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = limitedByRole.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            iac = role.authorize(SearchAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(true));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = role.authorize(DeleteIndexAction.NAME, Sets.newHashSet("_index", "_alias1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
            iac = role.authorize(CreateIndexAction.NAME, Sets.newHashSet("_index", "_index1"), md, fieldPermissionsCache);
            assertThat(iac.getIndexPermissions("_index"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index").isGranted(), is(false));
            assertThat(iac.getIndexPermissions("_index1"), is(notNullValue()));
            assertThat(iac.getIndexPermissions("_index1").isGranted(), is(false));
        }
    }

    public void testCheckClusterAction() {
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton(ClusterPrivilegeName.MANAGE_SECURITY), Collections.emptyList())
                .build();
        assertThat(fromRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.ALL), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(limitedByRole.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(true));
            assertThat(role.checkClusterAction("cluster:other-action", mock(TransportRequest.class)), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.MONITOR), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterAction("cluster:monitor/me", mock(TransportRequest.class)), is(false));
            assertThat(role.checkClusterAction("cluster:admin/xpack/security/x", mock(TransportRequest.class)), is(false));
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
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(true));
        assertThat(fromRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(false));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-11"), is(false));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));
        }
        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-*").build();
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(limitedByRole.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-1"), is(true));
            assertThat(role.allowedIndicesMatcher(SearchAction.NAME).test("ind-2"), is(false));
        }
    }

    public void testCheckClusterPrivilege() {
        Role fromRole = Role.builder("a-role").cluster(Collections.singleton(ClusterPrivilegeName.MANAGE_SECURITY), Collections.emptyList())
                .build();
        assertThat(fromRole.checkClusterPrivilege(ClusterPrivilege.ALL), is(false));
        assertThat(fromRole.checkClusterPrivilege(ClusterPrivilege.MANAGE_SECURITY), is(true));

        {
            Role limitedByRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.ALL), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterPrivilege(ClusterPrivilege.ALL), is(true));
            assertThat(limitedByRole.checkClusterPrivilege(ClusterPrivilege.MANAGE_SECURITY), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterPrivilege(ClusterPrivilege.ALL), is(false));
            assertThat(role.checkClusterPrivilege(ClusterPrivilege.MANAGE_SECURITY), is(true));
        }
        {
            Role limitedByRole = Role.builder("scoped-role")
                    .cluster(Collections.singleton(ClusterPrivilegeName.MONITOR), Collections.emptyList()).build();
            assertThat(limitedByRole.checkClusterPrivilege(ClusterPrivilege.ALL), is(false));
            assertThat(limitedByRole.checkClusterPrivilege(ClusterPrivilege.MONITOR), is(true));
            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            assertThat(role.checkClusterPrivilege(ClusterPrivilege.ALL), is(false));
            assertThat(role.checkClusterPrivilege(ClusterPrivilege.MANAGE_SECURITY), is(false));
            assertThat(role.checkClusterPrivilege(ClusterPrivilege.MONITOR), is(false));
        }
    }

    public void testGetPrivilegesForIndexPatterns() {
        Role fromRole = Role.builder("a-role").add(IndexPrivilege.READ, "ind-1*").build();
        ResourcesPrivileges resourcePrivileges = fromRole.getResourcePrivileges(Collections.singletonList("ind-1-1-*"), true,
                Arrays.asList("read", "write"));
        ResourcesPrivileges expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("ind-1-1-*",
                ResourcePrivileges.builder().setResource("ind-1-1-*").addPrivilege("read", true).addPrivilege("write", false).build()));
        verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

        resourcePrivileges = fromRole.getResourcePrivileges(Collections.singletonList("ind-*"), true, Arrays.asList("read", "write"));
        expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("ind-*",
                ResourcePrivileges.builder().setResource("ind-*").addPrivilege("read", false).addPrivilege("write", false).build()));
        verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

        {
            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            resourcePrivileges = limitedByRole.getResourcePrivileges(Collections.singletonList("ind-1"), true, Arrays.asList("read"));
            expectedAppPrivsByResource = new ResourcesPrivileges(true, Collections.singletonMap("ind-1",
                    ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = limitedByRole.getResourcePrivileges(Collections.singletonList("ind-1-1-*"), true, Arrays.asList("read"));
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("ind-1-1-*",
                    ResourcePrivileges.builder().setResource("ind-1-1-*").addPrivilege("read", false).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = limitedByRole.getResourcePrivileges(Collections.singletonList("ind-*"), true, Arrays.asList("read"));
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("ind-*",
                    ResourcePrivileges.builder().setResource("ind-*").addPrivilege("read", false).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            resourcePrivileges = role.getResourcePrivileges(Collections.singletonList("ind-1"), true, Arrays.asList("read"));
            expectedAppPrivsByResource = new ResourcesPrivileges(true, Collections.singletonMap("ind-1",
                    ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build()));
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            resourcePrivileges = role.getResourcePrivileges(Arrays.asList("ind-1-1-*", "ind-1"), true, Arrays.asList("read"));
            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    mapBuilder().put("ind-1-1-*", ResourcePrivileges.builder().setResource("ind-1-1-*").addPrivilege("read", false).build())
                            .put("ind-1", ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build()).map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);
        }
        {
            fromRole = Role.builder("a-role")
                    .add(FieldPermissions.DEFAULT, Collections.emptySet(), IndexPrivilege.READ, true, "ind-1*", ".security").build();
            resourcePrivileges = fromRole.getResourcePrivileges(Arrays.asList("ind-1", ".security"), true, Arrays.asList("read"));
            //Map<String, ResourcePrivileges> expectedResourceToResourcePrivs = new HashMap<>();
            ;
            expectedAppPrivsByResource = new ResourcesPrivileges(true,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build())
                            .put(".security", ResourcePrivileges.builder().setResource(".security").addPrivilege("read", true).build())
                            .map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role limitedByRole = Role.builder("limited-role").add(IndexPrivilege.READ, "ind-1", "ind-2").build();
            resourcePrivileges = limitedByRole.getResourcePrivileges(Arrays.asList("ind-1", "ind-2", ".security"), true,
                    Arrays.asList("read"));

            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build())
                            .put("ind-2", ResourcePrivileges.builder().setResource("ind-2").addPrivilege("read", true).build())
                            .put(".security", ResourcePrivileges.builder().setResource(".security").addPrivilege("read", false).build())
                            .map());
            verifyResourcesPrivileges(resourcePrivileges, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            resourcePrivileges = role.getResourcePrivileges(Arrays.asList("ind-1", "ind-2", ".security"), true, Arrays.asList("read"));

            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    mapBuilder().put("ind-1", ResourcePrivileges.builder().setResource("ind-1").addPrivilege("read", true).build())
                            .put("ind-2", ResourcePrivileges.builder().setResource("ind-2").addPrivilege("read", false).build())
                            .put(".security", ResourcePrivileges.builder().setResource(".security").addPrivilege("read", false).build())
                            .map());
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

        List<String> forPrivilegeNames = Arrays.asList("read", "write", "all");
        ResourcesPrivileges appPrivsByResource = fromRole.getResourcePrivileges("app1", Collections.singletonList("*"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        ResourcesPrivileges expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("*", ResourcePrivileges
                .builder().setResource("*").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.getResourcePrivileges("app1", Collections.singletonList("foo/x/y"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("foo/x/y", ResourcePrivileges.builder()
                .setResource("foo/x/y").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.getResourcePrivileges("app2", Collections.singletonList("foo/bar/a"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges.builder()
                .setResource("foo/bar/a").addPrivilege("read", true).addPrivilege("write", true).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        appPrivsByResource = fromRole.getResourcePrivileges("app2", Collections.singletonList("moon/bar/a"), forPrivilegeNames,
                applicationPrivilegeDescriptors);
        expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("moon/bar/a", ResourcePrivileges.builder()
                .setResource("moon/bar/a").addPrivilege("read", false).addPrivilege("write", true).addPrivilege("all", false).build()));
        verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

        {
            Role limitedByRole = Role.builder("test-role-scoped").addApplicationPrivilege(app1Read, Collections.singleton("foo/scoped/*"))
                    .addApplicationPrivilege(app2Read, Collections.singleton("foo/bar/*"))
                    .addApplicationPrivilege(app2Write, Collections.singleton("moo/bar/*")).build();
            appPrivsByResource = limitedByRole.getResourcePrivileges("app1", Collections.singletonList("*"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("*", ResourcePrivileges.builder()
                    .setResource("*").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.getResourcePrivileges("app1", Collections.singletonList("foo/x/y"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("foo/x/y", ResourcePrivileges.builder()
                    .setResource("foo/x/y").addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.getResourcePrivileges("app2", Collections.singletonList("foo/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges.builder()
                    .setResource("foo/bar/a").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = limitedByRole.getResourcePrivileges("app2", Collections.singletonList("moon/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    Collections.singletonMap("moon/bar/a", ResourcePrivileges.builder().setResource("moon/bar/a")
                            .addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            Role role = LimitedRole.createLimitedRole(fromRole, limitedByRole);
            appPrivsByResource = role.getResourcePrivileges("app2", Collections.singletonList("foo/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false, Collections.singletonMap("foo/bar/a", ResourcePrivileges.builder()
                    .setResource("foo/bar/a").addPrivilege("read", true).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.getResourcePrivileges("app2", Collections.singletonList("moon/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    Collections.singletonMap("moon/bar/a", ResourcePrivileges.builder().setResource("moon/bar/a")
                            .addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.getResourcePrivileges("unknown", Collections.singletonList("moon/bar/a"), forPrivilegeNames,
                    applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    Collections.singletonMap("moon/bar/a", ResourcePrivileges.builder().setResource("moon/bar/a")
                            .addPrivilege("read", false).addPrivilege("write", false).addPrivilege("all", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);

            appPrivsByResource = role.getResourcePrivileges("app2", Collections.singletonList("moo/bar/a"),
                    Arrays.asList("read", "write", "all", "unknown"), applicationPrivilegeDescriptors);
            expectedAppPrivsByResource = new ResourcesPrivileges(false,
                    Collections.singletonMap("moo/bar/a", ResourcePrivileges.builder().setResource("moo/bar/a").addPrivilege("read", false)
                            .addPrivilege("write", true).addPrivilege("all", false).addPrivilege("unknown", false).build()));
            verifyResourcesPrivileges(appPrivsByResource, expectedAppPrivsByResource);
        }
    }

    private void verifyResourcesPrivileges(ResourcesPrivileges resourcePrivileges, ResourcesPrivileges expectedAppPrivsByResource) {
        assertThat(resourcePrivileges, equalTo(expectedAppPrivsByResource));
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
