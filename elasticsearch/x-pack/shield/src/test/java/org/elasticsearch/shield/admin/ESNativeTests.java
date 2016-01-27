/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.admin;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.action.admin.role.DeleteRoleResponse;
import org.elasticsearch.shield.action.admin.role.GetRolesResponse;
import org.elasticsearch.shield.action.admin.user.DeleteUserResponse;
import org.elasticsearch.shield.action.admin.user.GetUsersResponse;
import org.elasticsearch.shield.authc.esnative.ESNativeUsersStore;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authz.RoleDescriptor;
import org.elasticsearch.shield.authz.esnative.ESNativeRolesStore;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.client.ShieldClient;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Tests for the ESNativeUsersStore and ESNativeRolesStore
 */
public class ESNativeTests extends ShieldIntegTestCase {

    public void testDeletingNonexistingUserAndRole() throws Exception {
        ShieldClient c = new ShieldClient(client());
        DeleteUserResponse resp = c.prepareDeleteUser().user("joe").get();
        assertFalse("user shouldn't be found", resp.found());
        DeleteRoleResponse resp2 = c.prepareDeleteRole().role("role").get();
        assertFalse("role shouldn't be found", resp2.found());
    }

    public void testGettingUserThatDoesntExist() throws Exception {
        ShieldClient c = new ShieldClient(client());
        GetUsersResponse resp = c.prepareGetUsers().users("joe").get();
        assertFalse("user should not exist", resp.isExists());
        GetRolesResponse resp2 = c.prepareGetRoles().roles("role").get();
        assertFalse("role should not exist", resp2.isExists());
    }

    public void testAddAndGetUser() throws Exception {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating user");
        c.prepareAddUser()
                .username("joe")
                .password("s3kirt")
                .roles("role1", "user")
                .get();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers().users("joe").get();
        assertTrue("user should exist", resp.isExists());
        User joe = resp.users().get(0);
        assertEquals(joe.principal(), "joe");
        assertArrayEquals(joe.roles(), new String[]{"role1", "user"});

        logger.info("--> adding two more users");
        c.prepareAddUser()
                .username("joe2")
                .password("s3kirt2")
                .roles("role2", "user")
                .get();
        c.prepareAddUser()
                .username("joe3")
                .password("s3kirt3")
                .roles("role3", "user")
                .get();
        // Since getting multiple users relies on them being visible to search, perform a refresh
        refresh();
        GetUsersResponse allUsersResp = c.prepareGetUsers().get();
        assertTrue("users should exist", allUsersResp.isExists());
        assertEquals("should be 3 users total", 3, allUsersResp.users().size());
        List<String> names = new ArrayList<>(3);
        for (User u : allUsersResp.users()) {
            names.add(u.principal());
        }
        CollectionUtil.timSort(names);
        assertArrayEquals(new String[]{"joe", "joe2", "joe3"}, names.toArray(Strings.EMPTY_ARRAY));

        GetUsersResponse someUsersResp = c.prepareGetUsers().users("joe", "joe3").get();
        assertTrue("users should exist", someUsersResp.isExists());
        assertEquals("should be 2 users returned", 2, someUsersResp.users().size());
        names = new ArrayList<>(2);
        for (User u : someUsersResp.users()) {
            names.add(u.principal());
        }
        CollectionUtil.timSort(names);
        assertArrayEquals(new String[]{"joe", "joe3"}, names.toArray(Strings.EMPTY_ARRAY));

        logger.info("--> deleting user");
        DeleteUserResponse delResp = c.prepareDeleteUser().user("joe").get();
        assertTrue(delResp.found());
        logger.info("--> retrieving user");
        resp = c.prepareGetUsers().users("joe").get();
        assertFalse("user should not exist after being deleted", resp.isExists());
    }

    public void testAddAndGetRole() throws Exception {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating role");
        c.prepareAddRole()
                .name("test_role")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .get();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);
        logger.info("--> retrieving role");
        GetRolesResponse resp = c.prepareGetRoles().roles("test_role").get();
        assertTrue("role should exist", resp.isExists());
        RoleDescriptor testRole = resp.roles().get(0);
        assertNotNull(testRole);

        c.prepareAddRole()
                .name("test_role2")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .get();
        c.prepareAddRole()
                .name("test_role3")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .get();

        // Refresh to make new roles visible
        refresh();

        logger.info("--> retrieving all roles");
        GetRolesResponse allRolesResp = c.prepareGetRoles().get();
        assertTrue("roles should exist", allRolesResp.isExists());
        assertEquals("should be 3 roles total", 3, allRolesResp.roles().size());

        logger.info("--> retrieving all roles");
        GetRolesResponse someRolesResp = c.prepareGetRoles().roles("test_role", "test_role3").get();
        assertTrue("roles should exist", someRolesResp.isExists());
        assertEquals("should be 2 roles total", 2, someRolesResp.roles().size());

        logger.info("--> deleting role");
        DeleteRoleResponse delResp = c.prepareDeleteRole().role("test_role").get();
        assertTrue(delResp.found());
        logger.info("--> retrieving role");
        GetRolesResponse resp2 = c.prepareGetRoles().roles("test_role").get();
        assertFalse("role should not exist after being deleted", resp2.isExists());
    }

    public void testAddUserAndRoleThenAuth() throws Exception {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating role");
        c.prepareAddRole()
                .name("test_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"match_all\": {}}"))
                .get();
        logger.error("--> creating user");
        c.prepareAddUser()
                .username("joe")
                .password("s3krit")
                .roles("test_role")
                .get();
        refresh();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers().users("joe").get();
        assertTrue("user should exist", resp.isExists());

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefresh(true).get();

        String token = basicAuthHeaderValue("joe", new SecuredString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(searchResp.getHits().getTotalHits(), 1L);
    }

    public void testUpdatingUserAndAuthentication() throws Exception {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating user");
        c.prepareAddUser()
                .username("joe")
                .password("s3krit")
                .roles(ShieldSettingsSource.DEFAULT_ROLE)
                .get();
        refresh();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers().users("joe").get();
        assertTrue("user should exist", resp.isExists());
        assertThat(resp.users().get(0).roles(), arrayContaining(ShieldSettingsSource.DEFAULT_ROLE));

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefresh(true).get();
        String token = basicAuthHeaderValue("joe", new SecuredString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(searchResp.getHits().getTotalHits(), 1L);

        c.prepareAddUser()
                .username("joe")
                .password("s3krit2")
                .roles(ShieldSettingsSource.DEFAULT_ROLE)
                .get();

        try {
            client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();
            fail("authentication with old credentials after an update to the user should fail!");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        }

        token = basicAuthHeaderValue("joe", new SecuredString("s3krit2".toCharArray()));
        searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();
        assertEquals(searchResp.getHits().getTotalHits(), 1L);
    }

    public void testCreateDeleteAuthenticate() {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating user");
        c.prepareAddUser()
                .username("joe")
                .password("s3krit")
                .roles(ShieldSettingsSource.DEFAULT_ROLE)
                .get();
        refresh();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers().users("joe").get();
        assertTrue("user should exist", resp.isExists());
        assertThat(resp.users().get(0).roles(), arrayContaining(ShieldSettingsSource.DEFAULT_ROLE));

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefresh(true).get();
        String token = basicAuthHeaderValue("joe", new SecuredString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(searchResp.getHits().getTotalHits(), 1L);

        DeleteUserResponse response = c.prepareDeleteUser().user("joe").get();
        assertThat(response.found(), is(true));
        try {
            client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();
            fail("authentication with a deleted user should fail!");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        }
    }

    public void testCreateAndUpdateRole() {
        final boolean authenticate = randomBoolean();
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating role");
        c.prepareAddRole()
                .name("test_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"match_all\": {}}"))
                .get();
        logger.error("--> creating user");
        c.prepareAddUser()
                .username("joe")
                .password("s3krit")
                .roles("test_role")
                .get();
        refresh();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);

        if (authenticate) {
            final String token = basicAuthHeaderValue("joe", new SecuredString("s3krit".toCharArray()));
            ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
            assertFalse(response.isTimedOut());
            c.prepareAddRole()
                    .name("test_role")
                    .cluster("none")
                    .addIndices(new String[]{"*"}, new String[]{"read"},
                            new String[]{"body", "title"}, new BytesArray("{\"match_all\": {}}"))
                    .get();
            try {
                client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
                fail("user should not be able to execute any cluster actions!");
            } catch (ElasticsearchSecurityException e) {
                assertThat(e.status(), is(RestStatus.FORBIDDEN));
            }
        } else {
            GetRolesResponse getRolesResponse = c.prepareGetRoles().roles("test_role").get();
            assertTrue("test_role does not exist!", getRolesResponse.isExists());
            assertTrue("any cluster permission should be authorized",
                    Role.builder(getRolesResponse.roles().get(0)).build().cluster().check("cluster:admin/foo"));
            c.prepareAddRole()
                    .name("test_role")
                    .cluster("none")
                    .addIndices(new String[]{"*"}, new String[]{"read"},
                            new String[]{"body", "title"}, new BytesArray("{\"match_all\": {}}"))
                    .get();
            getRolesResponse = c.prepareGetRoles().roles("test_role").get();
            assertTrue("test_role does not exist!", getRolesResponse.isExists());

            assertFalse("no cluster permission should be authorized",
                    Role.builder(getRolesResponse.roles().get(0)).build().cluster().check("cluster:admin/bar"));
        }
    }

    public void testAuthenticateWithDeletedRole() {
        ShieldClient c = new ShieldClient(client());
        logger.error("--> creating role");
        c.prepareAddRole()
                .name("test_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"read"},
                        new String[]{"body", "title"}, new BytesArray("{\"match_all\": {}}"))
                .get();
        c.prepareAddUser()
                .username("joe")
                .password("s3krit")
                .roles("test_role")
                .get();
        refresh();
        logger.error("--> waiting for .shield index");
        ensureGreen(ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME);

        final String token = basicAuthHeaderValue("joe", new SecuredString("s3krit".toCharArray()));
        ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
        assertFalse(response.isTimedOut());
        c.prepareDeleteRole().role("test_role").get();
        try {
            client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
            fail("user should not be able to execute any actions!");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }
    }

    @Before
    public void ensureStoresStarted() throws Exception {
        // Clear the realm cache for all realms since we use a SUITE scoped cluster
        ShieldClient client = new ShieldClient(client());
        client.prepareClearRealmCache().get();

        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeUsersStore.State.STARTED));
                }
            });
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), is(ESNativeRolesStore.State.STARTED));
                }
            });
        }
    }

    @After
    public void stopESNativeStores() throws Exception {
        for (ESNativeUsersStore store : internalCluster().getInstances(ESNativeUsersStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeUsersStore.State.STOPPED, ESNativeUsersStore.State.FAILED));
                }
            });
            store.reset();
        }

        for (ESNativeRolesStore store : internalCluster().getInstances(ESNativeRolesStore.class)) {
            store.stop();
            // the store may already be stopping so wait until it is stopped
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    assertThat(store.state(), isOneOf(ESNativeRolesStore.State.STOPPED, ESNativeRolesStore.State.FAILED));
                }
            });
            store.reset();
        }
    }
}
