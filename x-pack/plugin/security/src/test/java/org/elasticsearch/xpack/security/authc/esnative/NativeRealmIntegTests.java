/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleResponse;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordResponse;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserResponse;
import org.elasticsearch.xpack.core.security.action.user.GetUsersResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_INDEX_NAME;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.INTERNAL_SECURITY_INDEX;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the NativeUsersStore and NativeRolesStore
 */
public class NativeRealmIntegTests extends NativeRealmIntegTestCase {

    private static boolean anonymousEnabled;
    private static Hasher hasher;

    private boolean roleExists;

    @BeforeClass
    public static void init() {
        anonymousEnabled = randomBoolean();
        hasher = getFastStoredHashAlgoForTests();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("xpack.security.authc.password_hashing.algorithm", hasher.name());
        if (anonymousEnabled) {
            builder.put(AnonymousUser.ROLES_SETTING.getKey(), "native_anonymous");
        }
        return builder.build();
    }

    @Before
    public void setupAnonymousRoleIfNecessary() throws Exception {
        roleExists = anonymousEnabled && randomBoolean();
        if (anonymousEnabled) {
            if (roleExists) {
                logger.info("anonymous is enabled. creating [native_anonymous] role");
                PutRoleResponse response = securityClient()
                        .preparePutRole("native_anonymous")
                        .cluster("ALL")
                        .addIndices(new String[]{"*"}, new String[]{"ALL"}, null, null, null)
                        .get();
                assertTrue(response.isCreated());
            } else {
                logger.info("anonymous is enabled, but configured with a missing role");
            }
        }
    }

    public void testDeletingNonexistingUserAndRole() throws Exception {
        SecurityClient c = securityClient();
        // first create the index so it exists
        c.preparePutUser("joe", "s3kirt".toCharArray(), hasher, "role1", "user").get();
        DeleteUserResponse resp = c.prepareDeleteUser("missing").get();
        assertFalse("user shouldn't be found", resp.found());
        DeleteRoleResponse resp2 = c.prepareDeleteRole("role").get();
        assertFalse("role shouldn't be found", resp2.found());
    }

    public void testGettingUserThatDoesntExist() throws Exception {
        SecurityClient c = securityClient();
        GetUsersResponse resp = c.prepareGetUsers("joe").get();
        assertFalse("user should not exist", resp.hasUsers());
        GetRolesResponse resp2 = c.prepareGetRoles().names("role").get();
        assertFalse("role should not exist", resp2.hasRoles());
    }

    public void testAddAndGetUser() throws Exception {
        SecurityClient c = securityClient();
        final List<User> existingUsers = Arrays.asList(c.prepareGetUsers().get().users());
        final int existing = existingUsers.size();
        logger.error("--> creating user");
        c.preparePutUser("joe", "s3kirt".toCharArray(), hasher, "role1", "user").get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers("joe").get();
        assertTrue("user should exist", resp.hasUsers());
        User joe = resp.users()[0];
        assertEquals("joe", joe.principal());
        assertArrayEquals(joe.roles(), new String[]{"role1", "user"});

        logger.info("--> adding two more users");
        c.preparePutUser("joe2", "s3kirt2".toCharArray(), hasher, "role2", "user").get();
        c.preparePutUser("joe3", "s3kirt3".toCharArray(), hasher, "role3", "user").get();
        GetUsersResponse allUsersResp = c.prepareGetUsers().get();
        assertTrue("users should exist", allUsersResp.hasUsers());
        assertEquals("should be " + (3 + existing) + " users total", 3 + existing, allUsersResp.users().length);
        List<String> names = new ArrayList<>(3);
        for (User u : allUsersResp.users()) {
            if (existingUsers.contains(u) == false) {
                names.add(u.principal());
            }
        }
        CollectionUtil.timSort(names);
        assertArrayEquals(new String[] { "joe", "joe2", "joe3" }, names.toArray(Strings.EMPTY_ARRAY));

        GetUsersResponse someUsersResp = c.prepareGetUsers("joe", "joe3").get();
        assertTrue("users should exist", someUsersResp.hasUsers());
        assertEquals("should be 2 users returned", 2, someUsersResp.users().length);
        names = new ArrayList<>(2);
        for (User u : someUsersResp.users()) {
            names.add(u.principal());
        }
        CollectionUtil.timSort(names);
        assertArrayEquals(new String[]{"joe", "joe3"}, names.toArray(Strings.EMPTY_ARRAY));

        logger.info("--> deleting user");
        DeleteUserResponse delResp = c.prepareDeleteUser("joe").get();
        assertTrue(delResp.found());
        logger.info("--> retrieving user");
        resp = c.prepareGetUsers("joe").get();
        assertFalse("user should not exist after being deleted", resp.hasUsers());
    }

    public void testAddAndGetRole() throws Exception {
        SecurityClient c = securityClient();
        final List<RoleDescriptor> existingRoles = Arrays.asList(c.prepareGetRoles().get().roles());
        final int existing = existingRoles.size();
        final Map<String, Object> metadata = Collections.singletonMap("key", randomAlphaOfLengthBetween(1, 10));
        logger.error("--> creating role");
        c.preparePutRole("test_role")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                        new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .metadata(metadata)
                .get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);
        logger.info("--> retrieving role");
        GetRolesResponse resp = c.prepareGetRoles().names("test_role").get();
        assertTrue("role should exist", resp.hasRoles());
        RoleDescriptor testRole = resp.roles()[0];
        assertNotNull(testRole);
        assertThat(testRole.getMetadata().size(), is(1));
        assertThat(testRole.getMetadata().get("key"), is(metadata.get("key")));

        c.preparePutRole("test_role2")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                        new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .get();
        c.preparePutRole("test_role3")
                .cluster("all", "none")
                .runAs("root", "nobody")
                .addIndices(new String[]{"index"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                        new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .get();

        logger.info("--> retrieving all roles");
        GetRolesResponse allRolesResp = c.prepareGetRoles().get();
        assertTrue("roles should exist", allRolesResp.hasRoles());
        assertEquals("should be " + (3 + existing) + " roles total", 3 + existing, allRolesResp.roles().length);

        logger.info("--> retrieving test_role and test_role3");
        GetRolesResponse someRolesResp = c.prepareGetRoles().names("test_role", "test_role3").get();
        assertTrue("roles should exist", someRolesResp.hasRoles());
        assertEquals("should be 2 roles total", 2, someRolesResp.roles().length);

        logger.info("--> deleting role");
        DeleteRoleResponse delResp = c.prepareDeleteRole("test_role").get();
        assertTrue(delResp.found());
        logger.info("--> retrieving role");
        GetRolesResponse resp2 = c.prepareGetRoles().names("test_role").get();
        assertFalse("role should not exist after being deleted", resp2.hasRoles());
    }

    public void testAddUserAndRoleThenAuth() throws Exception {
        SecurityClient c = securityClient();
        logger.error("--> creating role");
        c.preparePutRole("test_role")
                .cluster("all")
                .addIndices(new String[] { "*" }, new String[] { "read" }, new String[]{"body", "title"}, null,
                        new BytesArray("{\"match_all\": {}}"))
                .get();
        logger.error("--> creating user");
        c.preparePutUser("joe", "s3krit".toCharArray(), hasher, "test_role").get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers("joe").get();
        assertTrue("user should exist", resp.hasUsers());

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();

        String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(1L, searchResp.getHits().getTotalHits().value);
    }

    public void testUpdatingUserAndAuthentication() throws Exception {
        SecurityClient c = securityClient();
        logger.error("--> creating user");
        c.preparePutUser("joe", "s3krit".toCharArray(), hasher, SecuritySettingsSource.TEST_ROLE).get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers("joe").get();
        assertTrue("user should exist", resp.hasUsers());
        assertThat(resp.users()[0].roles(), arrayContaining(SecuritySettingsSource.TEST_ROLE));

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
        String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(1L, searchResp.getHits().getTotalHits().value);

        c.preparePutUser("joe", "s3krit2".toCharArray(), hasher, SecuritySettingsSource.TEST_ROLE).get();

        try {
            client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();
            fail("authentication with old credentials after an update to the user should fail!");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertThat(e.status(), is(RestStatus.UNAUTHORIZED));
        }

        token = basicAuthHeaderValue("joe", new SecureString("s3krit2".toCharArray()));
        searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();
        assertEquals(1L, searchResp.getHits().getTotalHits().value);
    }

    public void testCreateDeleteAuthenticate() {
        SecurityClient c = securityClient();
        logger.error("--> creating user");
        c.preparePutUser("joe", "s3krit".toCharArray(), hasher,
            SecuritySettingsSource.TEST_ROLE).get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);
        logger.info("--> retrieving user");
        GetUsersResponse resp = c.prepareGetUsers("joe").get();
        assertTrue("user should exist", resp.hasUsers());
        assertThat(resp.users()[0].roles(), arrayContaining(SecuritySettingsSource.TEST_ROLE));

        createIndex("idx");
        ensureGreen("idx");
        // Index a document with the default test user
        client().prepareIndex("idx", "doc", "1").setSource("body", "foo").setRefreshPolicy(IMMEDIATE).get();
        String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        SearchResponse searchResp = client().filterWithHeader(Collections.singletonMap("Authorization", token)).prepareSearch("idx").get();

        assertEquals(1L, searchResp.getHits().getTotalHits().value);

        DeleteUserResponse response = c.prepareDeleteUser("joe").get();
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
        SecurityClient c = securityClient();
        logger.error("--> creating role");
        c.preparePutRole("test_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                        new BytesArray("{\"match_all\": {}}"))
                .get();
        logger.error("--> creating user");
        c.preparePutUser("joe", "s3krit".toCharArray(), hasher, "test_role").get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);

        if (authenticate) {
            final String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
            ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster()
                    .prepareHealth().get();
            assertFalse(response.isTimedOut());
            c.preparePutRole("test_role")
                    .cluster("none")
                    .addIndices(new String[]{"*"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                            new BytesArray("{\"match_all\": {}}"))
                    .get();
            if (anonymousEnabled && roleExists) {
                assertNoTimeout(client()
                        .filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
            } else {
                ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                        .filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
                assertThat(e.status(), is(RestStatus.FORBIDDEN));
            }
        } else {
            final TransportRequest request = mock(TransportRequest.class);
            GetRolesResponse getRolesResponse = c.prepareGetRoles().names("test_role").get();
            assertTrue("test_role does not exist!", getRolesResponse.hasRoles());
            assertTrue("any cluster permission should be authorized",
                    Role.builder(getRolesResponse.roles()[0], null).build().cluster().check("cluster:admin/foo", request));

            c.preparePutRole("test_role")
                    .cluster("none")
                    .addIndices(new String[]{"*"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                            new BytesArray("{\"match_all\": {}}"))
                    .get();
            getRolesResponse = c.prepareGetRoles().names("test_role").get();
            assertTrue("test_role does not exist!", getRolesResponse.hasRoles());

            assertFalse("no cluster permission should be authorized",
                    Role.builder(getRolesResponse.roles()[0], null).build().cluster().check("cluster:admin/bar", request));
        }
    }

    public void testAuthenticateWithDeletedRole() {
        SecurityClient c = securityClient();
        logger.error("--> creating role");
        c.preparePutRole("test_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"read"}, new String[]{"body", "title"}, null,
                        new BytesArray("{\"match_all\": {}}"))
                .get();
        c.preparePutUser("joe", "s3krit".toCharArray(), hasher, "test_role").get();
        logger.error("--> waiting for .security index");
        ensureGreen(SECURITY_INDEX_NAME);

        final String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster()
                .prepareHealth().get();
        assertFalse(response.isTimedOut());
        c.prepareDeleteRole("test_role").get();
        if (anonymousEnabled && roleExists) {
            assertNoTimeout(
                    client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
        } else {
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
                    client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
        }
    }

    public void testPutUserWithoutPassword() {
        SecurityClient client = securityClient();
        // create some roles
        client.preparePutRole("admin_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"all"}, null, null, null)
                .get();
        client.preparePutRole("read_role")
                .cluster("none")
                .addIndices(new String[]{"*"}, new String[]{"read"}, null, null, null)
                .get();

        assertThat(client.prepareGetUsers("joes").get().hasUsers(), is(false));
        // check that putting a user without a password fails if the user doesn't exist
        try {
            client.preparePutUser("joe", null, hasher, "admin_role").get();
            fail("cannot create a user without a password");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("password must be specified"));
        }

        assertThat(client.prepareGetUsers("joes").get().hasUsers(), is(false));

        // create joe with a password and verify the user works
        client.preparePutUser("joe", SecuritySettingsSourceField.TEST_PASSWORD.toCharArray(),
            hasher, "admin_role").get();
        assertThat(client.prepareGetUsers("joe").get().hasUsers(), is(true));
        final String token = basicAuthHeaderValue("joe", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster()
                .prepareHealth().get();
        assertFalse(response.isTimedOut());

        // modify joe without sending the password
        client.preparePutUser("joe", null, hasher, "read_role").fullName("Joe Smith").get();
        GetUsersResponse getUsersResponse = client.prepareGetUsers("joe").get();
        assertThat(getUsersResponse.hasUsers(), is(true));
        assertThat(getUsersResponse.users().length, is(1));
        User joe = getUsersResponse.users()[0];
        assertThat(joe.roles(), arrayContaining("read_role"));
        assertThat(joe.fullName(), is("Joe Smith"));

        // test that role change took effect if anonymous is disabled as anonymous grants monitoring permissions...
        if (anonymousEnabled && roleExists) {
            assertNoTimeout(
                    client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
        } else {
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
                    client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
            assertThat(e.status(), is(RestStatus.FORBIDDEN));
            assertThat(e.getMessage(), containsString("authorized"));
        }

        // update the user with password and admin role again
        String secondPassword = SecuritySettingsSourceField.TEST_PASSWORD + "2";
        client.preparePutUser("joe", secondPassword.toCharArray(), hasher, "admin_role").
            fullName("Joe Smith").get();
        getUsersResponse = client.prepareGetUsers("joe").get();
        assertThat(getUsersResponse.hasUsers(), is(true));
        assertThat(getUsersResponse.users().length, is(1));
        joe = getUsersResponse.users()[0];
        assertThat(joe.roles(), arrayContaining("admin_role"));
        assertThat(joe.fullName(), is("Joe Smith"));

        // validate that joe cannot auth with the old token
        try {
            client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
            fail("should not authenticate with old password");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("authenticate"));
        }

        // test with new password and role
        response = client()
                .filterWithHeader(
                        Collections.singletonMap("Authorization",
                                basicAuthHeaderValue("joe", new SecureString(secondPassword.toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertFalse(response.isTimedOut());
    }

    public void testCannotCreateUserWithShortPassword() throws Exception {
        SecurityClient client = securityClient();
        try {
            client.preparePutUser("joe", randomAlphaOfLengthBetween(0, 5).toCharArray(), hasher,
                "admin_role").get();
            fail("cannot create a user without a password < 6 characters");
        } catch (IllegalArgumentException v) {
            assertThat(v.getMessage().contains("password"), is(true));
        }
    }

    public void testCannotCreateUserWithInvalidCharactersInName() throws Exception {
        SecurityClient client = securityClient();
        IllegalArgumentException v = expectThrows(IllegalArgumentException.class,
            () -> client.preparePutUser("fóóbár", "my-am@zing-password".toCharArray(), hasher,
                "admin_role").get()
        );
        assertThat(v.getMessage(), containsString("names must be"));
    }

    public void testUsersAndRolesDoNotInterfereWithIndicesStats() throws Exception {
        client().prepareIndex("foo", "bar").setSource("ignore", "me").get();

        SecurityClient client = securityClient();
        if (randomBoolean()) {
            client.preparePutUser("joe", "s3krit".toCharArray(), hasher,
                SecuritySettingsSource.TEST_ROLE).get();
        } else {
            client.preparePutRole("read_role")
                    .cluster("none")
                    .addIndices(new String[]{"*"}, new String[]{"read"}, null, null, null)
                    .get();
        }

        IndicesStatsResponse response = client().admin().indices().prepareStats("foo", SECURITY_INDEX_NAME).get();
        assertThat(response.getFailedShards(), is(0));
        assertThat(response.getIndices().size(), is(2));
        assertThat(response.getIndices().get(INTERNAL_SECURITY_INDEX), notNullValue());
        assertThat(response.getIndices().get(INTERNAL_SECURITY_INDEX).getIndex(),
                is(INTERNAL_SECURITY_INDEX));
    }

    public void testOperationsOnReservedUsers() throws Exception {
        final String username = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().preparePutUser(username, randomBoolean() ? SecuritySettingsSourceField.TEST_PASSWORD.toCharArray()
                    : null, hasher, "admin").get());
        assertThat(exception.getMessage(), containsString("user [" + username + "] is reserved"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().prepareDeleteUser(username).get());
        assertThat(exception.getMessage(), containsString("user [" + username + "] is reserved"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().prepareDeleteUser(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME).get());
        assertThat(exception.getMessage(), containsString("user [" + AnonymousUser.DEFAULT_ANONYMOUS_USERNAME + "] is anonymous"));

        exception = expectThrows(IllegalArgumentException.class,
            () -> securityClient().prepareChangePassword(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME, "foobar".toCharArray(),
                hasher).get());
        assertThat(exception.getMessage(), containsString("user [" + AnonymousUser.DEFAULT_ANONYMOUS_USERNAME + "] is anonymous"));

        exception = expectThrows(IllegalArgumentException.class,
            () -> securityClient().preparePutUser(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME, "foobar".toCharArray(),
                hasher).get());
        assertThat(exception.getMessage(), containsString("user [" + AnonymousUser.DEFAULT_ANONYMOUS_USERNAME + "] is anonymous"));

        exception = expectThrows(IllegalArgumentException.class,
            () -> securityClient().preparePutUser(SystemUser.NAME, "foobar".toCharArray(), hasher).get());
        assertThat(exception.getMessage(), containsString("user [" + SystemUser.NAME + "] is internal"));

        exception = expectThrows(IllegalArgumentException.class,
            () -> securityClient().prepareChangePassword(SystemUser.NAME, "foobar".toCharArray(),
                hasher).get());
        assertThat(exception.getMessage(), containsString("user [" + SystemUser.NAME + "] is internal"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().prepareDeleteUser(SystemUser.NAME).get());
        assertThat(exception.getMessage(), containsString("user [" + SystemUser.NAME + "] is internal"));

        // get should work
        GetUsersResponse response = securityClient().prepareGetUsers(username).get();
        assertThat(response.hasUsers(), is(true));
        assertThat(response.users()[0].principal(), is(username));

        // authenticate should work
        AuthenticateResponse authenticateResponse = client()
                .filterWithHeader(Collections.singletonMap("Authorization",
                                basicAuthHeaderValue(username, getReservedPassword())))
                .execute(AuthenticateAction.INSTANCE, new AuthenticateRequest(username))
                .get();
        assertThat(authenticateResponse.authentication().getUser().principal(), is(username));
        assertThat(authenticateResponse.authentication().getAuthenticatedBy().getName(), equalTo("reserved"));
        assertThat(authenticateResponse.authentication().getAuthenticatedBy().getType(), equalTo("reserved"));
        assertNull(authenticateResponse.authentication().getLookedUpBy());
    }

    public void testOperationsOnReservedRoles() throws Exception {
        final String name = randomFrom(ReservedRolesStore.names());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().preparePutRole(name).cluster("monitor").get());
        assertThat(exception.getMessage(), containsString("role [" + name + "] is reserved"));

        exception = expectThrows(IllegalArgumentException.class,
                () -> securityClient().prepareDeleteRole(name).get());
        assertThat(exception.getMessage(), containsString("role [" + name + "] is reserved"));

        // get role is allowed
        GetRolesResponse response = securityClient().prepareGetRoles(name).get();
        assertThat(response.hasRoles(), is(true));
        assertThat(response.roles()[0].getName(), is(name));
    }

    public void testCreateAndChangePassword() throws Exception {
        securityClient().preparePutUser("joe", "s3krit".toCharArray(), hasher,
            SecuritySettingsSource.TEST_ROLE).get();
        final String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token))
                .admin().cluster().prepareHealth().get();
        assertThat(response.isTimedOut(), is(false));

        ChangePasswordResponse passwordResponse = securityClient(
                client().filterWithHeader(Collections.singletonMap("Authorization", token)))
            .prepareChangePassword("joe", SecuritySettingsSourceField.TEST_PASSWORD.toCharArray(), hasher).get();
        assertThat(passwordResponse, notNullValue());


        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
        assertThat(expected.status(), is(RestStatus.UNAUTHORIZED));

        response = client()
                .filterWithHeader(
                        Collections.singletonMap("Authorization",
                                basicAuthHeaderValue("joe", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)))
                .admin().cluster().prepareHealth().get();
        assertThat(response.isTimedOut(), is(false));
    }

    public void testRolesUsageStats() throws Exception {
        NativeRolesStore rolesStore = internalCluster().getInstance(NativeRolesStore.class);
        long roles = anonymousEnabled && roleExists ? 1L: 0L;
        logger.info("--> running testRolesUsageStats with anonymousEnabled=[{}], roleExists=[{}]",
                    anonymousEnabled, roleExists);
        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        rolesStore.usageStats(future);
        Map<String, Object> usage = future.get();
        assertEquals(roles, usage.get("size"));
        assertThat(usage.get("fls"), is(false));
        assertThat(usage.get("dls"), is(false));

        final boolean fls = randomBoolean();
        final boolean dls = randomBoolean();
        SecurityClient client = new SecurityClient(client());
        PutRoleResponse putRoleResponse = client.preparePutRole("admin_role")
                .cluster("all")
                .addIndices(new String[]{"*"}, new String[]{"all"}, null, null, null)
                .get();
        assertThat(putRoleResponse.isCreated(), is(true));
        roles++;
        if (fls) {
            PutRoleResponse roleResponse;
            String[] fields = new String[]{"foo"};
            final String[] grantedFields;
            final String[] deniedFields;
            if (randomBoolean()) {
                grantedFields = fields;
                deniedFields = null;
            } else {
                grantedFields = null;
                deniedFields = fields;
            }
            roleResponse = client.preparePutRole("admin_role_fls")
                    .cluster("all")
                    .addIndices(new String[]{"*"}, new String[]{"all"}, grantedFields, deniedFields, null)
                    .get();
            assertThat(roleResponse.isCreated(), is(true));
            roles++;
        }

        if (dls) {
            PutRoleResponse roleResponse = client.preparePutRole("admin_role_dls")
                    .cluster("all")
                    .addIndices(new String[]{"*"}, new String[]{"all"}, null, null, new BytesArray("{ \"match_all\": {} }"))
                    .get();
            assertThat(roleResponse.isCreated(), is(true));
            roles++;
        }

        client.prepareClearRolesCache().get();

        future = new PlainActionFuture<>();
        rolesStore.usageStats(future);
        usage = future.get();
        assertThat(usage.get("size"), is(roles));
        assertThat(usage.get("fls"), is(fls));
        assertThat(usage.get("dls"), is(dls));
    }

    public void testRealmUsageStats() {
        final int numNativeUsers = scaledRandomIntBetween(1, 32);
        SecurityClient securityClient = new SecurityClient(client());
        for (int i = 0; i < numNativeUsers; i++) {
            securityClient.preparePutUser("joe" + i, "s3krit".toCharArray(), hasher,
                "superuser").get();
        }

        XPackUsageResponse response = new XPackUsageRequestBuilder(client()).get();
        Optional<XPackFeatureSet.Usage> securityUsage = response.getUsages().stream()
            .filter(usage -> usage instanceof SecurityFeatureSetUsage)
            .findFirst();
        assertTrue(securityUsage.isPresent());
        SecurityFeatureSetUsage securityFeatureSetUsage = (SecurityFeatureSetUsage) securityUsage.get();
        Map<String, Object> realmsUsage = securityFeatureSetUsage.getRealmsUsage();
        assertNotNull(realmsUsage);
        assertNotNull(realmsUsage.get("native"));
        assertNotNull(((Map<String, Object>) realmsUsage.get("native")).get("size"));
        List<Long> sizeList = (List<Long>) ((Map<String, Object>) realmsUsage.get("native")).get("size");
        assertEquals(1, sizeList.size());
        assertEquals(numNativeUsers, Math.toIntExact(sizeList.get(0)));
    }

    public void testSetEnabled() throws Exception {

        securityClient().preparePutUser("joe", "s3krit".toCharArray(), hasher,
            SecuritySettingsSource.TEST_ROLE).get();
        final String token = basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()));
        ClusterHealthResponse response = client().filterWithHeader(Collections.singletonMap("Authorization", token))
                .admin().cluster().prepareHealth().get();
        assertThat(response.isTimedOut(), is(false));

        securityClient(client()).prepareSetEnabled("joe", false).get();

        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get());
        assertThat(expected.status(), is(RestStatus.UNAUTHORIZED));

        securityClient(client()).prepareSetEnabled("joe", true).get();

        response = client().filterWithHeader(Collections.singletonMap("Authorization", token)).admin().cluster().prepareHealth().get();
        assertThat(response.isTimedOut(), is(false));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> securityClient(client()).prepareSetEnabled("not_a_real_user", false).get());
        assertThat(e.getMessage(), containsString("only existing users can be disabled"));
    }

    public void testNegativeLookupsThenCreateRole() throws Exception {
        SecurityClient securityClient = new SecurityClient(client());
        securityClient.preparePutUser("joe", "s3krit".toCharArray(), hasher, "unknown_role").get();

        final int negativeLookups = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < negativeLookups; i++) {
            if (anonymousEnabled && roleExists) {
                ClusterHealthResponse response = client()
                        .filterWithHeader(Collections.singletonMap("Authorization",
                                basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()))))
                        .admin().cluster().prepareHealth().get();
                assertNoTimeout(response);
            } else {
                ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                        .filterWithHeader(Collections.singletonMap("Authorization",
                                basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()))))
                        .admin().cluster().prepareHealth().get());
                assertThat(e.status(), is(RestStatus.FORBIDDEN));
            }
        }

        securityClient.preparePutRole("unknown_role").cluster("all").get();
        ClusterHealthResponse response = client()
                .filterWithHeader(Collections.singletonMap("Authorization",
                        basicAuthHeaderValue("joe", new SecureString("s3krit".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    /**
     * Tests that multiple concurrent run as requests can be authenticated successfully. There was a bug in the Cache implementation used
     * for our internal realms that caused some run as requests to fail even when the authentication was valid and the run as user existed.
     *
     * The issue was that when iterating the realms there would be failed lookups and under heavy concurrency, requests will wait for an
     * existing load attempt in the cache. The original caller was thrown an ExecutionException with a nested NullPointerException since
     * the loader returned a null value, while the other caller(s) would get a null value unexpectedly
     */
    public void testConcurrentRunAs() throws Exception {
        securityClient().preparePutUser("joe", "s3krit".toCharArray(), hasher, SecuritySettingsSource
            .TEST_ROLE).get();
        securityClient().preparePutUser("executor", "s3krit".toCharArray(), hasher, "superuser").get();
        final String token = basicAuthHeaderValue("executor", new SecureString("s3krit".toCharArray()));
        final Client client = client().filterWithHeader(MapBuilder.<String, String>newMapBuilder()
                .put("Authorization", token)
                .put("es-security-runas-user", "joe")
                .immutableMap());
        final CountDownLatch latch = new CountDownLatch(1);
        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final int numberOfThreads = scaledRandomIntBetween(numberOfProcessors, numberOfProcessors * 3);
        final int numberOfIterations = scaledRandomIntBetween(20, 100);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; i++) {
            threads.add(new Thread(() -> {
                try {
                    latch.await();
                    for (int j = 0; j < numberOfIterations; j++) {
                        ClusterHealthResponse response = client.admin().cluster().prepareHealth().get();
                        assertNoTimeout(response);
                    }
                } catch (InterruptedException e) {
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
