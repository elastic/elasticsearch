/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class GetRolesIT extends SecurityInBasicRestTestCase {

    private static final String ADMIN_USER = "admin_user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("admin-password".toCharArray());
    protected static final String READ_SECURITY_USER = "read_security_user";
    private static final SecureString READ_SECURITY_PASSWORD = new SecureString("read-security-password".toCharArray());

    @Before
    public void initialize() {
        new ReservedRolesStore();
    }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(READ_SECURITY_USER, READ_SECURITY_PASSWORD.toString(), "read_security_user_role", false)
        .build();

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(READ_SECURITY_USER, READ_SECURITY_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testGetAllRolesNoNative() throws Exception {
        // Test get roles API with operator admin_user
        getAllRolesAndAssert(adminClient(), ReservedRolesStore.names());
        // Test get roles API with read_security_user
        getAllRolesAndAssert(client(), ReservedRolesStore.names());
    }

    public void testGetAllRolesWithNative() throws Exception {
        createRole("custom_role", "Test custom native role.", Map.of("owner", "test"));

        Set<String> expectedRoles = new HashSet<>(ReservedRolesStore.names());
        expectedRoles.add("custom_role");

        // Test get roles API with operator admin_user
        getAllRolesAndAssert(adminClient(), expectedRoles);
        // Test get roles API with read_security_user
        getAllRolesAndAssert(client(), expectedRoles);
    }

    public void testGetReservedOnly() throws Exception {
        createRole("custom_role", "Test custom native role.", Map.of("owner", "test"));

        Set<String> rolesToGet = new HashSet<>();
        rolesToGet.add("custom_role");
        rolesToGet.addAll(randomSet(1, 5, () -> randomFrom(ReservedRolesStore.names())));

        getRolesAndAssert(adminClient(), rolesToGet);
        getRolesAndAssert(client(), rolesToGet);
    }

    public void testGetNativeOnly() throws Exception {
        createRole("custom_role1", "Test custom native role.", Map.of("owner", "test1"));
        createRole("custom_role2", "Test custom native role.", Map.of("owner", "test2"));

        Set<String> rolesToGet = Set.of("custom_role1", "custom_role2");

        getRolesAndAssert(adminClient(), rolesToGet);
        getRolesAndAssert(client(), rolesToGet);
    }

    public void testGetMixedRoles() throws Exception {
        createRole("custom_role", "Test custom native role.", Map.of("owner", "test"));

        Set<String> rolesToGet = new HashSet<>();
        rolesToGet.add("custom_role");
        rolesToGet.addAll(randomSet(1, 5, () -> randomFrom(ReservedRolesStore.names())));

        getRolesAndAssert(adminClient(), rolesToGet);
        getRolesAndAssert(client(), rolesToGet);
    }

    public void testNonExistentRole() {
        var e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", "/_security/role/non_existent_role"))
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    private void createRole(String roleName, String description, Map<String, Object> metadata) throws IOException {
        Request request = new Request("POST", "/_security/role/" + roleName);
        Map<String, Object> requestMap = new HashMap<>();
        if (description != null) {
            requestMap.put(RoleDescriptor.Fields.DESCRIPTION.getPreferredName(), description);
        }
        if (metadata != null) {
            requestMap.put(RoleDescriptor.Fields.METADATA.getPreferredName(), metadata);
        }
        BytesReference source = BytesReference.bytes(jsonBuilder().map(requestMap));
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertTrue(ObjectPath.eval("role.created", responseMap));
    }

    private void getAllRolesAndAssert(RestClient client, Set<String> expectedRoles) throws IOException {
        final Response response = client.performRequest(new Request("GET", "/_security/role"));
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.keySet(), equalTo(expectedRoles));
    }

    private void getRolesAndAssert(RestClient client, Set<String> rolesToGet) throws IOException {
        final Response response = client.performRequest(new Request("GET", "/_security/role/" + String.join(",", rolesToGet)));
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.keySet(), equalTo(rolesToGet));
    }
}
