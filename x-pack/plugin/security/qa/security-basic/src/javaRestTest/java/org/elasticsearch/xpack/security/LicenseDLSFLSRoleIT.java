/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.security.QueryRoleIT.assertQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;

/**
 * This class tests that roles with DLS and FLS are disabled when queried when the license doesn't allow such features.
 */
public final class LicenseDLSFLSRoleIT extends ESRestTestCase {

    protected static final String REST_USER = "security_test_user";
    private static final SecureString REST_PASSWORD = new SecureString("security-test-password".toCharArray());
    private static final String ADMIN_USER = "admin_user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("admin-password".toCharArray());
    protected static final String READ_SECURITY_USER = "read_security_user";
    private static final SecureString READ_SECURITY_PASSWORD = new SecureString("read-security-password".toCharArray());

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(REST_USER, REST_PASSWORD.toString(), "security_test_role", false)
        .user(READ_SECURITY_USER, READ_SECURITY_PASSWORD.toString(), "read_security_user_role", false)
        .build();

    @Before
    public void setupLicense() throws IOException {
        // start with trial license
        Request request = new Request("POST", "/_license/start_trial?acknowledge=true");
        Response response = adminClient().performRequest(request);
        assertOK(response);
        assertTrue((boolean) responseAsMap(response).get("trial_was_started"));
    }

    @After
    public void removeLicense() throws IOException {
        // start with trial license
        Request request = new Request("DELETE", "/_license");
        Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(REST_USER, REST_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testQueryDLSFLSRolesShowAsDisabled() throws Exception {
        // neither DLS nor FLS role
        {
            RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("no-dls-nor-fls*").privileges("read").build() };
            createRoleWithIndicesPrivileges(adminClient(), "role_with_neither", indicesPrivileges);
        }
        // role with DLS
        {
            RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("read").query("{\"match_all\":{}}").build() };
            createRoleWithIndicesPrivileges(adminClient(), "role_with_DLS", indicesPrivileges);
        }
        // role with FLS
        {
            RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("read")
                    .grantedFields("granted_field1", "granted*")
                    .build() };
            createRoleWithIndicesPrivileges(adminClient(), "role_with_FLS", indicesPrivileges);
        }
        // role with DLS and FLS
        {
            RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("read")
                    .grantedFields("granted_field1", "granted*")
                    .build(),
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices("*")
                    .privileges("read")
                    .query("{\"match\": {\"category\": \"click\"}}")
                    .build() };
            createRoleWithIndicesPrivileges(adminClient(), "role_with_FLS_and_DLS", indicesPrivileges);
        }
        assertQuery(client(), """
            {"query":{"bool":{"must_not":{"term":{"metadata._reserved":true}}}}}""", 4, roles -> {
            roles.sort(Comparator.comparing(o -> ((String) o.get("name"))));
            assertThat(roles, iterableWithSize(4));
            assertThat(roles.get(0).get("name"), equalTo("role_with_DLS"));
            assertRoleEnabled(roles.get(0), true);
            assertThat(roles.get(1).get("name"), equalTo("role_with_FLS"));
            assertRoleEnabled(roles.get(1), true);
            assertThat(roles.get(2).get("name"), equalTo("role_with_FLS_and_DLS"));
            assertRoleEnabled(roles.get(2), true);
            assertThat(roles.get(3).get("name"), equalTo("role_with_neither"));
            assertRoleEnabled(roles.get(3), true);
        });
        // start "basic" license
        Request request = new Request("POST", "/_license/start_basic?acknowledge=true");
        Response response = adminClient().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertTrue(((Boolean) responseMap.get("basic_was_started")));
        assertTrue(((Boolean) responseMap.get("acknowledged")));
        // now the same roles show up as disabled ("enabled" is "false")
        assertQuery(client(), """
            {"query":{"bool":{"must_not":{"term":{"metadata._reserved":true}}}}}""", 4, roles -> {
            roles.sort(Comparator.comparing(o -> ((String) o.get("name"))));
            assertThat(roles, iterableWithSize(4));
            assertThat(roles.get(0).get("name"), equalTo("role_with_DLS"));
            assertRoleEnabled(roles.get(0), false);
            assertThat(roles.get(1).get("name"), equalTo("role_with_FLS"));
            assertRoleEnabled(roles.get(1), false);
            assertThat(roles.get(2).get("name"), equalTo("role_with_FLS_and_DLS"));
            assertRoleEnabled(roles.get(2), false);
            // role with neither DLS nor FLS is still enabled
            assertThat(roles.get(3).get("name"), equalTo("role_with_neither"));
            assertRoleEnabled(roles.get(3), true);
        });
    }

    @SuppressWarnings("unchecked")
    private void createRoleWithIndicesPrivileges(RestClient adminClient, String name, RoleDescriptor.IndicesPrivileges[] indicesPrivileges)
        throws IOException {
        Request request = new Request("POST", "/_security/role/" + name);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(RoleDescriptor.Fields.INDICES.getPreferredName(), indicesPrivileges);
        BytesReference source = BytesReference.bytes(jsonBuilder().map(requestMap));
        request.setJsonEntity(source.utf8ToString());
        Response response = adminClient.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertTrue((Boolean) ((Map<String, Object>) responseMap.get("role")).get("created"));
    }

    @SuppressWarnings("unchecked")
    private static void assertRoleEnabled(Map<String, Object> roleMap, boolean enabled) {
        assertTrue(roleMap.containsKey("transient_metadata"));
        assertThat(roleMap.get("transient_metadata"), instanceOf(Map.class));
        assertThat(((Map<String, Object>) roleMap.get("transient_metadata")).get("enabled"), equalTo(enabled));
    }
}
