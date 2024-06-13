/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class BulkPutRoleRestIT extends SecurityOnTrialLicenseRestTestCase {
    public void testPutManyValidRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""");
        assertFalse((boolean) responseMap.get("errors"));
        fetchRoleAndAssertEqualsExpected(
            "test1",
            new RoleDescriptor(
                "test1",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build() },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        fetchRoleAndAssertEqualsExpected(
            "test2",
            new RoleDescriptor(
                "test2",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("read").build() },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        fetchRoleAndAssertEqualsExpected(
            "test3",
            new RoleDescriptor(
                "test3",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("write").build() },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testPutMixedValidInvalidRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["bad_privilege"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""");

        assertTrue((boolean) responseMap.get("errors"));

        Map<String, Object> test2Result = ((List<Map<String, Object>>) responseMap.get("items")).get(1);
        String reason = (String) ((Map<String, Object>) test2Result.get("error")).get("reason");
        assertThat(reason, containsString("unknown cluster privilege [bad_privilege]"));
        fetchRoleAndAssertEqualsExpected(
            "test1",
            new RoleDescriptor(
                "test1",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("all").build() },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );

        fetchRoleAndAssertEqualsExpected(
            "test3",
            new RoleDescriptor(
                "test3",
                new String[] { "all" },
                new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("write").build() },
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );

        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> adminClient().performRequest(new Request("GET", "/_security/role/test2"))
        );
        assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
    }

    @SuppressWarnings("unchecked")
    public void testPutNoValidRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["bad_privilege"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["bad_privilege"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["bad_privilege"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""");

        assertTrue((boolean) responseMap.get("errors"));

        for (Map<String, Object> result : ((List<Map<String, Object>>) responseMap.get("items"))) {
            String reason = (String) ((Map<String, Object>) result.get("error")).get("reason");
            assertThat(reason, containsString("unknown cluster privilege [bad_privilege]"));
        }

        for (String name : List.of("test1", "test2", "test3")) {
            final ResponseException e = expectThrows(
                ResponseException.class,
                () -> adminClient().performRequest(new Request("GET", "/_security/role/" + name))
            );
            assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        }
    }

    @SuppressWarnings("unchecked")
    public void testBulkUpdates() throws Exception {
        String request = """
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""";

        {
            Map<String, Object> responseMap = upsertRoles(request);
            assertFalse((boolean) responseMap.get("errors"));
            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("items");
            assertEquals(3, items.size());

            for (Map<String, Object> item : items) {
                assertEquals("created", item.get("result"));
                assertEquals(201, item.get("status"));
            }
        }
        {
            Map<String, Object> responseMap = upsertRoles(request);
            assertFalse((boolean) responseMap.get("errors"));
            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("items");
            assertEquals(3, items.size());

            for (Map<String, Object> item : items) {
                assertEquals("noop", item.get("result"));
                assertEquals(200, item.get("status"));
            }
        }
        {
            request = """
                {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test2":
                {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test3":
                {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}}}""";

            Map<String, Object> responseMap = upsertRoles(request);
            assertFalse((boolean) responseMap.get("errors"));
            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("items");
            assertEquals(3, items.size());

            for (Map<String, Object> item : items) {
                assertEquals("updated", item.get("result"));
                assertEquals(200, item.get("status"));
            }
        }
    }

    protected Map<String, Object> upsertRoles(String roleDescriptorsByName) throws IOException {
        Request request = rolesRequest(roleDescriptorsByName);
        Response response = adminClient().performRequest(request);
        assertOK(response);
        return responseAsMap(response);
    }

    protected Request rolesRequest(String roleDescriptorsByName) {
        Request rolesRequest;
        rolesRequest = new Request(HttpPost.METHOD_NAME, "/_security/_bulk/role");
        rolesRequest.setJsonEntity(org.elasticsearch.core.Strings.format(roleDescriptorsByName));
        return rolesRequest;
    }

}
