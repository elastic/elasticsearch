/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class PutRoleRestIT extends SecurityOnTrialLicenseRestTestCase {
    public void testPutManyValidRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""");
        assertThat(responseMap, not(hasKey("errors")));
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

        assertThat(responseMap, hasKey("errors"));

        List<String> created = (List<String>) responseMap.get("created");
        assertThat(created, hasSize(2));
        assertThat(created, contains("test1", "test3"));

        Map<String, Object> errors = (Map<String, Object>) responseMap.get("errors");
        Map<String, Object> failedItems = (Map<String, Object>) errors.get("details");
        assertEquals(failedItems.size(), 1);

        for (var entry : failedItems.entrySet()) {
            Map<String, Object> error = (Map<String, Object>) entry.getValue();
            assertThat((String) error.get("reason"), containsString("unknown cluster privilege [bad_privilege]"));
        }

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

        assertThat(responseMap, hasKey("errors"));
        Map<String, Object> errors = (Map<String, Object>) responseMap.get("errors");
        Map<String, Object> failedItems = (Map<String, Object>) errors.get("details");
        assertEquals(failedItems.size(), 3);

        for (var entry : failedItems.entrySet()) {
            Map<String, Object> error = (Map<String, Object>) entry.getValue();
            assertThat((String) error.get("reason"), containsString("unknown cluster privilege [bad_privilege]"));
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
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}], "description": "something"}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}], "remote_indices":[{"names":["logs-*"],
            "privileges":["read"],"clusters":["my_cluster*","other_cluster"]}]}}}""";
        {
            Map<String, Object> responseMap = upsertRoles(request);
            assertThat(responseMap, not(hasKey("errors")));

            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("created");
            assertEquals(3, items.size());

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
                    "something"
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
                    new RoleDescriptor.RemoteIndicesPrivileges[] {
                        RoleDescriptor.RemoteIndicesPrivileges.builder("my_cluster*", "other_cluster")
                            .indices("logs-*")
                            .privileges("read")
                            .build() },
                    null,
                    null,
                    null
                )
            );
        }
        {
            Map<String, Object> responseMap = upsertRoles(request);
            assertThat(responseMap, not(hasKey("errors")));

            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("noop");
            assertEquals(3, items.size());
        }
        {
            request = """
                {"roles": {"test1": {}, "test2":
                {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test3":
                {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}}}""";

            Map<String, Object> responseMap = upsertRoles(request);
            assertThat(responseMap, not(hasKey("errors")));
            List<Map<String, Object>> items = (List<Map<String, Object>>) responseMap.get("updated");
            assertEquals(3, items.size());

            assertThat(responseMap, not(hasKey("errors")));

            fetchRoleAndAssertEqualsExpected(
                "test1",
                new RoleDescriptor("test1", null, null, null, null, null, null, null, null, null, null, null)
            );
            fetchRoleAndAssertEqualsExpected(
                "test2",
                new RoleDescriptor(
                    "test2",
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
        }
    }

    public void testPutRoleWithInvalidManageRolesPrivilege() throws Exception {
        final String badRoleName = "bad-role";

        final ResponseException exception = expectThrows(ResponseException.class, () -> upsertRoles(String.format("""
            {
                "roles": {
                    "%s": {
                        "global": {
                            "role": {
                                "manage": {
                                    "indices": [
                                        {
                                            "names": ["allowed-index-prefix-*"],
                                            "privileges": ["foobar"]
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            }""", badRoleName)));

        assertThat(exception.getMessage(), containsString("unknown index privilege [foobar]"));
        assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
        assertRoleDoesNotExist(badRoleName);
    }

    private void assertRoleDoesNotExist(final String roleName) throws Exception {
        final ResponseException roleNotFound = expectThrows(
            ResponseException.class,
            () -> adminClient().performRequest(new Request("GET", "/_security/role/" + roleName))
        );
        assertEquals(404, roleNotFound.getResponse().getStatusLine().getStatusCode());
    }
}
