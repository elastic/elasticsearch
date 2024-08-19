/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.apache.http.client.methods.HttpDelete;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class BulkDeleteRoleRestIT extends SecurityOnTrialLicenseRestTestCase {
    @SuppressWarnings("unchecked")
    public void testDeleteValidExistingRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}, "test2":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["read"]}]}, "test3":
            {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["write"]}]}}}""");
        assertThat(responseMap, not(hasKey("errors")));

        List<String> rolesToDelete = List.of("test1", "test3");
        Map<String, Object> response = deleteRoles(rolesToDelete);
        List<String> deleted = (List<String>) response.get("deleted");
        assertThat(deleted, equalTo(rolesToDelete));

        assertRolesDeleted(rolesToDelete);
        assertRolesNotDeleted(List.of("test2"));
    }

    @SuppressWarnings("unchecked")
    public void testTryDeleteNonExistingRoles() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}}}""");
        assertThat(responseMap, not(hasKey("errors")));

        List<String> rolesToDelete = List.of("test1", "test2", "test3");

        Map<String, Object> response = deleteRoles(rolesToDelete);
        List<String> deleted = (List<String>) response.get("deleted");

        List<String> notFound = (List<String>) response.get("not_found");

        assertThat(deleted, equalTo(List.of("test1")));
        assertThat(notFound, equalTo(List.of("test2", "test3")));

        assertRolesDeleted(rolesToDelete);
    }

    @SuppressWarnings("unchecked")
    public void testTryDeleteReservedRoleName() throws Exception {
        Map<String, Object> responseMap = upsertRoles("""
            {"roles": {"test1": {"cluster": ["all"],"indices": [{"names": ["*"],"privileges": ["all"]}]}}}""");
        assertThat(responseMap, not(hasKey("errors")));

        Map<String, Object> response = deleteRoles(List.of("superuser", "test1"));

        List<String> deleted = (List<String>) response.get("deleted");
        assertThat(deleted, equalTo(List.of("test1")));

        Map<String, Object> errors = (Map<String, Object>) response.get("errors");
        assertThat((Integer) errors.get("count"), equalTo(1));
        Map<String, Object> errorDetails = (Map<String, Object>) ((Map<String, Object>) errors.get("details")).get("superuser");

        assertThat(
            errorDetails,
            equalTo(Map.of("type", "illegal_argument_exception", "reason", "role [superuser] is reserved and cannot be deleted"))
        );

        assertRolesDeleted(List.of("test1"));
        assertRolesNotDeleted(List.of("superuser"));
    }

    protected Map<String, Object> deleteRoles(List<String> roles) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "/_security/role");
        request.setJsonEntity(Strings.format("""
            {"names": [%s]}""", String.join(",", roles.stream().map(role -> "\"" + role + "\"").toList())));

        Response response = adminClient().performRequest(request);
        assertOK(response);
        return responseAsMap(response);
    }

    protected void assertRolesDeleted(List<String> roleNames) {
        for (String roleName : roleNames) {
            ResponseException exception = assertThrows(
                ResponseException.class,
                () -> adminClient().performRequest(new Request("GET", "/_security/role/" + roleName))
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    protected void assertRolesNotDeleted(List<String> roleNames) throws IOException {
        for (String roleName : roleNames) {
            Response response = adminClient().performRequest(new Request("GET", "/_security/role/" + roleName));
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        }
    }
}
