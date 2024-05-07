/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RoleWithDescriptionRestIT extends SecurityOnTrialLicenseRestTestCase {

    public void testCreateOrUpdateRoleWithDescription() throws Exception {
        final String roleName = "role_with_description";
        final String initialRoleDescription = randomAlphaOfLengthBetween(0, 10);
        {
            Request createRoleRequest = new Request(HttpPut.METHOD_NAME, "/_security/role/" + roleName);
            createRoleRequest.setJsonEntity(Strings.format("""
                {
                  "description": "%s",
                  "cluster": ["all"],
                  "indices": [{"names": ["*"], "privileges": ["all"]}]
                }""", initialRoleDescription));
            Response createResponse = adminClient().performRequest(createRoleRequest);
            assertOK(createResponse);
            fetchRoleAndAssertEqualsExpected(
                roleName,
                new RoleDescriptor(
                    roleName,
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
                    initialRoleDescription
                )
            );
        }
        {
            final String newRoleDescription = randomValueOtherThan(initialRoleDescription, () -> randomAlphaOfLengthBetween(0, 10));
            Request updateRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role/" + roleName);
            updateRoleRequest.setJsonEntity(Strings.format("""
                {
                  "description": "%s",
                  "cluster": ["all"],
                  "indices": [{"names": ["index-*"], "privileges": ["all"]}]
                }""", newRoleDescription));
            Response updateResponse = adminClient().performRequest(updateRoleRequest);
            assertOK(updateResponse);

            fetchRoleAndAssertEqualsExpected(
                roleName,
                new RoleDescriptor(
                    roleName,
                    new String[] { "all" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("index-*").privileges("all").build() },
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    newRoleDescription
                )
            );
        }
    }

    public void testCreateRoleWithInvalidDescriptionFails() {
        Request createRoleRequest = new Request(HttpPut.METHOD_NAME, "/_security/role/role_with_large_description");
        createRoleRequest.setJsonEntity(Strings.format("""
            {
              "description": "%s",
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["all"]}]
            }""", randomAlphaOfLength(Validation.Roles.MAX_DESCRIPTION_LENGTH + randomIntBetween(1, 5))));

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(createRoleRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(
            e.getMessage(),
            containsString("Role description must be less than " + Validation.Roles.MAX_DESCRIPTION_LENGTH + " characters.")
        );
    }

    public void testUpdateRoleWithInvalidDescriptionFails() throws IOException {
        Request createRoleRequest = new Request(HttpPut.METHOD_NAME, "/_security/role/my_role");
        createRoleRequest.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["all"]}]
            }""");
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);

        Request updateRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role/my_role");
        updateRoleRequest.setJsonEntity(Strings.format("""
            {
              "description": "%s",
              "cluster": ["all"],
              "indices": [{"names": ["index-*"], "privileges": ["all"]}]
            }""", randomAlphaOfLength(Validation.Roles.MAX_DESCRIPTION_LENGTH + randomIntBetween(1, 5))));

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(updateRoleRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(
            e.getMessage(),
            containsString("Role description must be less than " + Validation.Roles.MAX_DESCRIPTION_LENGTH + " characters.")
        );
    }

    private void fetchRoleAndAssertEqualsExpected(final String roleName, final RoleDescriptor expectedRoleDescriptor) throws IOException {
        final Response getRoleResponse = adminClient().performRequest(new Request("GET", "/_security/role/" + roleName));
        assertOK(getRoleResponse);
        final Map<String, RoleDescriptor> actual = responseAsParser(getRoleResponse).map(
            HashMap::new,
            p -> RoleDescriptor.parserBuilder().allowDescription(true).build().parse(expectedRoleDescriptor.getName(), p)
        );
        assertThat(actual, equalTo(Map.of(expectedRoleDescriptor.getName(), expectedRoleDescriptor)));
    }
}
