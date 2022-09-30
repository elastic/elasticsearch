/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class RoleWithRemoteIndexPrivilegesRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    @Before
    public void setup() throws IOException {
        createUser(REMOTE_SEARCH_USER, PASSWORD, List.of(REMOTE_SEARCH_ROLE));
        createIndex(adminClient(), "index-a", null, null, null);
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(REMOTE_SEARCH_USER);
        deleteIndex(adminClient(), "index-a");
    }

    public void testIndexPrivilegesWithRemoteClusters() throws IOException {
        var putRoleRequest = new Request("PUT", "_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["all"],
                  "remote_clusters": ["remote-a", "*"]
                }
              ]
            }""");
        final Response putRoleResponse1 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse1);

        // Remote privilege does not authorize local search
        var searchRequest = new Request("GET", "/index-a/_search");
        searchRequest.setOptions(
            searchRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
        );
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));
        assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("action [" + SearchAction.NAME + "] is unauthorized for user"));

        // Add local privilege and check local search authorized
        putRoleRequest = new Request("PUT", "_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-a", "*"],
                  "privileges": ["all"],
                  "remote_clusters": ["remote-a", "*"]
                },
                {
                  "names": ["index-a", "*"],
                  "privileges": ["read"]
                }
              ]
            }""");
        final Response putRoleResponse2 = adminClient().performRequest(putRoleRequest);
        assertOK(putRoleResponse2);
        final Response searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
    }

}
