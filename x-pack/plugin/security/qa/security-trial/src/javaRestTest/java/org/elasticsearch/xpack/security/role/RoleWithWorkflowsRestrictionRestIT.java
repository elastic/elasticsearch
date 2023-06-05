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
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class RoleWithWorkflowsRestrictionRestIT extends SecurityOnTrialLicenseRestTestCase {

    public void testCreateRoleWithWorkflowsRestrictionFail() {
        Request createRoleRequest = new Request(HttpPut.METHOD_NAME, "/_security/role/role_with_restriction");
        createRoleRequest.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"]
                }
              ],
              "restriction":{
                "workflows": ["foo", "bar"]
              }
            }""");

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(createRoleRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("failed to parse role [role_with_restriction]. unexpected field [restriction]"));
    }

    public void testUpdateRoleWithWorkflowsRestrictionFail() throws IOException {
        Request createRoleRequest = new Request(HttpPut.METHOD_NAME, "/_security/role/my_role");
        createRoleRequest.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"]
                }
              ]
            }""");
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);

        Request updateRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role/my_role");
        updateRoleRequest.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-*"],
                  "privileges": ["all"]
                }
              ],
              "restriction":{
                "workflows": ["foo", "bar"]
              }
            }""");

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(updateRoleRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("failed to parse role [my_role]. unexpected field [restriction]"));
    }
}
