/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.role;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class RoleWithWorkflowsRestrictionRestIT extends SecurityOnTrialLicenseRestTestCase {

    public void testCreateRoleWithWorkflowsRestrictionFail() {
        Request request = roleRequest("""
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
            }""", "role_with_restriction");

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("failed to parse role [role_with_restriction]. unexpected field [restriction]"));
    }

    public void testUpdateRoleWithWorkflowsRestrictionFail() throws IOException {
        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["all"]
                }
              ]
            }""", "my_role");

        Request updateRoleRequest = roleRequest("""
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
            }""", "my_role");

        ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(updateRoleRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("failed to parse role [my_role]. unexpected field [restriction]"));
    }
}
