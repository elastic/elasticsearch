/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.XContentTestUtils;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.test.XContentTestUtils.createJsonMapView;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SecuritySettingsIT extends SecurityInBasicRestTestCase {

    @Before
    public void setupClient() throws IOException {
        // Poke the Main index and Profiles index, but leave the Tokens index un-created
        var userReq = new Request("PUT", "/_security/user/jacknich");
        userReq.setJsonEntity("""
            {
                "password" : "l0ng-r4nd0m-p@ssw0rd",
                "roles" : [ "admin", "other_role1" ],
                "full_name" : "Jack Nicholson",
                "email" : "jacknich@example.com"
            }
            """);
        assertOK(adminClient().performRequest(userReq));
        var profileReq = new Request("POST", "_security/profile/_activate");
        profileReq.setJsonEntity("""
            {
                "grant_type": "password",
                "username": "jacknich",
                "password" : "l0ng-r4nd0m-p@ssw0rd"
            }
            """);
        assertOK(adminClient().performRequest(profileReq));
    }

    /**
     * This test only checks the main and profiles indices. Making sure the tokens index is not trivial;
     * this test should be updated to do so (and also un-mute the docs test for this API).
     * @throws IOException
     */
    public void testBasicWorkflow() throws IOException {
        Request req = new Request("PUT", "/_security/settings");
        req.setJsonEntity("""
            {
                "security": {
                    "index.auto_expand_replicas": "0-all"
                },
                "security-profile": {
                        "index.auto_expand_replicas": "0-all"
                }
            }
            """);
        Response resp = adminClient().performRequest(req);
        assertOK(resp);
        Request getRequest = new Request("GET", "/_security/settings");
        Response getResp = adminClient().performRequest(getRequest);
        assertOK(getResp);
        final XContentTestUtils.JsonMapView mapView = createJsonMapView(getResp.getEntity().getContent());
        assertThat(mapView.get("security.index.auto_expand_replicas"), equalTo("0-all"));
    }

    public void testNoUpdatesThrowsException() throws IOException {
        Request req = new Request("PUT", "/_security/settings");
        req.setJsonEntity("{}");
        ResponseException ex = expectThrows(ResponseException.class, () -> adminClient().performRequest(req));
        assertThat(EntityUtils.toString(ex.getResponse().getEntity()), containsString("No settings given to update"));
    }

    public void testDisallowedSettingThrowsException() throws IOException {
        Request req = new Request("PUT", "/_security/settings");
        req.setJsonEntity("{\"security\": {\"index.max_ngram_diff\": 0}}"); // Disallowed setting chosen arbitrarily
        ResponseException ex = expectThrows(ResponseException.class, () -> adminClient().performRequest(req));
        assertThat(
            EntityUtils.toString(ex.getResponse().getEntity()),
            containsString("illegal settings for index [security]: " + "[index.max_ngram_diff], these settings may not be configured.")
        );
    }

    public void testIndexDoesntExistThrowsException() throws IOException {
        Request req = new Request("PUT", "/_security/settings");
        req.setJsonEntity("{\"security-tokens\": {\"index.auto_expand_replicas\": \"0-all\"}}");
        ResponseException ex = expectThrows(ResponseException.class, () -> adminClient().performRequest(req));
        assertThat(
            EntityUtils.toString(ex.getResponse().getEntity()),
            containsString("the [.security-tokens] index is not in use on this system yet")
        );
    }
}
