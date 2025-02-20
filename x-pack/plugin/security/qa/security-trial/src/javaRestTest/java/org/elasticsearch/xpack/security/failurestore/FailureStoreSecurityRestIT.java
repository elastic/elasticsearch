/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.failurestore;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FailureStoreSecurityRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testGetUserPrivileges() throws IOException {
        Request userRequest = new Request("PUT", "/_security/user/user");
        userRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles": ["role"]
            }
            """);
        assertOK(adminClient().performRequest(userRequest));

        putRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """);
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [{
                  "names": ["*"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        putRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }
            """);
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        putRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"]
                }
              ]
            }
            """);
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");
    }

    private static void expectUserPrivilegesResponse(String userPrivilegesResponse) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("user", new SecureString("x-pack-test-password".toCharArray())))
        );
        Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response), equalTo(mapFromJson(userPrivilegesResponse)));
    }

    private static void putRole(String rolePayload) throws IOException {
        Request roleRequest = new Request("PUT", "/_security/role/role");
        roleRequest.setJsonEntity(rolePayload);
        assertOK(adminClient().performRequest(roleRequest));
    }

    private static Map<String, Object> mapFromJson(String json) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }
}
