/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;

public class ILMRestTestStateCleaner {

    public static void clearILMMetadata(RestClient adminClient) throws Exception {
        removePoliciesFromAllIndexes(adminClient);
        deleteAllPolicies(adminClient);
        // indices will be deleted by the ESRestTestCase class
    }

    private static void removePoliciesFromAllIndexes(RestClient adminClient) throws IOException {
        Response response = adminClient.performRequest(new Request("GET", "/_all"));
        Map<String, Object> indexes = ESRestTestCase.entityAsMap(response);

        if (indexes == null || indexes.isEmpty()) {
            return;
        }

        for (String indexName : indexes.keySet()) {
            try {
                adminClient.performRequest(new Request("DELETE", indexName + "/_ilm/"));
            } catch (Exception e) {
                // ok
            }
        }
    }

    private static void deleteAllPolicies(RestClient adminClient) throws Exception {
        Map<String, Object> policies;

        try {
            Response response = adminClient.performRequest(new Request("GET", "/_ilm"));
            policies = ESRestTestCase.entityAsMap(response);
        } catch (Exception e) {
            return;
        }

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            try {
                adminClient.performRequest(new Request("DELETE", "/_ilm/" + policyName));
            } catch (Exception e) {
                // ok
            }
        }
    }
}
