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

    public static void clearRollupMetadata(RestClient adminClient) throws Exception {
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
                response = adminClient.performRequest(new Request("DELETE", indexName + "/_ilm/"));
            } catch (Exception e) {
                e.printStackTrace();
                // ok
            }
        }
    }

    private static void deleteAllPolicies(RestClient adminClient) throws Exception {
        Response response = adminClient.performRequest(new Request("GET", "/_ilm"));
        Map<String, Object> policies = ESRestTestCase.entityAsMap(response);

        if (policies == null || policies.isEmpty()) {
            return;
        }

        for (String policyName : policies.keySet()) {
            try {
                response = adminClient.performRequest(new Request("DELETE", "/_ilm/" + policyName));
            } catch (Exception e) {
                e.printStackTrace();
                // ok
            }
        }
    }
}
