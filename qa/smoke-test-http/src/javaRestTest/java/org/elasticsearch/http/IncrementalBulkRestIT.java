/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 2, numClientNodes = 0)
public class IncrementalBulkRestIT extends HttpSmokeTestCase {

    @SuppressWarnings("unchecked")
    public void testIncrementalBulk() throws IOException {
        Request createRequest = new Request("PUT", "/index_name");
        createRequest.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1,
                  "write.wait_for_active_shards": 2
                }
              }
            }""");
        final Response indexCreatedResponse = getRestClient().performRequest(createRequest);
        assertThat(indexCreatedResponse.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        Request successfulIndexingRequest = new Request("POST", "/index_name/_bulk");

        // index documents for the rollup job
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            bulk.append("{\"index\":{\"_index\":\"index_name\"}}\n");
            bulk.append("{\"field\":").append(i).append("}\n");
        }
        bulk.append("\r\n");

        successfulIndexingRequest.setJsonEntity(bulk.toString());

        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            indexSuccessFul.getEntity().getContent(),
            true
        );
    }
}
