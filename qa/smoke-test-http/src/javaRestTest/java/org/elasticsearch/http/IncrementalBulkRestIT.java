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
    public void testIndexingPressureStats() throws IOException {
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

        successfulIndexingRequest.setJsonEntity("""
            { "index" : { "_index" : "index_name" } }
            { "field" : "value1" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value2" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value3" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value4" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value5" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value6" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value7" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value8" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value9" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value10" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value11" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value12" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value13" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value14" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value15" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value16" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value17" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value18" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value19" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value20" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value21" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value22" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value23" }
            { "index" : { "_index" : "index_name" } }
            { "field" : "value24" }
            """);
        final Response indexSuccessFul = getRestClient().performRequest(successfulIndexingRequest);
        assertThat(indexSuccessFul.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));
        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            indexSuccessFul.getEntity().getContent(),
            true
        );
    }
}
