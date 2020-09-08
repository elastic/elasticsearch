/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.eql;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public abstract class CommonEqlRestTestCase extends ESRestTestCase {

    private static final String defaultValidationIndexName = "eql_search_validation_test";
    private static final String validQuery = "process where user = 'SYSTEM'";

    private static final String[][] testBadRequests = {
            {null, "request body or source parameter is required"},
            {"{}", "query is null or empty"},
            {"{\"query\": \"\"}", "query is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"timestamp_field\": \"\"}", "timestamp field is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"event_category_field\": \"\"}", "event category field is null or empty"},
            {"{\"query\": \"" + validQuery + "\", \"size\": 0}", "size must be greater than 0"},
            {"{\"query\": \"" + validQuery + "\", \"size\": -1}", "size must be greater than 0"},
            {"{\"query\": \"" + validQuery + "\", \"filter\": null}", "filter doesn't support values of type: VALUE_NULL"},
            {"{\"query\": \"" + validQuery + "\", \"filter\": {}}", "query malformed, empty clause found"}
    };

    @BeforeClass
    public static void checkForSnapshot() {
        assumeTrue("Only works on snapshot builds for now", Build.CURRENT.isSnapshot());
    }

    public void testBadRequests() throws Exception {
        createIndex(defaultValidationIndexName, Settings.EMPTY);
        
        final String contentType = "application/json";
        for (String[] test : testBadRequests) {
            final String endpoint = "/" + defaultValidationIndexName + "/_eql/search";
            Request request = new Request("GET", endpoint);
            request.setJsonEntity(test[0]);

            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            Response response = e.getResponse();

            assertThat(response.getHeader("Content-Type"), containsString(contentType));
            assertThat(EntityUtils.toString(response.getEntity()), containsString(test[1]));
            assertThat(response.getStatusLine().getStatusCode(), is(400));
        }
        
        deleteIndex(defaultValidationIndexName);
    }

    @SuppressWarnings("unchecked")
    public void testIndexWildcardPatterns() throws Exception {
        createIndex("test1", Settings.EMPTY, null, "\"my_alias\" : {}, \"test_alias\" : {}");
        createIndex("test2", Settings.EMPTY, null, "\"my_alias\" : {}");

        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {\"_index\": \"test1\", \"_id\": 1}}\n");
        bulk.append("{\"event\":{\"category\":\"process\"},\"@timestamp\":\"2020-09-04T12:34:56Z\"}\n");
        bulk.append("{\"index\": {\"_index\": \"test2\", \"_id\": 2}}\n");
        bulk.append("{\"event\":{\"category\":\"process\"},\"@timestamp\":\"2020-09-05T12:34:56Z\"}\n");
        bulkIndex(bulk.toString());

        String[] wildcardRequests = {
            "test1,test2","test1*,test2","test1,test2*","test1*,test2*","test*","test1,test2,inexistent","my_alias","my_alias,test*",
            "test2,my_alias,test1","my_al*"
        };

        for (String indexPattern : wildcardRequests) {
            String endpoint = "/" + indexPattern + "/_eql/search";
            Request request = new Request("GET", endpoint);
            request.setJsonEntity("{\"query\":\"process where true\"}");
            Response response = client().performRequest(request);

            Map<String, Object> responseMap;
            try (InputStream content = response.getEntity().getContent()) {
                responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
            }
            Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
            List<Map<String, Object>> events = (List<Map<String, Object>>) hits.get("events");
            assertEquals(2, events.size());
            assertEquals("1", events.get(0).get("_id"));
            assertEquals("2", events.get(1).get("_id"));
        }

        deleteIndex("test1");
        deleteIndex("test2");
    }

    private void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");

        Response response = client().performRequest(bulkRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        String bulkResponse = EntityUtils.toString(response.getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": true")));
    }
}
