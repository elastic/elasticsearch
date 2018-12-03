/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

// broken right now, as index creation is done on the transport thread
//@AwaitsFix(bugUrl = "NA")
public class DataframePivotIT extends ESRestTestCase {

    private static final String DATAFRAME_ENDPOINT = "/_data_frame/jobs/";

    // TODO: this test incomplete due to the lack of a status api
    public void testSimplePivot() throws IOException {
        final Request createDataframeJobRequest = new Request("PUT", DATAFRAME_ENDPOINT + "simple");
        createDataframeJobRequest.setJsonEntity("{"
                + " \"index_pattern\": \"reviews\","
                + " \"destination_index\": \"pivot_reviews\","
                + " \"sources\": {"
                + "   \"sources\": [ {"
                + "     \"reviewer\": {"
                + "       \"terms\": {"
                + "         \"field\": \"user_id\""
                + " } } } ] },"
                + " \"aggregations\": {"
                + "   \"avg_rating\": {"
                + "     \"avg\": {"
                + "       \"field\": \"stars\""
                + " } } }"
                + "}");
        Map<String, Object> createDataframeJobResponse = toMap(client().performRequest(createDataframeJobRequest));
        assertThat(createDataframeJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        // start the job
        final Request startJobRequest = new Request("POST", DATAFRAME_ENDPOINT + "simple/_start");
        Map<String, Object> startJobResponse = toMap(client().performRequest(startJobRequest));
        assertThat(startJobResponse.get("started"), equalTo(Boolean.TRUE));

        // TODO: right now lacking a way to get status information for a job
        // 
        
        
        // Refresh the index to make sure all newly indexed docs are searchable
        final Request refreshIndex = new Request("POST", "pivot_reviews/_refresh");
        toMap(client().performRequest(refreshIndex));
    }

    @Before
    public void createReviewsIndex() throws IOException {
        int[] distributionTable = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 3, 3, 2, 1, 1, 1};

        final int numDocs = 1000;

        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("_doc")
                      .startObject("properties")
                        .startObject("user_id")
                          .field("type", "keyword")
                        .endObject()
                        .startObject("business_id")
                          .field("type", "keyword")
                        .endObject()
                        .startObject("stars")
                          .field("type", "integer")
                        .endObject()
                      .endObject()
                    .endObject()
                  .endObject();
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", "reviews");
            req.setEntity(entity);
            client().performRequest(req);
        }

        // create index
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{\"_index\":\"reviews\",\"_type\":\"_doc\"}}\n");
            long user = Math.round(Math.pow(i * 31, distributionTable[i % distributionTable.length]) % 27);
            int stars = distributionTable[(i * 33) % distributionTable.length];
            long business = Math.round(Math.pow(user * stars, distributionTable[i % distributionTable.length]) % 13);
            bulk.append("{\"user_id\":\"")
                .append("user_")
                .append(user)
                .append("\",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append("}\n");

            if (i % 50 == 0) {
                bulk.append("\r\n");
                final Request bulkRequest = new Request("POST", "/_bulk");
                bulkRequest.addParameter("refresh", "true");
                bulkRequest.setJsonEntity(bulk.toString());
                client().performRequest(bulkRequest);
                // clear the builder
                bulk.setLength(0);
            }
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }
}
