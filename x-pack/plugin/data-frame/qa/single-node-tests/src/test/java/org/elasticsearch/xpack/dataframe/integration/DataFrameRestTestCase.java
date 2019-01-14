/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public abstract class DataFrameRestTestCase extends ESRestTestCase {

    protected static final String DATAFRAME_ENDPOINT = DataFrameField.REST_BASE_PATH + "jobs/";

    /**
     * Create a simple dataset for testing with reviewers, ratings and businesses
     */
    protected void createReviewsIndex() throws IOException {
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
            long user = Math.round(Math.pow(i * 31 % 1000, distributionTable[i % distributionTable.length]) % 27);
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

    protected void createPivotReviewsJob(String jobId, String dataFrameIndex) throws IOException {
        final Request createDataframeJobRequest = new Request("PUT", DATAFRAME_ENDPOINT + jobId);
        createDataframeJobRequest.setJsonEntity("{"
                + " \"index_pattern\": \"reviews\","
                + " \"destination_index\": \"" + dataFrameIndex + "\","
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
        Map<String, Object> createDataframeJobResponse = entityAsMap(client().performRequest(createDataframeJobRequest));
        assertThat(createDataframeJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertTrue(indexExists(dataFrameIndex));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getDataFrameJobs() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all"));
        Map<String, Object> jobs = entityAsMap(response);
        List<Map<String, Object>> jobConfigs = (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        return jobConfigs == null ? Collections.emptyList() : jobConfigs;
    }

    protected static String getDataFrameIndexerState(String jobId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + jobId + "/_stats"));

        Map<?, ?> jobStatsAsMap = (Map<?, ?>) ((List<?>) entityAsMap(statsResponse).get("jobs")).get(0);
        return (String) XContentMapValues.extractValue("state.job_state", jobStatsAsMap);
    }

    @AfterClass
    public static void removeIndices() throws Exception {
        wipeDataFrameJobs();
        waitForPendingDataFrameTasks();
        // we might have disabled wiping indices, but now its time to get rid of them
        // note: can not use super.cleanUpCluster() as this method must be static
        wipeIndices();
    }

    protected static void wipeDataFrameJobs() throws IOException, InterruptedException {
        List<Map<String, Object>> jobConfigs = getDataFrameJobs();

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("POST", DATAFRAME_ENDPOINT + jobId + "/_stop");
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            request.addParameter("ignore", "404");
            adminClient().performRequest(request);
            assertEquals("stopped", getDataFrameIndexerState(jobId));
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("DELETE", DATAFRAME_ENDPOINT + jobId);
            request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
            adminClient().performRequest(request);
        }

        // jobs should be all gone
        jobConfigs = getDataFrameJobs();
        assertTrue(jobConfigs.isEmpty());

        // the configuration index should be empty
        Request request = new Request("GET", DataFrameInternalIndex.INDEX_NAME + "/_search");
        try {
            Response searchResponse = adminClient().performRequest(request);
            Map<String, Object> searchResult = entityAsMap(searchResponse);

            assertEquals(0, XContentMapValues.extractValue("hits.total.value", searchResult));
        } catch (ResponseException e) {
            // 404 here just means we had no data frame jobs, true for some tests
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    protected static void waitForPendingDataFrameTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(DataFrameField.TASK_NAME) == false);
    }

    protected static void wipeIndices() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }
}