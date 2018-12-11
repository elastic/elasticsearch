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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.dataframe.DataFrame;
import org.junit.AfterClass;
import org.junit.Before;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class DataframePivotRestIT extends ESRestTestCase {

    private static final String DATAFRAME_ENDPOINT = DataFrame.BASE_PATH + "jobs/";
    private boolean indicesCreated = false;

    // preserve indices in order to reuse source indices in several test cases
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createReviewsIndex() throws IOException {

        // it's not possible to run it as @BeforeClass as clients aren't initialized then, so we need this little hack
        if (indicesCreated) {
            return;
        }

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
        indicesCreated = true;
    }

    @AfterClass
    public static void removeIndices() throws Exception {
        wipeDataFrameJobs();
        waitForPendingDataFrameTasks();
        // we disabled wiping indices, but now its time to get rid of them
        // note: can not use super.cleanUpCluster() as this method must be static
        wipeIndices();
    }

    public void testSimplePivot() throws Exception {
        String jobId = "simplePivot";
        String dataFrameIndex = "pivot_reviews";

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

        // start the job
        final Request startJobRequest = new Request("POST", DATAFRAME_ENDPOINT + jobId + "/_start");
        Map<String, Object> startJobResponse = entityAsMap(client().performRequest(startJobRequest));
        assertThat(startJobResponse.get("started"), equalTo(Boolean.TRUE));

        // wait unit the dataframe has been created and all data is available
        waitForDataFrameGeneration(jobId);
        refreshIndex(dataFrameIndex);

        // we expect 27 documents as there shall be 27 user_id's
        Map<String, Object> indexStats = getAsMap(dataFrameIndex + "/_stats");
        assertEquals(27, XContentMapValues.extractValue("_all.total.docs.count", indexStats));

        // get and check some users
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_5", 3.72);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_11", 3.846153846);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_20", 3.769230769);
        assertOnePivotValue(dataFrameIndex + "/_search?q=reviewer:user_26", 3.918918918);
    }

    private void waitForDataFrameGeneration(String jobId) throws Exception {
        assertBusy(() -> {
            long generation = getDataFrameGeneration(jobId);
            assertEquals(1, generation);
        }, 30, TimeUnit.SECONDS);
    }

    private int getDataFrameGeneration(String jobId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + jobId + "/_stats"));

        Map<?, ?> jobStatsAsMap = (Map<?, ?>) ((List<?>) entityAsMap(statsResponse).get("jobs")).get(0);
        return (int) XContentMapValues.extractValue("state.generation", jobStatsAsMap);
    }

    private void refreshIndex(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
    }

    private void assertOnePivotValue(String query, double expected) throws IOException {
        Map<String, Object> searchResult = getAsMap(query);

        assertEquals(1, XContentMapValues.extractValue("hits.total.value", searchResult));
        double actual = (double) ((List<?>) XContentMapValues.extractValue("hits.hits._source.avg_rating", searchResult)).get(0);
        assertEquals(expected, actual, 0.000001);
    }

    private static void wipeDataFrameJobs() throws IOException, InterruptedException {
        Response response = adminClient().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all"));
        Map<String, Object> jobs = entityAsMap(response);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> jobConfigs =
                (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobs);

        if (jobConfigs == null) {
            return;
        }

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("POST", DATAFRAME_ENDPOINT + jobId + "/_stop");
            request.addParameter("ignore", "404");
            adminClient().performRequest(request);
        }

        // TODO this is temporary until the StopDataFrameJob API gains the ability to block until stopped
        boolean stopped = awaitBusy(() -> {
            Request request = new Request("GET", DATAFRAME_ENDPOINT + "_all");
            try {
                Response jobsResponse = adminClient().performRequest(request);
                String body = EntityUtils.toString(jobsResponse.getEntity());
                // If the body contains any of the non-stopped states, at least one job is not finished yet
                return Arrays.stream(new String[]{"started", "aborting", "stopping", "indexing"}).noneMatch(body::contains);
            } catch (IOException e) {
                return false;
            }
        }, 10, TimeUnit.SECONDS);

        assertTrue("Timed out waiting for data frame job(s) to stop", stopped);

        for (Map<String, Object> jobConfig : jobConfigs) {
            String jobId = (String) jobConfig.get("id");
            Request request = new Request("DELETE", DATAFRAME_ENDPOINT + jobId);
            request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
            adminClient().performRequest(request);
        }
    }

    private static void waitForPendingDataFrameTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(DataFrame.TASK_NAME) == false);
    }

    private static void wipeIndices() throws IOException {
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
