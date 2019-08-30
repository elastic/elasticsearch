/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public abstract class DataFrameRestTestCase extends ESRestTestCase {

    protected static final String TEST_PASSWORD = "x-pack-test-password";
    protected static final SecureString TEST_PASSWORD_SECURE_STRING = new SecureString(TEST_PASSWORD.toCharArray());
    private static final String BASIC_AUTH_VALUE_SUPER_USER = basicAuthHeaderValue("x_pack_rest_user", TEST_PASSWORD_SECURE_STRING);

    protected static final String REVIEWS_INDEX_NAME = "reviews";

    protected static final String DATAFRAME_ENDPOINT = DataFrameField.REST_BASE_PATH + "transforms/";

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    protected void createReviewsIndex(String indexName, int numDocs) throws IOException {
        int[] distributionTable = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 3, 3, 2, 1, 1, 1};

        // create mapping
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startObject("mappings")
                    .startObject("properties")
                    .startObject("timestamp")
                    .field("type", "date")
                    .endObject()
                    .startObject("user_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("business_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("stars")
                    .field("type", "integer")
                    .endObject()
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject();
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", indexName);
            req.setEntity(entity);
            client().performRequest(req);
        }

        // create index
        final StringBuilder bulk = new StringBuilder();
        int day = 10;
        int hour = 10;
        int min = 10;
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{\"_index\":\"" + indexName + "\"}}\n");
            long user = Math.round(Math.pow(i * 31 % 1000, distributionTable[i % distributionTable.length]) % 27);
            int stars = distributionTable[(i * 33) % distributionTable.length];
            long business = Math.round(Math.pow(user * stars, distributionTable[i % distributionTable.length]) % 13);
            if (i % 12 == 0) {
                hour = 10 + (i % 13);
            }
            if (i % 5 == 0) {
                min = 10 + (i % 49);
            }
            int sec = 10 + (i % 49);
            String location = (user + 10) + "," + (user + 15);

            String date_string = "2017-01-" + day + "T" + hour + ":" + min + ":" + sec + "Z";
            bulk.append("{\"user_id\":\"")
                .append("user_")
                .append(user)
                .append("\",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"location\":\"")
                .append(location)
                .append("\",\"timestamp\":\"")
                .append(date_string)
                .append("\"}\n");

            if (i % 50 == 0) {
                bulk.append("\r\n");
                final Request bulkRequest = new Request("POST", "/_bulk");
                bulkRequest.addParameter("refresh", "true");
                bulkRequest.setJsonEntity(bulk.toString());
                client().performRequest(bulkRequest);
                // clear the builder
                bulk.setLength(0);
                day += 1;
            }
        }
        bulk.append("\r\n");

        final Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }
    /**
     * Create a simple dataset for testing with reviewers, ratings and businesses
     */
    protected void createReviewsIndex() throws IOException {
        createReviewsIndex(REVIEWS_INDEX_NAME);
    }

    protected void createReviewsIndex(String indexName) throws IOException {
        createReviewsIndex(indexName, 1000);
    }

    protected void createPivotReviewsTransform(String transformId, String dataFrameIndex, String query) throws IOException {
        createPivotReviewsTransform(transformId, dataFrameIndex, query, null);
    }

    protected void createPivotReviewsTransform(String transformId, String dataFrameIndex, String query, String pipeline)
        throws IOException {
        createPivotReviewsTransform(transformId, dataFrameIndex, query, pipeline, null);
    }

    protected void createContinuousPivotReviewsTransform(String transformId, String dataFrameIndex, String authHeader) throws IOException {

        final Request createDataframeTransformRequest = createRequestWithAuth("PUT", DATAFRAME_ENDPOINT + transformId, authHeader);

        String config = "{ \"dest\": {\"index\":\"" + dataFrameIndex + "\"},"
            + " \"source\": {\"index\":\"" + REVIEWS_INDEX_NAME + "\"},"
            //Set frequency high for testing
            + " \"sync\": {\"time\":{\"field\": \"timestamp\", \"delay\": \"15m\"}},"
            + " \"frequency\": \"1s\","
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";

        createDataframeTransformRequest.setJsonEntity(config);

        Map<String, Object> createDataframeTransformResponse = entityAsMap(client().performRequest(createDataframeTransformRequest));
        assertThat(createDataframeTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }


    protected void createPivotReviewsTransform(String transformId, String dataFrameIndex, String query, String pipeline, String authHeader)
        throws IOException {
        final Request createDataframeTransformRequest = createRequestWithAuth("PUT", DATAFRAME_ENDPOINT + transformId, authHeader);

        String config = "{";

        if (pipeline != null) {
            config += " \"dest\": {\"index\":\"" + dataFrameIndex + "\", \"pipeline\":\"" + pipeline + "\"},";
        } else {
            config += " \"dest\": {\"index\":\"" + dataFrameIndex + "\"},";
        }

        if (query != null) {
            config += " \"source\": {\"index\":\"" + REVIEWS_INDEX_NAME + "\", \"query\":{" + query + "}},";
        } else {
            config += " \"source\": {\"index\":\"" + REVIEWS_INDEX_NAME + "\"},";
        }

        config += " \"pivot\": {"
                + "   \"group_by\": {"
                + "     \"reviewer\": {"
                + "       \"terms\": {"
                + "         \"field\": \"user_id\""
                + " } } },"
                + "   \"aggregations\": {"
                + "     \"avg_rating\": {"
                + "       \"avg\": {"
                + "         \"field\": \"stars\""
                + " } } } },"
                + "\"frequency\":\"1s\""
                + "}";

        createDataframeTransformRequest.setJsonEntity(config);

        Map<String, Object> createDataframeTransformResponse = entityAsMap(client().performRequest(createDataframeTransformRequest));
        assertThat(createDataframeTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }

    protected void startDataframeTransform(String transformId, boolean force) throws IOException {
        startDataframeTransform(transformId, force, null);
    }

    protected void startDataframeTransform(String transformId, boolean force, String authHeader, String... warnings) throws IOException {
        // start the transform
        final Request startTransformRequest = createRequestWithAuth("POST", DATAFRAME_ENDPOINT + transformId + "/_start", authHeader);
        startTransformRequest.addParameter(DataFrameField.FORCE.getPreferredName(), Boolean.toString(force));
        if (warnings.length > 0) {
            startTransformRequest.setOptions(expectWarnings(warnings));
        }
        Map<String, Object> startTransformResponse = entityAsMap(client().performRequest(startTransformRequest));
        assertThat(startTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }

    protected void stopDataFrameTransform(String transformId, boolean force) throws Exception {
        // start the transform
        final Request stopTransformRequest = createRequestWithAuth("POST", DATAFRAME_ENDPOINT + transformId + "/_stop", null);
        stopTransformRequest.addParameter(DataFrameField.FORCE.getPreferredName(), Boolean.toString(force));
        stopTransformRequest.addParameter(DataFrameField.WAIT_FOR_COMPLETION.getPreferredName(), Boolean.toString(true));
        Map<String, Object> stopTransformResponse = entityAsMap(client().performRequest(stopTransformRequest));
        assertThat(stopTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
    }

    protected void startAndWaitForTransform(String transformId, String dataFrameIndex) throws Exception {
        startAndWaitForTransform(transformId, dataFrameIndex, null);
    }

    protected void startAndWaitForTransform(String transformId, String dataFrameIndex, String authHeader) throws Exception {
        startAndWaitForTransform(transformId, dataFrameIndex, authHeader, new String[0]);
    }

    protected void startAndWaitForTransform(String transformId, String dataFrameIndex,
                                            String authHeader, String... warnings) throws Exception {
        // start the transform
        startDataframeTransform(transformId, false, authHeader, warnings);
        assertTrue(indexExists(dataFrameIndex));
        // wait until the dataframe has been created and all data is available
        waitForDataFrameCheckpoint(transformId);

        waitForDataFrameStopped(transformId);
        refreshIndex(dataFrameIndex);
    }

    protected void startAndWaitForContinuousTransform(String transformId,
                                                      String dataFrameIndex,
                                                      String authHeader) throws Exception {
        startAndWaitForContinuousTransform(transformId, dataFrameIndex, authHeader, 1L);
    }

    protected void startAndWaitForContinuousTransform(String transformId,
                                                      String dataFrameIndex,
                                                      String authHeader,
                                                      long checkpoint) throws Exception {
        // start the transform
        startDataframeTransform(transformId, false, authHeader, new String[0]);
        assertTrue(indexExists(dataFrameIndex));
        // wait until the dataframe has been created and all data is available
        waitForDataFrameCheckpoint(transformId, checkpoint);
        refreshIndex(dataFrameIndex);
    }

    protected Request createRequestWithAuth(final String method, final String endpoint, final String authHeader) {
        final Request request = new Request(method, endpoint);

        if (authHeader != null) {
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", authHeader);
            request.setOptions(options);
        }

        return request;
    }

    void waitForDataFrameStopped(String transformId) throws Exception {
        assertBusy(() -> {
            assertEquals("stopped", getDataFrameTransformState(transformId));
        }, 15, TimeUnit.SECONDS);
    }

    void waitForDataFrameCheckpoint(String transformId) throws Exception {
        waitForDataFrameCheckpoint(transformId, 1L);
    }

    void waitForDataFrameCheckpoint(String transformId, long checkpoint) throws Exception {
        assertBusy(() -> assertEquals(checkpoint, getDataFrameCheckpoint(transformId)), 30, TimeUnit.SECONDS);
    }

    void refreshIndex(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getDataFrameTransforms() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", DATAFRAME_ENDPOINT + "_all"));
        Map<String, Object> transforms = entityAsMap(response);
        List<Map<String, Object>> transformConfigs = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", transforms);

        return transformConfigs == null ? Collections.emptyList() : transformConfigs;
    }

    protected static String getDataFrameTransformState(String transformId) throws IOException {
        Map<?, ?> transformStatsAsMap = getDataFrameState(transformId);
        return transformStatsAsMap == null ? null : (String) XContentMapValues.extractValue("state", transformStatsAsMap);
    }

    protected static Map<?, ?> getDataFrameState(String transformId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + transformId + "/_stats"));
        List<?> transforms = ((List<?>) entityAsMap(statsResponse).get("transforms"));
        if (transforms.isEmpty()) {
            return null;
        }
        return (Map<?, ?>) transforms.get(0);
    }

    protected static void deleteDataFrameTransform(String transformId) throws IOException {
        Request request = new Request("DELETE", DATAFRAME_ENDPOINT + transformId);
        request.addParameter("ignore", "404"); // Ignore 404s because they imply someone was racing us to delete this
        adminClient().performRequest(request);
    }

    @After
    public void waitForDataFrame() throws Exception {
        wipeDataFrameTransforms();
        waitForPendingDataFrameTasks();
    }

    @AfterClass
    public static void removeIndices() throws Exception {
        // we might have disabled wiping indices, but now its time to get rid of them
        // note: can not use super.cleanUpCluster() as this method must be static
        wipeAllIndices();
    }

    public void wipeDataFrameTransforms() throws IOException {
        List<Map<String, Object>> transformConfigs = getDataFrameTransforms();
        for (Map<String, Object> transformConfig : transformConfigs) {
            String transformId = (String) transformConfig.get("id");
            Request request = new Request("POST", DATAFRAME_ENDPOINT + transformId + "/_stop");
            request.addParameter("wait_for_completion", "true");
            request.addParameter("timeout", "10s");
            request.addParameter("ignore", "404");
            adminClient().performRequest(request);
        }

        for (Map<String, Object> transformConfig : transformConfigs) {
            String transformId = (String) transformConfig.get("id");
            String state = getDataFrameTransformState(transformId);
            assertEquals("Transform [" + transformId + "] is not in the stopped state", "stopped", state);
        }

        for (Map<String, Object> transformConfig : transformConfigs) {
            String transformId = (String) transformConfig.get("id");
            deleteDataFrameTransform(transformId);
        }

        // transforms should be all gone
        transformConfigs = getDataFrameTransforms();
        assertTrue(transformConfigs.isEmpty());

        // the configuration index should be empty
        Request request = new Request("GET", DataFrameInternalIndex.LATEST_INDEX_NAME + "/_search");
        try {
            Response searchResponse = adminClient().performRequest(request);
            Map<String, Object> searchResult = entityAsMap(searchResponse);

            assertEquals(0, XContentMapValues.extractValue("hits.total.value", searchResult));
        } catch (ResponseException e) {
            // 404 here just means we had no data frame transforms, true for some tests
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    protected static void waitForPendingDataFrameTasks() throws Exception {
        waitForPendingTasks(adminClient(), taskName -> taskName.startsWith(DataFrameField.TASK_NAME) == false);
    }

    static int getDataFrameCheckpoint(String transformId) throws IOException {
        Response statsResponse = client().performRequest(new Request("GET", DATAFRAME_ENDPOINT + transformId + "/_stats"));

        Map<?, ?> transformStatsAsMap = (Map<?, ?>) ((List<?>) entityAsMap(statsResponse).get("transforms")).get(0);
        return (int) XContentMapValues.extractValue("checkpointing.last.checkpoint", transformStatsAsMap);
    }

    protected void setupDataAccessRole(String role, String... indices) throws IOException {
        String indicesStr = Arrays.stream(indices).collect(Collectors.joining("\",\"", "\"", "\""));
        Request request = new Request("PUT", "/_security/role/" + role);
        request.setJsonEntity("{"
            + "  \"indices\" : ["
            + "    { \"names\": [" + indicesStr + "], \"privileges\": [\"create_index\", \"read\", \"write\", \"view_index_metadata\"] }"
            + "  ]"
            + "}");
        client().performRequest(request);
    }

    protected void setupUser(String user, List<String> roles) throws IOException {
        String password = new String(TEST_PASSWORD_SECURE_STRING.getChars());

        String rolesStr = roles.stream().collect(Collectors.joining("\",\"", "\"", "\""));
        Request request = new Request("PUT", "/_security/user/" + user);
        request.setJsonEntity("{"
            + "  \"password\" : \"" + password + "\","
            + "  \"roles\" : [ " + rolesStr + " ]"
            + "}");
        client().performRequest(request);
    }
}
