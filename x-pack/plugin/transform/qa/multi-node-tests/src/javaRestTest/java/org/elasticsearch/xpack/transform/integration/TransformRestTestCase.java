/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.integration.common.TransformCommonRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.transform.TransformField.BASIC_STATS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public abstract class TransformRestTestCase extends TransformCommonRestTestCase {

    private final Set<String> createdTransformIds = new HashSet<>();

    protected void cleanUp() throws Exception {
        logAudits();
        cleanUpTransforms();
        waitForPendingTasks();
    }

    protected void cleanUpTransforms() throws IOException {
        for (String id : createdTransformIds) {
            try {
                stopTransform(id);
                deleteTransform(id);
            } catch (ResponseException ex) {
                if (ex.getResponse().getStatusLine().getStatusCode() == RestStatus.NOT_FOUND.getStatus()) {
                    logger.info("tried to cleanup already deleted transform [{}]", id);
                } else {
                    throw ex;
                }
            }
        }
        createdTransformIds.clear();
    }

    protected Map<String, Object> getIndexMapping(String index, RequestOptions options) throws IOException {
        var r = new Request("GET", "/" + index + "/_mapping");
        r.setOptions(options);
        return entityAsMap(client().performRequest(r));
    }

    protected void stopTransform(String id) throws IOException {
        stopTransform(id, true, null, false, false);
    }

    protected void stopTransform(
        String id,
        boolean waitForCompletion,
        @Nullable TimeValue timeout,
        boolean waitForCheckpoint,
        boolean force
    ) throws IOException {

        final Request stopTransformRequest = new Request("POST", TRANSFORM_ENDPOINT + id + "/_stop");
        stopTransformRequest.addParameter(TransformField.WAIT_FOR_COMPLETION.getPreferredName(), Boolean.toString(waitForCompletion));
        stopTransformRequest.addParameter(TransformField.WAIT_FOR_CHECKPOINT.getPreferredName(), Boolean.toString(waitForCheckpoint));
        if (timeout != null) {
            stopTransformRequest.addParameter(TransformField.TIMEOUT.getPreferredName(), timeout.getStringRep());
        }
        if (force) {
            stopTransformRequest.addParameter(TransformField.FORCE.getPreferredName(), "true");
        }
        assertAcknowledged(client().performRequest(stopTransformRequest));
    }

    protected void startTransform(String id, RequestOptions options) throws IOException {
        Request startTransformRequest = new Request("POST", TRANSFORM_ENDPOINT + id + "/_start");
        startTransformRequest.setOptions(options);
        assertAcknowledged(client().performRequest(startTransformRequest));
    }

    // workaround for https://github.com/elastic/elasticsearch/issues/62204
    protected void startTransformWithRetryOnConflict(String id, RequestOptions options) throws Exception {
        final int totalRetries = 10;
        long totalSleepTime = 0;
        ResponseException lastConflict = null;
        for (int retries = totalRetries; retries > 0; --retries) {
            try {
                startTransform(id, options);
                return;
            } catch (ResponseException e) {
                logger.warn(
                    "Failed to start transform [{}], remaining retries [{}], error: [{}], status: [{}]",
                    id,
                    retries,
                    e.getMessage(),
                    e.getResponse().getStatusLine().getStatusCode()
                );

                if ((RestStatus.CONFLICT.getStatus() == e.getResponse().getStatusLine().getStatusCode()) == false) {
                    throw e;
                }

                lastConflict = e;

                // wait between some ms max 5s, between a check,
                // with 10 retries the total retry should not be longer than 10s
                final long sleepTime = 5 * Math.round((Math.min(Math.pow(2, 1 + totalRetries - retries), 1000)));
                totalSleepTime += sleepTime;
                Thread.sleep(sleepTime);
            }
        }
        throw new AssertionError("startTransformWithRetryOnConflict timed out after " + totalSleepTime + "ms", lastConflict);
    }

    protected void deleteTransform(String id) throws IOException {
        deleteTransform(id, false);
    }

    protected void deleteTransform(String id, boolean force) throws IOException {
        Request request = new Request("DELETE", TRANSFORM_ENDPOINT + id);
        if (force) {
            request.addParameter(TransformField.FORCE.getPreferredName(), "true");
        }
        assertAcknowledged(adminClient().performRequest(request));
        createdTransformIds.remove(id);
    }

    protected void putTransform(String id, String config, RequestOptions options) throws IOException {
        putTransform(id, config, false, options);
    }

    protected void putTransform(String id, String config, boolean deferValidation, RequestOptions options) throws IOException {
        if (createdTransformIds.contains(id)) {
            throw new IllegalArgumentException("transform [" + id + "] is already registered");
        }

        Request request = new Request("PUT", TRANSFORM_ENDPOINT + id);
        request.setJsonEntity(config);
        if (deferValidation) {
            request.addParameter("defer_validation", "true");
        }
        request.setOptions(options);
        assertAcknowledged(client().performRequest(request));
        createdTransformIds.add(id);
    }

    protected Map<String, Object> previewTransform(String transformConfig, RequestOptions options) throws IOException {
        var request = new Request("POST", TRANSFORM_ENDPOINT + "_preview");
        request.setJsonEntity(transformConfig);
        request.setOptions(options);
        return entityAsMap(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getTransformStats(String id) throws IOException {
        var request = new Request("GET", TRANSFORM_ENDPOINT + id + "/_stats");
        request.setOptions(RequestOptions.DEFAULT);
        Response response = client().performRequest(request);
        List<Map<String, Object>> stats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", entityAsMap(response));
        assertThat(stats, hasSize(1));
        return stats.get(0);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getBasicTransformStats(String id) throws IOException {
        var request = new Request("GET", TRANSFORM_ENDPOINT + id + "/_stats");
        request.addParameter(BASIC_STATS.getPreferredName(), "true");
        request.setOptions(RequestOptions.DEFAULT);
        var stats = (List<Map<String, Object>>) XContentMapValues.extractValue("transforms", entityAsMap(client().performRequest(request)));
        assertThat(stats, hasSize(1));
        return stats.get(0);
    }

    protected String getTransformState(String id) throws IOException {
        return (String) getBasicTransformStats(id).get("state");
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getTransform(String id) throws IOException {
        var request = new Request("GET", TRANSFORM_ENDPOINT + id);
        var transformConfigs = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "transforms",
            entityAsMap(client().performRequest(request))
        );
        assertThat(transformConfigs, hasSize(1));
        return transformConfigs.get(0);
    }

    protected Map<String, Object> getTransforms(String id) throws IOException {
        Request request = new Request("GET", TRANSFORM_ENDPOINT + id);
        return entityAsMap(client().performRequest(request));
    }

    protected void waitUntilCheckpoint(String id, long checkpoint) throws Exception {
        waitUntilCheckpoint(id, checkpoint, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilCheckpoint(String id, long checkpoint, TimeValue waitTime) throws Exception {
        assertBusy(() -> assertEquals(checkpoint, getCheckpoint(id)), waitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    protected long getCheckpoint(String id) throws IOException {
        return getCheckpoint(getBasicTransformStats(id));
    }

    protected long getCheckpoint(Map<String, Object> stats) {
        return ((Integer) XContentMapValues.extractValue("checkpointing.last.checkpoint", stats)).longValue();
    }

    protected DateHistogramGroupSource createDateHistogramGroupSourceWithCalendarInterval(
        String field,
        DateHistogramInterval interval,
        ZoneId zone
    ) {
        return new DateHistogramGroupSource(field, null, false, new DateHistogramGroupSource.CalendarInterval(interval), zone, null);
    }

    /**
     * GroupConfig has 2 internal representations - source and a map
     * of SingleGroupSource, both need to be present.
     * The fromXContent parser populates both so the trick here is
     * to JSON serialise {@code groups} and build the
     * GroupConfig from JSON.
     *
     * @param groups Agg factory
     * @param xContentRegistry registry
     * @return GroupConfig
     * @throws IOException on parsing
     */
    public static GroupConfig createGroupConfig(Map<String, SingleGroupSource> groups, NamedXContentRegistry xContentRegistry)
        throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (Map.Entry<String, SingleGroupSource> entry : groups.entrySet()) {
                builder.startObject(entry.getKey());
                builder.field(entry.getValue().getType().value(), entry.getValue());
                builder.endObject();
            }
            builder.endObject();

            try (
                XContentParser sourceParser = XContentType.JSON.xContent()
                    .createParser(
                        XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
                            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                        BytesReference.bytes(builder).streamInput()
                    )
            ) {
                return GroupConfig.fromXContent(sourceParser, false);
            }
        }
    }

    protected GroupConfig createGroupConfig(Map<String, SingleGroupSource> groups) throws IOException {
        return createGroupConfig(groups, xContentRegistry());
    }

    /**
     * AggregationConfig has 2 internal representations - source and an
     * Aggregation Factory, both need to be present.
     * The fromXContent parser populates both so the trick here is
     * to JSON serialise {@code aggregations} and build the
     * AggregationConfig from JSON.
     *
     * @param aggregations Agg factory
     * @param xContentRegistry registry
     * @return AggregationConfig
     * @throws IOException on parsing
     */
    public static AggregationConfig createAggConfig(AggregatorFactories.Builder aggregations, NamedXContentRegistry xContentRegistry)
        throws IOException {

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            aggregations.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            try (
                XContentParser sourceParser = XContentType.JSON.xContent()
                    .createParser(
                        XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
                            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                        BytesReference.bytes(xContentBuilder).streamInput()
                    )
            ) {
                return AggregationConfig.fromXContent(sourceParser, false);
            }
        }
    }

    protected AggregationConfig createAggConfig(AggregatorFactories.Builder aggregations) throws IOException {
        return createAggConfig(aggregations, xContentRegistry());
    }

    protected PivotConfig createPivotConfig(Map<String, SingleGroupSource> groups, AggregatorFactories.Builder aggregations)
        throws Exception {
        return new PivotConfig(createGroupConfig(groups), createAggConfig(aggregations), null);
    }

    protected TransformConfig.Builder createTransformConfigBuilder(
        String id,
        String destinationIndex,
        QueryConfig queryConfig,
        String... sourceIndices
    ) {
        return TransformConfig.builder()
            .setId(id)
            .setSource(new SourceConfig(sourceIndices, queryConfig, Collections.emptyMap()))
            .setDest(new DestConfig(destinationIndex, null, null))
            .setFrequency(TimeValue.timeValueSeconds(10))
            .setDescription("Test transform config id: " + id);
    }

    protected void updateConfig(String id, String update, RequestOptions options) throws Exception {
        updateConfig(id, update, false, options);
    }

    protected void updateConfig(String id, String update, boolean deferValidation, RequestOptions options) throws Exception {
        Request updateRequest = new Request("POST", "_transform/" + id + "/_update");
        if (deferValidation) {
            updateRequest.addParameter("defer_validation", String.valueOf(deferValidation));
        }
        updateRequest.setJsonEntity(update);
        updateRequest.setOptions(options);
        assertOKAndConsume(client().performRequest(updateRequest));
    }

    protected void createReviewsIndex(
        String indexName,
        int numDocs,
        int numUsers,
        Function<Integer, Integer> userIdProvider,
        Function<Integer, String> dateStringProvider
    ) throws Exception {
        createReviewsIndex(indexName, numDocs, numUsers, userIdProvider, dateStringProvider, null);
    }

    protected void createReviewsIndex(
        String indexName,
        int numDocs,
        int numUsers,
        Function<Integer, Integer> userIdProvider,
        Function<Integer, String> dateStringProvider,
        String defaultPipeline
    ) throws Exception {
        assert numUsers > 0;

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
                    .startObject("count")
                    .field("type", "integer")
                    .endObject()
                    .startObject("business_id")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("stars")
                    .field("type", "integer")
                    .endObject()
                    .startObject("regular_object")
                    .field("type", "object")
                    .endObject()
                    .startObject("nested_object")
                    .field("type", "nested")
                    .endObject()
                    .startObject("comment")
                    .field("type", "text")
                    .startObject("fields")
                    .startObject("keyword")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                if (defaultPipeline != null) {
                    builder.startObject("settings").field("index.default_pipeline", defaultPipeline).endObject();
                }
            }
            builder.endObject();

            final StringEntity indexMappings = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT", indexName);
            req.setEntity(indexMappings);
            req.setOptions(RequestOptions.DEFAULT);
            assertOKAndConsume(adminClient().performRequest(req));
        }

        // create index
        StringBuilder sourceBuilder = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            Integer user = userIdProvider.apply(i);
            int stars = i % 5;
            long business = i % 50;
            String dateString = dateStringProvider.apply(i);

            sourceBuilder.append(Strings.format("""
                {"create":{"_index":"%s"}}
                """, indexName));

            sourceBuilder.append("{");
            if (user != null) {
                sourceBuilder.append("\"user_id\":\"").append("user_").append(user).append("\",");
            }
            sourceBuilder.append(Strings.format("""
                "count":%s,"business_id":"business_%s","stars":%s,"comment":"Great stuff, deserves %s stars","regular_object":\
                {"foo": 42},"nested_object":{"bar": 43},"timestamp":"%s"}
                """, i, business, stars, stars, dateString));

            if (i % 100 == 0) {
                sourceBuilder.append("\r\n");
                doBulk(sourceBuilder.toString(), false);
                sourceBuilder.setLength(0);
            }
        }
        sourceBuilder.append("\r\n");
        doBulk(sourceBuilder.toString(), true);
    }

    protected void doBulk(String bulkDocuments, boolean refresh) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        if (refresh) {
            bulkRequest.addParameter("refresh", "true");
        }
        bulkRequest.setJsonEntity(bulkDocuments);
        bulkRequest.setOptions(RequestOptions.DEFAULT);
        Response bulkResponse = adminClient().performRequest(bulkRequest);
        try {
            var bulkMap = entityAsMap(assertOK(bulkResponse));
            assertThat((boolean) bulkMap.get("errors"), is(equalTo(false)));
        } finally {
            EntityUtils.consumeQuietly(bulkResponse.getEntity());
        }
    }

    protected Map<String, Object> matchAllSearch(String index, int size, RequestOptions options) throws IOException {
        Request request = new Request("GET", index + "/_search");
        request.addParameter("size", Integer.toString(size));
        request.setOptions(options);
        Response response = client().performRequest(request);
        try {
            return entityAsMap(assertOK(response));
        } finally {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    private void waitForPendingTasks() {
        Request request = new Request(HttpGet.METHOD_NAME, "/_tasks");
        Map<String, String> parameters = Map.of(
            "wait_for_completion",
            Boolean.TRUE.toString(),
            "detailed",
            Boolean.TRUE.toString(),
            "timeout",
            TimeValue.timeValueSeconds(10).getStringRep()
        );
        request.addParameters(parameters);
        try {
            EntityUtils.consumeQuietly(adminClient().performRequest(request).getEntity());
        } catch (Exception e) {
            throw new AssertionError("Failed to wait for pending tasks to complete", e);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected Settings restClientSettings() {
        final String token = "Basic "
            + Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
