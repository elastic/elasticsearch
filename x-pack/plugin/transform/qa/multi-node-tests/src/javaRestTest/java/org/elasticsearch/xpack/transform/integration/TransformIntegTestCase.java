/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.transform.DeleteTransformRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.GetTransformStatsRequest;
import org.elasticsearch.client.transform.GetTransformStatsResponse;
import org.elasticsearch.client.transform.PreviewTransformRequest;
import org.elasticsearch.client.transform.PreviewTransformResponse;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.StartTransformRequest;
import org.elasticsearch.client.transform.StartTransformResponse;
import org.elasticsearch.client.transform.StopTransformRequest;
import org.elasticsearch.client.transform.StopTransformResponse;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.QueryConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.client.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.joda.time.Instant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

abstract class TransformIntegTestCase extends ESRestTestCase {

    private Map<String, TransformConfig> transformConfigs = new HashMap<>();

    protected void cleanUp() throws IOException {
        logAudits();
        cleanUpTransforms();
        waitForPendingTasks();
    }

    private void logAudits() throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {

            // using '*' to make this lenient and do not fail if the audit index does not exist
            SearchRequest searchRequest = new SearchRequest(".transform-notifications-*");
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(100).sort("timestamp", SortOrder.ASC));

            restClient.indices().refresh(new RefreshRequest(searchRequest.indices()), RequestOptions.DEFAULT);

            SearchResponse searchResponse = restClient.search(searchRequest, RequestOptions.DEFAULT);

            for (SearchHit hit : searchResponse.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                String level = (String) source.getOrDefault("level", "info");
                logger.log(
                    Level.getLevel(level.toUpperCase(Locale.ROOT)),
                    "Transform audit: [{}] [{}] [{}] [{}]",
                    Instant.ofEpochMilli((long) source.getOrDefault("timestamp", 0)),
                    source.getOrDefault("transform_id", "n/a"),
                    source.getOrDefault("message", "n/a"),
                    source.getOrDefault("node_name", "n/a")
                );
            }
        }
    }

    protected void cleanUpTransforms() throws IOException {
        for (TransformConfig config : transformConfigs.values()) {
            try {
                stopTransform(config.getId());
                deleteTransform(config.getId());
            } catch (ElasticsearchStatusException ex) {
                if (ex.status().equals(RestStatus.NOT_FOUND)) {
                    logger.info("tried to cleanup already deleted transform [{}]", config.getId());
                } else {
                    throw ex;
                }
            }
        }
        transformConfigs.clear();
    }

    protected StopTransformResponse stopTransform(String id) throws IOException {
        return stopTransform(id, true, null, false);
    }

    protected StopTransformResponse stopTransform(String id, boolean waitForCompletion, TimeValue timeout, boolean waitForCheckpoint)
        throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform()
                .stopTransform(new StopTransformRequest(id, waitForCompletion, timeout, waitForCheckpoint), RequestOptions.DEFAULT);
        }
    }

    protected StartTransformResponse startTransform(String id, RequestOptions options) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().startTransform(new StartTransformRequest(id), options);
        }
    }

    // workaround for https://github.com/elastic/elasticsearch/issues/62204
    protected StartTransformResponse startTransformWithRetryOnConflict(String id, RequestOptions options) throws Exception {
        final int totalRetries = 10;
        long totalSleepTime = 0;
        ElasticsearchStatusException lastConflict = null;
        for (int retries = totalRetries; retries > 0; --retries) {
            try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
                return restClient.transform().startTransform(new StartTransformRequest(id), options);
            } catch (ElasticsearchStatusException e) {
                logger.warn(
                    "Failed to start transform [{}], remaining retries [{}], error: [{}], status: [{}]",
                    id,
                    retries,
                    e.getDetailedMessage(),
                    e.status()
                );

                if (RestStatus.CONFLICT.equals(e.status()) == false) {
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

    protected AcknowledgedResponse deleteTransform(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            AcknowledgedResponse response = restClient.transform().deleteTransform(new DeleteTransformRequest(id), RequestOptions.DEFAULT);
            if (response.isAcknowledged()) {
                transformConfigs.remove(id);
            }
            return response;
        }
    }

    protected AcknowledgedResponse putTransform(TransformConfig config, RequestOptions options) throws IOException {
        if (transformConfigs.keySet().contains(config.getId())) {
            throw new IllegalArgumentException("transform [" + config.getId() + "] is already registered");
        }
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            AcknowledgedResponse response = restClient.transform().putTransform(new PutTransformRequest(config), options);

            if (response.isAcknowledged()) {
                transformConfigs.put(config.getId(), config);
            }
            return response;
        }
    }

    protected PreviewTransformResponse previewTransform(TransformConfig config, RequestOptions options) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().previewTransform(new PreviewTransformRequest(config), options);
        }
    }

    protected GetTransformStatsResponse getTransformStats(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().getTransformStats(new GetTransformStatsRequest(id), RequestOptions.DEFAULT);
        }
    }

    protected GetTransformResponse getTransform(String id) throws IOException {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            return restClient.transform().getTransform(new GetTransformRequest(id), RequestOptions.DEFAULT);
        }
    }

    protected void waitUntilCheckpoint(String id, long checkpoint) throws Exception {
        waitUntilCheckpoint(id, checkpoint, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilCheckpoint(String id, long checkpoint, TimeValue waitTime) throws Exception {
        assertBusy(
            () -> assertEquals(
                checkpoint,
                getTransformStats(id).getTransformsStats().get(0).getCheckpointingInfo().getLast().getCheckpoint()
            ),
            waitTime.getMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    protected DateHistogramGroupSource createDateHistogramGroupSourceWithFixedInterval(
        String field,
        DateHistogramInterval interval,
        ZoneId zone
    ) {
        DateHistogramGroupSource.Builder builder = DateHistogramGroupSource.builder()
            .setField(field)
            .setInterval(new DateHistogramGroupSource.FixedInterval(interval))
            .setTimeZone(zone);
        return builder.build();
    }

    protected DateHistogramGroupSource createDateHistogramGroupSourceWithCalendarInterval(
        String field,
        DateHistogramInterval interval,
        ZoneId zone
    ) {
        DateHistogramGroupSource.Builder builder = DateHistogramGroupSource.builder()
            .setField(field)
            .setInterval(new DateHistogramGroupSource.CalendarInterval(interval))
            .setTimeZone(zone);
        return builder.build();
    }

    protected GroupConfig createGroupConfig(Map<String, SingleGroupSource> groups) throws Exception {
        GroupConfig.Builder builder = GroupConfig.builder();
        for (Map.Entry<String, SingleGroupSource> sgs : groups.entrySet()) {
            builder.groupBy(sgs.getKey(), sgs.getValue());
        }
        return builder.build();
    }

    protected QueryConfig createQueryConfig(QueryBuilder queryBuilder) throws Exception {
        return new QueryConfig(queryBuilder);
    }

    protected AggregationConfig createAggConfig(AggregatorFactories.Builder aggregations) throws Exception {
        return new AggregationConfig(aggregations);
    }

    protected PivotConfig createPivotConfig(
        Map<String, SingleGroupSource> groups,
        AggregatorFactories.Builder aggregations
    ) throws Exception {
        return PivotConfig.builder()
            .setGroups(createGroupConfig(groups))
            .setAggregationConfig(createAggConfig(aggregations))
            .build();
    }

    protected TransformConfig.Builder createTransformConfigBuilder(
        String id,
        String destinationIndex,
        QueryBuilder queryBuilder,
        String... sourceIndices
    ) throws Exception {
        return TransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder().setIndex(sourceIndices).setQueryConfig(createQueryConfig(queryBuilder)).build())
            .setDest(DestConfig.builder().setIndex(destinationIndex).build())
            .setFrequency(TimeValue.timeValueSeconds(10))
            .setDescription("Test transform config id: " + id);
    }

    protected void bulkIndexDocs(BulkRequest request) throws Exception {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            BulkResponse response = restClient.bulk(request, RequestOptions.DEFAULT);
            assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
        }
    }

    protected void updateConfig(String id, TransformConfigUpdate update) throws Exception {
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            restClient.transform().updateTransform(new UpdateTransformRequest(update, id), RequestOptions.DEFAULT);
        }
    }

    protected void createReviewsIndex(String indexName,
                                      int numDocs,
                                      int numUsers,
                                      Function<Integer, Integer> userIdProvider,
                                      Function<Integer, String> dateStringProvider) throws Exception {
        assert numUsers > 0;
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {

            // create mapping
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.startObject("properties")
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
                        .endObject();
                }
                builder.endObject();
                CreateIndexResponse response = restClient.indices()
                    .create(new CreateIndexRequest(indexName).mapping(builder), RequestOptions.DEFAULT);
                assertThat(response.isAcknowledged(), is(true));
            }

            // create index
            BulkRequest bulk = new BulkRequest(indexName);
            for (int i = 0; i < numDocs; i++) {
                Integer user = userIdProvider.apply(i);
                int stars = i % 5;
                long business = i % 50;
                String dateString = dateStringProvider.apply(i);

                StringBuilder sourceBuilder = new StringBuilder().append("{");
                if (user != null) {
                    sourceBuilder
                        .append("\"user_id\":\"")
                        .append("user_")
                        .append(user)
                        .append("\",");
                }
                sourceBuilder
                    .append("\"count\":")
                    .append(i)
                    .append(",\"business_id\":\"")
                    .append("business_")
                    .append(business)
                    .append("\",\"stars\":")
                    .append(stars)
                    .append(",\"comment\":")
                    .append("\"Great stuff, deserves " + stars + " stars\"")
                    .append(",\"regular_object\":{\"foo\": 42}")
                    .append(",\"nested_object\":{\"bar\": 43}")
                    .append(",\"timestamp\":\"")
                    .append(dateString)
                    .append("\"}");
                bulk.add(new IndexRequest().source(sourceBuilder.toString(), XContentType.JSON));

                if (i % 100 == 0) {
                    BulkResponse response = restClient.bulk(bulk, RequestOptions.DEFAULT);
                    assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
                    bulk = new BulkRequest(indexName);
                }
            }
            BulkResponse response = restClient.bulk(bulk, RequestOptions.DEFAULT);
            assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
            restClient.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
        }
    }

    protected Map<String, Object> toLazy(ToXContent parsedObject) throws Exception {
        BytesReference bytes = XContentHelper.toXContent(parsedObject, XContentType.JSON, false);
        try (
            XContentParser parser = XContentHelper.createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                bytes,
                XContentType.JSON
            )
        ) {
            return parser.mapOrdered();
        }
    }

    private void waitForPendingTasks() {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setWaitForCompletion(true);
        listTasksRequest.setDetailed(true);
        listTasksRequest.setTimeout(TimeValue.timeValueSeconds(10));
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {

            restClient.tasks().list(listTasksRequest, RequestOptions.DEFAULT);
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

    protected static class TestRestHighLevelClient extends RestHighLevelClient {
        private static final List<NamedXContentRegistry.Entry> X_CONTENT_ENTRIES = new SearchModule(Settings.EMPTY, Collections.emptyList())
            .getNamedXContents();

        TestRestHighLevelClient() {
            super(client(), restClient -> {}, X_CONTENT_ENTRIES);
        }
    }
}
