/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.dataframe.DeleteDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsRequest;
import org.elasticsearch.client.dataframe.GetDataFrameTransformStatsResponse;
import org.elasticsearch.client.dataframe.PutDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StartDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.StopDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.StopDataFrameTransformResponse;
import org.elasticsearch.client.dataframe.UpdateDataFrameTransformRequest;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.client.dataframe.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.client.dataframe.transforms.DestConfig;
import org.elasticsearch.client.dataframe.transforms.QueryConfig;
import org.elasticsearch.client.dataframe.transforms.SourceConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.AggregationConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.client.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.pivot.SingleGroupSource;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

abstract class DataFrameIntegTestCase extends ESRestTestCase {

    private Map<String, DataFrameTransformConfig> transformConfigs = new HashMap<>();

    protected void cleanUp() throws IOException {
        cleanUpTransforms();
        waitForPendingTasks();
    }

    protected void cleanUpTransforms() throws IOException {
        for (DataFrameTransformConfig config : transformConfigs.values()) {
            stopDataFrameTransform(config.getId());
            deleteDataFrameTransform(config.getId());
        }
        transformConfigs.clear();
    }

    protected StopDataFrameTransformResponse stopDataFrameTransform(String id) throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        return restClient.dataFrame().stopDataFrameTransform(new StopDataFrameTransformRequest(id, true, null), RequestOptions.DEFAULT);
    }

    protected StartDataFrameTransformResponse startDataFrameTransform(String id, RequestOptions options) throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        return restClient.dataFrame().startDataFrameTransform(new StartDataFrameTransformRequest(id), options);
    }

    protected AcknowledgedResponse deleteDataFrameTransform(String id) throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        AcknowledgedResponse response =
            restClient.dataFrame().deleteDataFrameTransform(new DeleteDataFrameTransformRequest(id), RequestOptions.DEFAULT);
        if (response.isAcknowledged()) {
            transformConfigs.remove(id);
        }
        return response;
    }

    protected AcknowledgedResponse putDataFrameTransform(DataFrameTransformConfig config, RequestOptions options) throws IOException {
        if (transformConfigs.keySet().contains(config.getId())) {
            throw new IllegalArgumentException("data frame transform [" + config.getId() + "] is already registered");
        }
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        AcknowledgedResponse response =
            restClient.dataFrame().putDataFrameTransform(new PutDataFrameTransformRequest(config), options);
        if (response.isAcknowledged()) {
            transformConfigs.put(config.getId(), config);
        }
        return response;
    }

    protected GetDataFrameTransformStatsResponse getDataFrameTransformStats(String id) throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        return restClient.dataFrame().getDataFrameTransformStats(new GetDataFrameTransformStatsRequest(id), RequestOptions.DEFAULT);
    }

    protected GetDataFrameTransformResponse getDataFrameTransform(String id) throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        return restClient.dataFrame().getDataFrameTransform(new GetDataFrameTransformRequest(id), RequestOptions.DEFAULT);
    }

    protected void waitUntilCheckpoint(String id, long checkpoint) throws Exception {
        waitUntilCheckpoint(id, checkpoint, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilCheckpoint(String id, long checkpoint, TimeValue waitTime) throws Exception {
        assertBusy(() ->
            assertEquals(checkpoint, getDataFrameTransformStats(id)
                .getTransformsStats()
                .get(0)
                .getCheckpointingInfo()
                .getLast()
                .getCheckpoint()),
            waitTime.getMillis(),
            TimeUnit.MILLISECONDS);
    }

    protected DateHistogramGroupSource createDateHistogramGroupSourceWithFixedInterval(String field,
                                                                                       DateHistogramInterval interval,
                                                                                       ZoneId zone) {
        DateHistogramGroupSource.Builder builder = DateHistogramGroupSource.builder()
            .setField(field)
            .setInterval(new DateHistogramGroupSource.FixedInterval(interval))
            .setTimeZone(zone);
        return builder.build();
    }

    protected DateHistogramGroupSource createDateHistogramGroupSourceWithCalendarInterval(String field,
                                                                                          DateHistogramInterval interval,
                                                                                          ZoneId zone) {
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

    protected PivotConfig createPivotConfig(Map<String, SingleGroupSource> groups,
                                            AggregatorFactories.Builder aggregations) throws Exception {
        return createPivotConfig(groups, aggregations, null);
    }

    protected PivotConfig createPivotConfig(Map<String, SingleGroupSource> groups,
                                            AggregatorFactories.Builder aggregations,
                                            Integer size) throws Exception {
        PivotConfig.Builder builder = PivotConfig.builder()
            .setGroups(createGroupConfig(groups))
            .setAggregationConfig(createAggConfig(aggregations))
            .setMaxPageSearchSize(size);
        return builder.build();
    }

    protected DataFrameTransformConfig createTransformConfig(String id,
                                                             Map<String, SingleGroupSource> groups,
                                                             AggregatorFactories.Builder aggregations,
                                                             String destinationIndex,
                                                             String... sourceIndices) throws Exception {
        return createTransformConfig(id, groups, aggregations, destinationIndex, QueryBuilders.matchAllQuery(), sourceIndices);
    }

    protected DataFrameTransformConfig.Builder createTransformConfigBuilder(String id,
                                                                            Map<String, SingleGroupSource> groups,
                                                                            AggregatorFactories.Builder aggregations,
                                                                            String destinationIndex,
                                                                            QueryBuilder queryBuilder,
                                                                            String... sourceIndices) throws Exception {
        return DataFrameTransformConfig.builder()
            .setId(id)
            .setSource(SourceConfig.builder().setIndex(sourceIndices).setQueryConfig(createQueryConfig(queryBuilder)).build())
            .setDest(DestConfig.builder().setIndex(destinationIndex).build())
            .setFrequency(TimeValue.timeValueSeconds(10))
            .setPivotConfig(createPivotConfig(groups, aggregations))
            .setDescription("Test data frame transform config id: " + id);
    }

    protected DataFrameTransformConfig createTransformConfig(String id,
                                                             Map<String, SingleGroupSource> groups,
                                                             AggregatorFactories.Builder aggregations,
                                                             String destinationIndex,
                                                             QueryBuilder queryBuilder,
                                                             String... sourceIndices) throws Exception {
        return createTransformConfigBuilder(id, groups, aggregations, destinationIndex, queryBuilder, sourceIndices).build();
    }

    protected void bulkIndexDocs(BulkRequest request) throws Exception {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        BulkResponse response = restClient.bulk(request, RequestOptions.DEFAULT);
        assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
    }

    protected void updateConfig(String id, DataFrameTransformConfigUpdate update) throws Exception {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        restClient.dataFrame().updateDataFrameTransform(new UpdateDataFrameTransformRequest(update, id), RequestOptions.DEFAULT);
    }

    protected void createReviewsIndex(String indexName, int numDocs) throws Exception {
        RestHighLevelClient restClient = new TestRestHighLevelClient();

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
                    .endObject();
            }
            builder.endObject();
            CreateIndexResponse response =
                restClient.indices().create(new CreateIndexRequest(indexName).mapping(builder), RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        // create index
        BulkRequest bulk = new BulkRequest(indexName);
        int day = 10;
        for (int i = 0; i < numDocs; i++) {
            long user = i % 28;
            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;
            int hour = 10 + (i % 13);
            int min = 10 + (i % 49);
            int sec = 10 + (i % 49);

            String date_string = "2017-01-" + (day < 10 ? "0" + day : day) + "T" + hour + ":" + min + ":" + sec + "Z";

            StringBuilder sourceBuilder = new StringBuilder();
            sourceBuilder.append("{\"user_id\":\"")
                .append("user_")
                .append(user)
                .append("\",\"count\":")
                .append(i)
                .append(",\"business_id\":\"")
                .append("business_")
                .append(business)
                .append("\",\"stars\":")
                .append(stars)
                .append(",\"timestamp\":\"")
                .append(date_string)
                .append("\"}");
            bulk.add(new IndexRequest().source(sourceBuilder.toString(), XContentType.JSON));

            if (i % 50 == 0) {
                BulkResponse response = restClient.bulk(bulk, RequestOptions.DEFAULT);
                assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
                bulk = new BulkRequest(indexName);
                day = (day + 1) % 28;
            }
        }
        BulkResponse response = restClient.bulk(bulk, RequestOptions.DEFAULT);
        assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
        restClient.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
    }

    protected Map<String, Object> toLazy(ToXContent parsedObject) throws Exception {
        BytesReference bytes = XContentHelper.toXContent(parsedObject, XContentType.JSON, false);
        try(XContentParser parser = XContentHelper.createParser(xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            bytes,
            XContentType.JSON)) {
            return parser.mapOrdered();
        }
    }

    private void waitForPendingTasks() {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setWaitForCompletion(true);
        listTasksRequest.setDetailed(true);
        listTasksRequest.setTimeout(TimeValue.timeValueSeconds(10));
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        try {
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
        final String token = "Basic " +
            Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    protected static class TestRestHighLevelClient extends RestHighLevelClient {
        private static final List<NamedXContentRegistry.Entry> X_CONTENT_ENTRIES =
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents();
        TestRestHighLevelClient() {
            super(client(), restClient -> {}, X_CONTENT_ENTRIES);
        }
    }
}
