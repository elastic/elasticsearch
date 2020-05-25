/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.transform.transforms.TransformProgressGatherer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.transform.integration.TransformRestTestCase.REVIEWS_INDEX_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformProgressIT extends ESRestTestCase {
    protected void createReviewsIndex() throws Exception {
        final int numDocs = 1000;
        final RestHighLevelClient restClient = new TestRestHighLevelClient();

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
            CreateIndexResponse response = restClient.indices()
                .create(new CreateIndexRequest(REVIEWS_INDEX_NAME).mapping(builder), RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), is(true));
        }

        // create index
        BulkRequest bulk = new BulkRequest(REVIEWS_INDEX_NAME);
        int day = 10;
        for (int i = 0; i < numDocs; i++) {
            long user = i % 28;
            int stars = (i + 20) % 5;
            long business = (i + 100) % 50;
            int hour = 10 + (i % 13);
            int min = 10 + (i % 49);
            int sec = 10 + (i % 49);

            String date_string = "2017-01-" + day + "T" + hour + ":" + min + ":" + sec + "Z";

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
                bulk = new BulkRequest(REVIEWS_INDEX_NAME);
                day += 1;
            }
        }
        restClient.bulk(bulk, RequestOptions.DEFAULT);
        restClient.indices().refresh(new RefreshRequest(REVIEWS_INDEX_NAME), RequestOptions.DEFAULT);
    }

    public void testGetProgress() throws Exception {
        createReviewsIndex();
        SourceConfig sourceConfig = new SourceConfig(REVIEWS_INDEX_NAME);
        DestConfig destConfig = new DestConfig("unnecessary", null);
        GroupConfig histgramGroupConfig = new GroupConfig(
            Collections.emptyMap(),
            Collections.singletonMap("every_50", new HistogramGroupSource("count", null, 50.0))
        );
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggregationConfig = new AggregationConfig(Collections.emptyMap(), aggs);
        PivotConfig pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig, null);
        TransformConfig config = new TransformConfig(
            "get_progress_transform",
            sourceConfig,
            destConfig,
            null,
            null,
            null,
            pivotConfig,
            null,
            null
        );

        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        SearchResponse response = restClient.search(
            TransformProgressGatherer.getSearchRequest(config, config.getSource().getQueryConfig().getQuery()),
            RequestOptions.DEFAULT
        );

        TransformProgress progress = TransformProgressGatherer.searchResponseToTransformProgressFunction().apply(response);

        assertThat(progress.getTotalDocs(), equalTo(1000L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        QueryConfig queryConfig = new QueryConfig(Collections.emptyMap(), QueryBuilders.termQuery("user_id", "user_26"));
        pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig, null);
        sourceConfig = new SourceConfig(new String[] { REVIEWS_INDEX_NAME }, queryConfig);
        config = new TransformConfig("get_progress_transform", sourceConfig, destConfig, null, null, null, pivotConfig, null, null);

        response = restClient.search(
            TransformProgressGatherer.getSearchRequest(config, config.getSource().getQueryConfig().getQuery()),
            RequestOptions.DEFAULT
        );
        progress = TransformProgressGatherer.searchResponseToTransformProgressFunction().apply(response);

        assertThat(progress.getTotalDocs(), equalTo(35L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        histgramGroupConfig = new GroupConfig(
            Collections.emptyMap(),
            Collections.singletonMap("every_50", new HistogramGroupSource("missing_field", null, 50.0))
        );
        pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig, null);
        config = new TransformConfig("get_progress_transform", sourceConfig, destConfig, null, null, null, pivotConfig, null, null);

        response = restClient.search(
            TransformProgressGatherer.getSearchRequest(config, config.getSource().getQueryConfig().getQuery()),
            RequestOptions.DEFAULT
        );
        progress = TransformProgressGatherer.searchResponseToTransformProgressFunction().apply(response);

        assertThat(progress.getTotalDocs(), equalTo(0L));
        assertThat(progress.getDocumentsProcessed(), equalTo(0L));
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        deleteIndex(REVIEWS_INDEX_NAME);
    }

    @Override
    protected Settings restClientSettings() {
        final String token = "Basic "
            + Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private class TestRestHighLevelClient extends RestHighLevelClient {
        TestRestHighLevelClient() {
            super(client(), restClient -> {}, Collections.emptyList());
        }
    }
}
