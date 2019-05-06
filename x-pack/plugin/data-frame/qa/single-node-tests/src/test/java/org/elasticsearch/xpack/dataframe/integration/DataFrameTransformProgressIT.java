/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DestConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.QueryConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.dataframe.transforms.TransformProgressGatherer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.dataframe.integration.DataFrameRestTestCase.REVIEWS_INDEX_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataFrameTransformProgressIT extends ESIntegTestCase {

    protected void createReviewsIndex() throws Exception {
        final int numDocs = 1000;

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
            CreateIndexResponse response = client().admin()
                .indices()
                .prepareCreate(REVIEWS_INDEX_NAME)
                .addMapping("_doc", builder)
                .get();
            assertThat(response.isAcknowledged(), is(true));
        }

        // create index
        BulkRequestBuilder bulk = client().prepareBulk(REVIEWS_INDEX_NAME, "_doc");
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
                BulkResponse response = client().bulk(bulk.request()).get();
                assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
                bulk = client().prepareBulk(REVIEWS_INDEX_NAME, "_doc");
                day += 1;
            }
        }
        client().bulk(bulk.request()).get();
        client().admin().indices().prepareRefresh(REVIEWS_INDEX_NAME).get();
    }

    public void testGetProgress() throws Exception {
        createReviewsIndex();
        SourceConfig sourceConfig = new SourceConfig(REVIEWS_INDEX_NAME);
        DestConfig destConfig = new DestConfig("unnecessary");
        GroupConfig histgramGroupConfig = new GroupConfig(Collections.emptyMap(),
            Collections.singletonMap("every_50", new HistogramGroupSource("count", 50.0)));
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        AggregationConfig aggregationConfig = new AggregationConfig(Collections.emptyMap(), aggs);
        PivotConfig pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig);
        DataFrameTransformConfig config = new DataFrameTransformConfig("get_progress_transform",
            sourceConfig,
            destConfig,
            null,
            pivotConfig,
            null);

        PlainActionFuture<DataFrameTransformProgress> progressFuture = new PlainActionFuture<>();
        TransformProgressGatherer.getInitialProgress(client(), config, progressFuture);

        DataFrameTransformProgress progress = progressFuture.get();

        assertThat(progress.getTotalDocs(), equalTo(1000L));
        assertThat(progress.getRemainingDocs(), equalTo(1000L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));


        QueryConfig queryConfig = new QueryConfig(Collections.emptyMap(), QueryBuilders.termQuery("user_id", "user_26"));
        pivotConfig = new PivotConfig(histgramGroupConfig, aggregationConfig);
        sourceConfig = new SourceConfig(new String[]{REVIEWS_INDEX_NAME}, queryConfig);
        config = new DataFrameTransformConfig("get_progress_transform",
            sourceConfig,
            destConfig,
            null,
            pivotConfig,
            null);


        progressFuture = new PlainActionFuture<>();

        TransformProgressGatherer.getInitialProgress(client(), config, progressFuture);
        progress = progressFuture.get();

        assertThat(progress.getTotalDocs(), equalTo(35L));
        assertThat(progress.getRemainingDocs(), equalTo(35L));
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        client().admin().indices().prepareDelete(REVIEWS_INDEX_NAME).get();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        builder.put(SecurityField.USER_SETTING.getKey(), "x_pack_rest_user:" +  SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return builder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(XPackClientPlugin.class, Netty4Plugin.class);
    }
}
