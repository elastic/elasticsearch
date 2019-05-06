/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.integration;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.StartDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DestConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.QueryConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.security.SecurityField;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

abstract class DataFrameIntegTestCase extends ESIntegTestCase {

    protected static final String REVIEWS_INDEX_NAME = "data_frame_reviews";

    private Map<String, DataFrameTransformConfig> transformConfigs = new HashMap<>();

    protected void cleanUp() {
        cleanUpTransforms();
        waitForPendingTasks();
    }

    protected void cleanUpTransforms() {
        for (DataFrameTransformConfig config : transformConfigs.values()) {
            stopDataFrameTransform(config.getId());
            deleteDataFrameTransform(config.getId());
        }
        transformConfigs.clear();
    }

    protected StopDataFrameTransformAction.Response stopDataFrameTransform(String id) {
        return client().execute(StopDataFrameTransformAction.INSTANCE,
            new StopDataFrameTransformAction.Request(id, true, false, null)).actionGet();
    }

    protected StartDataFrameTransformAction.Response startDataFrameTransform(String id) {
        return client().execute(StartDataFrameTransformAction.INSTANCE,
            new StartDataFrameTransformAction.Request(id, false)).actionGet();
    }

    protected DeleteDataFrameTransformAction.Response deleteDataFrameTransform(String id) {
        DeleteDataFrameTransformAction.Response response = client().execute(DeleteDataFrameTransformAction.INSTANCE,
            new DeleteDataFrameTransformAction.Request(id))
            .actionGet();
        if (response.isDeleted()) {
            transformConfigs.remove(id);
        }
        return response;
    }

    protected AcknowledgedResponse putDataFrameTransform(DataFrameTransformConfig config) {
        if (transformConfigs.keySet().contains(config.getId())) {
            throw new IllegalArgumentException("data frame transform [" + config.getId() + "] is already registered");
        }
        AcknowledgedResponse response = client().execute(PutDataFrameTransformAction.INSTANCE,
            new PutDataFrameTransformAction.Request(config))
            .actionGet();
        if (response.isAcknowledged()) {
            transformConfigs.put(config.getId(), config);
        }
        return response;
    }

    protected GetDataFrameTransformsStatsAction.Response getDataFrameTransformStats(String id) {
        return client().execute(GetDataFrameTransformsStatsAction.INSTANCE, new GetDataFrameTransformsStatsAction.Request(id)).actionGet();
    }

    protected void waitUntilCheckpoint(String id, long checkpoint) throws Exception {
        waitUntilCheckpoint(id, checkpoint, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilCheckpoint(String id, long checkpoint, TimeValue waitTime) throws Exception {
        assertBusy(() ->
            assertEquals(checkpoint, getDataFrameTransformStats(id)
                .getTransformsStateAndStats()
                .get(0)
                .getTransformState()
                .getCheckpoint()),
            waitTime.getMillis(),
            TimeUnit.MILLISECONDS);
    }

    protected DateHistogramGroupSource createDateHistogramGroupSource(String field, long interval, ZoneId zone, String format) {
        DateHistogramGroupSource source = new DateHistogramGroupSource(field);
        source.setFormat(format);
        source.setInterval(interval);
        source.setTimeZone(zone);
        return source;
    }

    protected DateHistogramGroupSource createDateHistogramGroupSource(String field,
                                                                      DateHistogramInterval interval,
                                                                      ZoneId zone,
                                                                      String format) {
        DateHistogramGroupSource source = new DateHistogramGroupSource(field);
        source.setFormat(format);
        source.setDateHistogramInterval(interval);
        source.setTimeZone(zone);
        return source;
    }

    protected GroupConfig createGroupConfig(Map<String, SingleGroupSource> groups) throws Exception {
        Map<String, Object> lazyParsed = new HashMap<>(groups.size());
        for(Map.Entry<String, SingleGroupSource> sgs : groups.entrySet()) {
            lazyParsed.put(sgs.getKey(), Collections.singletonMap(sgs.getValue().getType().value(), toLazy(sgs.getValue())));
        }
        return new GroupConfig(lazyParsed, groups);
    }

    protected QueryConfig createQueryConfig(QueryBuilder queryBuilder) throws Exception {
        return new QueryConfig(toLazy(queryBuilder), queryBuilder);
    }

    protected AggregationConfig createAggConfig(AggregatorFactories.Builder aggregations) throws Exception {
        return new AggregationConfig(toLazy(aggregations), aggregations);
    }

    protected PivotConfig createPivotConfig(Map<String, SingleGroupSource> groups,
                                            AggregatorFactories.Builder aggregations) throws Exception {
        return new PivotConfig(createGroupConfig(groups), createAggConfig(aggregations));
    }

    protected DataFrameTransformConfig createTransformConfig(String id,
                                                             Map<String, SingleGroupSource> groups,
                                                             AggregatorFactories.Builder aggregations,
                                                             String destinationIndex,
                                                             String... sourceIndices) throws Exception {
        return createTransformConfig(id, groups, aggregations, destinationIndex, QueryBuilders.matchAllQuery(), sourceIndices);
    }

    protected DataFrameTransformConfig createTransformConfig(String id,
                                                             Map<String, SingleGroupSource> groups,
                                                             AggregatorFactories.Builder aggregations,
                                                             String destinationIndex,
                                                             QueryBuilder queryBuilder,
                                                             String... sourceIndices) throws Exception {
        return new DataFrameTransformConfig(id,
            new SourceConfig(sourceIndices, createQueryConfig(queryBuilder)),
            new DestConfig(destinationIndex),
            Collections.emptyMap(),
            createPivotConfig(groups, aggregations),
            "Test data frame transform config id: " + id);
    }

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
        BulkResponse response = client().bulk(bulk.request()).get();
        assertThat(response.buildFailureMessage(), response.hasFailures(), is(false));
        client().admin().indices().prepareRefresh(REVIEWS_INDEX_NAME).get();
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
        try {
            admin().cluster().listTasks(listTasksRequest).get();
        } catch (Exception e) {
            throw new AssertionError("Failed to wait for pending tasks to complete", e);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected Settings externalClusterClientSettings() {
        Path key;
        Path certificate;
        try {
            key = PathUtils.get(getClass().getResource("/testnode.pem").toURI());
            certificate = PathUtils.get(getClass().getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("error trying to get keystore path", e);
        }
        Settings.Builder builder = Settings.builder();
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4);
        builder.put(SecurityField.USER_SETTING.getKey(), "x_pack_rest_user:" +  SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        builder.put("xpack.security.transport.ssl.enabled", true);
        builder.put("xpack.security.transport.ssl.key", key.toAbsolutePath().toString());
        builder.put("xpack.security.transport.ssl.certificate", certificate.toAbsolutePath().toString());
        builder.put("xpack.security.transport.ssl.key_passphrase", "testnode");
        builder.put("xpack.security.transport.ssl.verification_mode", "certificate");
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
