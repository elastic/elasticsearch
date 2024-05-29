/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static org.elasticsearch.datastreams.DataStreamsStatsTests.StaticWriteLoadForecasterPlugin.WRITE_LOAD_FORECAST_PER_SHARD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class DataStreamsStatsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class, StaticWriteLoadForecasterPlugin.class);
    }

    private final Set<String> createdDataStreams = new HashSet<>();

    @Override
    @After
    public void tearDown() throws Exception {
        if (createdDataStreams.isEmpty() == false) {
            for (String createdDataStream : createdDataStreams) {
                deleteDataStream(createdDataStream);
            }
            createdDataStreams.clear();
        }
        super.tearDown();
    }

    public void testStatsNoDataStream() throws Exception {
        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(0, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(0, stats.getDataStreamCount());
        assertEquals(0, stats.getBackingIndices());
        assertEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(0, stats.getDataStreams().length);
    }

    public void testStatsEmptyDataStream() throws Exception {
        String dataStreamName = createDataStream();

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(1, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(1, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(0L, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    public void testStatsExistingDataStream() throws Exception {
        String dataStreamName = createDataStream();
        long timestamp = createDocument(dataStreamName);

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(1, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(1, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(timestamp, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    public void testStatsExistingHiddenDataStream() throws Exception {
        String dataStreamName = createDataStream(true);
        long timestamp = createDocument(dataStreamName);

        DataStreamsStatsAction.Response stats = getDataStreamsStats(true);
        assertEquals(1, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(1, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(timestamp, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    public void testStatsClosedBackingIndexDataStream() throws Exception {
        String dataStreamName = createDataStream();
        createDocument(dataStreamName);
        assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get().isAcknowledged());
        assertTrue(indicesAdmin().close(new CloseIndexRequest(".ds-" + dataStreamName + "-*-000001")).actionGet().isAcknowledged());

        assertBusy(
            () -> assertNotEquals(ClusterHealthStatus.RED, clusterAdmin().health(new ClusterHealthRequest()).actionGet().getStatus())
        );

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(2, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(2, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(2, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(0L, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));

        // Call stats again after writing a new event into the write index
        long timestamp = createDocument(dataStreamName);

        stats = getDataStreamsStats();
        assertEquals(2, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(2, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(2, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(timestamp, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    public void testStatsRolledDataStream() throws Exception {
        String dataStreamName = createDataStream();
        long timestamp = createDocument(dataStreamName);
        assertTrue(indicesAdmin().rolloverIndex(new RolloverRequest(dataStreamName, null)).get().isAcknowledged());
        timestamp = max(timestamp, createDocument(dataStreamName));

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(2, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(2, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(2, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(timestamp, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    public void testStatsMultipleDataStreams() throws Exception {
        for (int dataStreamCount = 0; dataStreamCount < (2 + randomInt(3)); dataStreamCount++) {
            createDataStream();
        }

        // Create a number of documents in each data stream
        Map<String, Long> maxTimestamps = new HashMap<>();
        for (String createdDataStream : createdDataStreams) {
            for (int documentCount = 0; documentCount < (1 + randomInt(10)); documentCount++) {
                long ts = createDocument(createdDataStream);
                long maxTS = max(maxTimestamps.getOrDefault(createdDataStream, 0L), ts);
                maxTimestamps.put(createdDataStream, maxTS);
            }
        }

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        logger.error(stats.toString());
        assertEquals(createdDataStreams.size(), stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(createdDataStreams.size(), stats.getDataStreamCount());
        assertEquals(createdDataStreams.size(), stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(createdDataStreams.size(), stats.getDataStreams().length);
        for (DataStreamsStatsAction.DataStreamStats dataStreamStats : stats.getDataStreams()) {
            Long expectedMaxTS = maxTimestamps.get(dataStreamStats.getDataStream());
            assertNotNull("All indices should have max timestamps", expectedMaxTS);
            assertEquals(1, dataStreamStats.getBackingIndices());
            assertEquals(expectedMaxTS.longValue(), dataStreamStats.getMaximumTimestamp());
            assertNotEquals(0L, dataStreamStats.getStoreSize().getBytes());
            assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
            assertThat(null, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
        }
    }

    public void testWriteLoadStatsExistingDataStream() throws Exception {
        int shards = 3;
        String dataStreamName = createDataStream("write-load-test-", 3);
        long timestamp = createDocument(dataStreamName);

        DataStreamsStatsAction.Response stats = getDataStreamsStats();
        assertEquals(shards, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getDataStreamCount());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreams().length);
        assertEquals(dataStreamName, stats.getDataStreams()[0].getDataStream());
        assertEquals(1, stats.getDataStreams()[0].getBackingIndices());
        assertEquals(timestamp, stats.getDataStreams()[0].getMaximumTimestamp());
        assertNotEquals(0L, stats.getDataStreams()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreams()[0].getStoreSize().getBytes());
        assertThat(WRITE_LOAD_FORECAST_PER_SHARD, is(stats.getDataStreams()[0].getWriteLoadForecastPerShard()));
        assertThat(WRITE_LOAD_FORECAST_PER_SHARD * shards, is(stats.getDataStreams()[0].getWriteLoadForecastPerIndex()));
    }

    private String createDataStream() throws Exception {
        return createDataStream(false, "", 1);
    }

    private String createDataStream(String namePrefix, int shards) throws Exception {
        return createDataStream(false, namePrefix, shards);
    }
    private String createDataStream(boolean hidden) throws Exception {
        return createDataStream(hidden, "", 1);
    }

    private String createDataStream(boolean hidden, String namePrefix, int shards) throws Exception {
        String dataStreamName = namePrefix + randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template idxTemplate = new Template(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards).build(),
            new CompressedXContent("""
            {"properties":{"@timestamp":{"type":"date"},"data":{"type":"keyword"}}}
            """), null);
        ComposableIndexTemplate template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(dataStreamName + "*"))
            .template(idxTemplate)
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(hidden, false))
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request(dataStreamName + "_template").indexTemplate(template)
            )
        );
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)));
        createdDataStreams.add(dataStreamName);
        return dataStreamName;
    }

    private long createDocument(String dataStreamName) throws Exception {
        // Get some randomized but reasonable timestamps on the data since not all of it is guaranteed to arrive in order.
        long timeSeed = System.currentTimeMillis();
        long timestamp = randomLongBetween(timeSeed - TimeUnit.HOURS.toMillis(5), timeSeed);
        client().index(
            new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .source(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("@timestamp", timestamp)
                        .field("data", randomAlphaOfLength(25))
                        .endObject()
                )
        ).get();
        indicesAdmin().refresh(new RefreshRequest(".ds-" + dataStreamName + "*").indicesOptions(IndicesOptions.lenientExpandOpenHidden()))
            .get();
        return timestamp;
    }

    private DataStreamsStatsAction.Response getDataStreamsStats() throws Exception {
        return getDataStreamsStats(false);
    }

    private DataStreamsStatsAction.Response getDataStreamsStats(boolean includeHidden) throws Exception {
        DataStreamsStatsAction.Request request = new DataStreamsStatsAction.Request();
        if (includeHidden) {
            request.indicesOptions(
                IndicesOptions.builder(request.indicesOptions())
                    .wildcardOptions(IndicesOptions.WildcardOptions.builder(request.indicesOptions().wildcardOptions()).includeHidden(true))
                    .build()
            );
        }
        return client().execute(DataStreamsStatsAction.INSTANCE, request).get();
    }

    private void deleteDataStream(String dataStreamName) {
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(new String[] { dataStreamName })));
        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request(dataStreamName + "_template")
            )
        );
    }

    /**
     * Plugin providing {@link WriteLoadForecaster} implementation for Test purposes.
     * If the given index's name contains "write-load-test" the forecaster returns 0.25, else {@link OptionalDouble#empty()}
     */
    public static class StaticWriteLoadForecasterPlugin extends Plugin implements ClusterPlugin {

        public static final double WRITE_LOAD_FORECAST_PER_SHARD = 0.25;

        @Override
        public Collection<WriteLoadForecaster> createWriteLoadForecasters(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings
        ) {
            return List.of(new WriteLoadForecaster() {
                @Override
                public Metadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, Metadata.Builder metadata) {
                    return metadata;
                }

                @Override
                public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
                    if (indexMetadata.getIndex().getName().contains("write-load-test")) {
                        return OptionalDouble.of(WRITE_LOAD_FORECAST_PER_SHARD);
                    }
                    return OptionalDouble.empty();
                }
            });
        }
    }
}
