/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FailureStoreMetricsWithIncrementalBulkIT extends ESIntegTestCase {

    private static final List<String> METRICS = List.of(
        FailureStoreMetrics.METRIC_TOTAL,
        FailureStoreMetrics.METRIC_FAILURE_STORE,
        FailureStoreMetrics.METRIC_REJECTED
    );

    private String dataStream = "data-stream-incremental";
    private String template = "template-incremental";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, TestTelemetryPlugin.class, MapperExtrasPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "512B")
            .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "2048B")
            .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "2KB")
            .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "1024B")
            .build();
    }

    public void testShortCircuitFailure() throws Exception {
        putComposableIndexTemplate(true);
        createDataStream();

        String coordinatingOnlyNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, coordinatingOnlyNode);
        IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest();

        AtomicBoolean nextRequested = new AtomicBoolean(true);
        AtomicLong hits = new AtomicLong(0);
        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(dataStream)), refCounted::decRef, () -> nextRequested.set(true));
            hits.incrementAndGet();
            System.out.println("Hits = " + hits.get());
        }
        assertBusy(() -> assertTrue(nextRequested.get()));
        System.out.println("Final Hits = " + hits.get());
        var measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), (int) hits.get(), dataStream);
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(0, measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());

        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "*" });
        GetDataStreamAction.Response getDataStreamResponse = client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest)
            .actionGet();
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getName(), equalTo(dataStream));
        assertThat(getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().size(), equalTo(1));
        String backingIndex = getDataStreamResponse.getDataStreams().get(0).getDataStream().getIndices().get(0).getName();
        System.out.println("Backing index = " + backingIndex);
        assertThat(backingIndex, backingIndexEqualTo(dataStream, 1));

        String node = findShard(resolveIndex(backingIndex), 0);
        System.out.println("Node = " + node);
        IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, node);
        long memoryLimit = primaryPressure.stats().getMemoryLimit();
        long primaryRejections = primaryPressure.stats().getPrimaryRejections();
        System.out.println("Primary rejections = " + primaryRejections);
        System.out.println("Memory limit = " + memoryLimit);
        try (Releasable releasable = primaryPressure.markPrimaryOperationStarted(10, memoryLimit, false)) {
            while (primaryPressure.stats().getPrimaryRejections() == primaryRejections) {
                while (nextRequested.get()) {
                    nextRequested.set(false);
                    refCounted.incRef();
                    List<DocWriteRequest<?>> requests = new ArrayList<>();
                    for (int i = 0; i < 20; ++i) {
                        requests.add(indexRequest(dataStream));
                    }
                    handler.addItems(requests, refCounted::decRef, () -> nextRequested.set(true));
                }
                assertBusy(() -> assertTrue(nextRequested.get()));
                System.out.println("Primary rejections = " + primaryPressure.stats().getPrimaryRejections());
            }
            System.out.println("Primary rejections = " + primaryPressure.stats().getPrimaryRejections());
        }

        while (nextRequested.get()) {
            nextRequested.set(false);
            refCounted.incRef();
            handler.addItems(List.of(indexRequest(dataStream)), refCounted::decRef, () -> nextRequested.set(true));
        }

        assertBusy(() -> assertTrue(nextRequested.get()));

        System.out.println("Hits = " + hits.get());
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        handler.lastItems(List.of(indexRequest(dataStream)), refCounted::decRef, future);

        BulkResponse bulkResponse = safeGet(future);
        System.out.println("total bulk items = " + bulkResponse.getItems().length);
        assertTrue(bulkResponse.hasFailures());

        for (int i = 0; i < hits.get(); ++i) {
            System.out.println("i = " + i);
            if (bulkResponse.getItems()[i].isFailed()) System.out.println(bulkResponse.getItems()[i].toString());
            assertFalse(bulkResponse.getItems()[i].isFailed());
        }

        for (int i = (int) hits.get(); i < bulkResponse.getItems().length; ++i) {
            BulkItemResponse item = bulkResponse.getItems()[i];
            assertTrue(item.isFailed());
            assertThat(item.getFailure().getCause().getCause(), instanceOf(EsRejectedExecutionException.class));
        }
        measurements = collectTelemetry();
        assertMeasurements(measurements.get(FailureStoreMetrics.METRIC_TOTAL), bulkResponse.getItems().length, dataStream);
        assertEquals(bulkResponse.getItems().length - hits.get(), measurements.get(FailureStoreMetrics.METRIC_FAILURE_STORE).size());
        assertEquals(bulkResponse.getItems().length - hits.get(), measurements.get(FailureStoreMetrics.METRIC_REJECTED).size());
    }

    private void createDataStream() {
        final var createDataStreamRequest = new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream);
        assertAcked(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).actionGet());
    }

    private static Map<String, List<Measurement>> collectTelemetry() {
        Map<String, List<Measurement>> measurements = new HashMap<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();

            telemetryPlugin.collect();

            for (String metricName : METRICS) {
                measurements.put(metricName, telemetryPlugin.getLongCounterMeasurement(metricName));
            }
        }
        return measurements;
    }

    private void assertMeasurements(List<Measurement> measurements, int expectedSize, String expectedDataStream) {
        assertMeasurements(measurements, expectedSize, expectedDataStream, (Consumer<Measurement>) null);
    }

    private void assertMeasurements(
        List<Measurement> measurements,
        int expectedSize,
        String expectedDataStream,
        FailureStoreMetrics.ErrorLocation location
    ) {
        assertMeasurements(
            measurements,
            expectedSize,
            expectedDataStream,
            measurement -> assertEquals(location.name(), measurement.attributes().get("error_location"))
        );
    }

    private void assertMeasurements(
        List<Measurement> measurements,
        int expectedSize,
        String expectedDataStream,
        Consumer<Measurement> customAssertion
    ) {
        assertEquals(expectedSize, measurements.size());
        for (Measurement measurement : measurements) {
            assertEquals(expectedDataStream, measurement.attributes().get("data_stream"));
            if (customAssertion != null) {
                customAssertion.accept(measurement);
            }
        }
    }

    private static IndexRequest indexRequest(String dataStream) {
        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        String value = "1";
        IndexRequest indexRequest = new IndexRequest(dataStream).opType(DocWriteRequest.OpType.CREATE)
            .source(Strings.format("{\"%s\":\"%s\", \"count\": %s}", DEFAULT_TIMESTAMP_FIELD, time, value), XContentType.JSON);
        return indexRequest;
    }

    private void putComposableIndexTemplate(boolean failureStore) throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(template);
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream + "*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, failureStore))
                .template(new Template(null, new CompressedXContent("""
                    {
                      "dynamic": false,
                      "properties": {
                        "@timestamp": {
                          "type": "date"
                        },
                        "count": {
                            "type": "long"
                        }
                      }
                    }"""), null))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
    }

    protected static String findShard(Index index, int shardId) {
        for (String node : internalCluster().getNodeNames()) {
            var indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                IndexShard shard = indexService.getShardOrNull(shardId);
                if (shard != null && shard.isActive() && shard.routingEntry().primary()) {
                    return node;
                }
            }
        }
        throw new AssertionError("IndexShard instance not found for shard " + new ShardId(index, shardId));
    }
}
