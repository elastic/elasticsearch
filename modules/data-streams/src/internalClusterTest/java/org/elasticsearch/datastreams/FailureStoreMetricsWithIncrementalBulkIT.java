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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
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

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class FailureStoreMetricsWithIncrementalBulkIT extends ESIntegTestCase {

    private static final List<String> METRICS = List.of(
        FailureStoreMetrics.METRIC_TOTAL,
        FailureStoreMetrics.METRIC_FAILURE_STORE,
        FailureStoreMetrics.METRIC_REJECTED
    );

    private static final String DATA_STREAM_NAME = "data-stream-incremental";

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
        createDataStreamWithFailureStore();

        String coordinatingOnlyNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        AbstractRefCounted refCounted = AbstractRefCounted.of(() -> {});
        IncrementalBulkService incrementalBulkService = internalCluster().getInstance(IncrementalBulkService.class, coordinatingOnlyNode);
        try (IncrementalBulkService.Handler handler = incrementalBulkService.newBulkRequest()) {

            AtomicBoolean nextRequested = new AtomicBoolean(true);
            int successfullyStored = 0;
            while (nextRequested.get()) {
                nextRequested.set(false);
                refCounted.incRef();
                handler.addItems(List.of(indexRequest(DATA_STREAM_NAME)), refCounted::decRef, () -> nextRequested.set(true));
                successfullyStored++;
            }
            assertBusy(() -> assertTrue(nextRequested.get()));
            var metrics = collectTelemetry();
            assertDataStreamMetric(metrics, FailureStoreMetrics.METRIC_TOTAL, DATA_STREAM_NAME, successfullyStored);
            assertDataStreamMetric(metrics, FailureStoreMetrics.METRIC_FAILURE_STORE, DATA_STREAM_NAME, 0);
            assertDataStreamMetric(metrics, FailureStoreMetrics.METRIC_REJECTED, DATA_STREAM_NAME, 0);

            // Introduce artificial pressure that will reject the following requests
            String node = findNodeOfPrimaryShard(DATA_STREAM_NAME);
            IndexingPressure primaryPressure = internalCluster().getInstance(IndexingPressure.class, node);
            long memoryLimit = primaryPressure.stats().getMemoryLimit();
            long primaryRejections = primaryPressure.stats().getPrimaryRejections();
            try (Releasable ignored = primaryPressure.validateAndMarkPrimaryOperationStarted(10, memoryLimit, 0, false, false)) {
                while (primaryPressure.stats().getPrimaryRejections() == primaryRejections) {
                    while (nextRequested.get()) {
                        nextRequested.set(false);
                        refCounted.incRef();
                        List<DocWriteRequest<?>> requests = new ArrayList<>();
                        for (int i = 0; i < 20; ++i) {
                            requests.add(indexRequest(DATA_STREAM_NAME));
                        }
                        handler.addItems(requests, refCounted::decRef, () -> nextRequested.set(true));
                    }
                    assertBusy(() -> assertTrue(nextRequested.get()));
                }
            }

            while (nextRequested.get()) {
                nextRequested.set(false);
                refCounted.incRef();
                handler.addItems(List.of(indexRequest(DATA_STREAM_NAME)), refCounted::decRef, () -> nextRequested.set(true));
            }

            assertBusy(() -> assertTrue(nextRequested.get()));

            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            handler.lastItems(List.of(indexRequest(DATA_STREAM_NAME)), refCounted::decRef, future);

            BulkResponse bulkResponse = safeGet(future);

            for (int i = 0; i < bulkResponse.getItems().length; ++i) {
                // the first requests were successful
                boolean hasFailed = i >= successfullyStored;
                assertThat(bulkResponse.getItems()[i].isFailed(), is(hasFailed));
                assertThat(bulkResponse.getItems()[i].getFailureStoreStatus(), is(IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN));
            }

            metrics = collectTelemetry();
            assertDataStreamMetric(metrics, FailureStoreMetrics.METRIC_TOTAL, DATA_STREAM_NAME, bulkResponse.getItems().length);
            assertDataStreamMetric(
                metrics,
                FailureStoreMetrics.METRIC_REJECTED,
                DATA_STREAM_NAME,
                bulkResponse.getItems().length - successfullyStored
            );
            assertDataStreamMetric(metrics, FailureStoreMetrics.METRIC_FAILURE_STORE, DATA_STREAM_NAME, 0);
        }
    }

    private void createDataStreamWithFailureStore() throws IOException {
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            "template-incremental"
        );
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(DATA_STREAM_NAME + "*"))
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .template(Template.builder().mappings(new CompressedXContent("""
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
                    }""")).dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
                .build()
        );
        assertAcked(safeGet(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request)));

        final var createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            DATA_STREAM_NAME
        );
        assertAcked(safeGet(client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest)));
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

    private void assertDataStreamMetric(Map<String, List<Measurement>> metrics, String metric, String dataStreamName, int expectedValue) {
        List<Measurement> measurements = metrics.get(metric);
        assertThat(measurements, notNullValue());
        long totalValue = measurements.stream()
            .filter(m -> m.attributes().get("data_stream").equals(dataStreamName))
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(totalValue, equalTo((long) expectedValue));
    }

    private static IndexRequest indexRequest(String dataStreamName) {
        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        String value = "1";
        return new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
            .source(Strings.format("{\"%s\":\"%s\", \"count\": %s}", DEFAULT_TIMESTAMP_FIELD, time, value), XContentType.JSON);
    }

    protected static String findNodeOfPrimaryShard(String dataStreamName) {
        GetDataStreamAction.Request getDataStreamRequest = new GetDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { dataStreamName }
        );
        GetDataStreamAction.Response getDataStreamResponse = safeGet(client().execute(GetDataStreamAction.INSTANCE, getDataStreamRequest));
        assertThat(getDataStreamResponse.getDataStreams().size(), equalTo(1));
        DataStream dataStream = getDataStreamResponse.getDataStreams().getFirst().getDataStream();
        assertThat(dataStream.getName(), equalTo(DATA_STREAM_NAME));
        assertThat(dataStream.getIndices().size(), equalTo(1));
        String backingIndex = dataStream.getIndices().getFirst().getName();
        assertThat(backingIndex, backingIndexEqualTo(DATA_STREAM_NAME, 1));

        Index index = resolveIndex(backingIndex);
        int shardId = 0;
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
