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
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.max;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DataStreamsStatsTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataStreamsPlugin.class);
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
        }
    }

    private String createDataStream() throws Exception {
        return createDataStream(false);
    }

    private String createDataStream(boolean hidden) throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template idxTemplate = new Template(null, new CompressedXContent("""
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
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStreamName)
            )
        );
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
        assertAcked(
            client().execute(
                DeleteDataStreamAction.INSTANCE,
                new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName })
            )
        );
        assertAcked(
            client().execute(
                TransportDeleteComposableIndexTemplateAction.TYPE,
                new TransportDeleteComposableIndexTemplateAction.Request(dataStreamName + "_template")
            )
        );
    }
}
