/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.datastream.CreateDataStreamAction;
import org.elasticsearch.action.admin.indices.datastream.DeleteDataStreamAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DataStreamsStatsTests extends ESSingleNodeTestCase {

    private String timestampFieldName;
    private final Set<String> createdDataStreams = new HashSet<>();

    @Before
    public void createResourceNames() {
        this.timestampFieldName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
    }

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
        DataStreamStatsAction.Response stats = getDataStreamsStats();
        assertEquals(0, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(0, stats.getStreams());
        assertEquals(0, stats.getBackingIndices());
        assertEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(0, stats.getDataStreamStats().length);
    }

    public void testStatsEmptyDataStream() throws Exception {
        String dataStreamName = createDataStream();

        DataStreamStatsAction.Response stats = getDataStreamsStats();
        assertEquals(1, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getStreams());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreamStats().length);
        assertEquals(dataStreamName, stats.getDataStreamStats()[0].getDataStreamName());
        assertEquals(0L, stats.getDataStreamStats()[0].getMaxTimestamp());
        assertNotEquals(0L, stats.getDataStreamStats()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreamStats()[0].getStoreSize().getBytes());
    }

    public void testStatsExistingDataStream() throws Exception {
        String dataStreamName = createDataStream();
        long timestamp = createDocument(dataStreamName);

        DataStreamStatsAction.Response stats = getDataStreamsStats();
        assertEquals(1, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getStreams());
        assertEquals(1, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreamStats().length);
        assertEquals(dataStreamName, stats.getDataStreamStats()[0].getDataStreamName());
        assertEquals(timestamp, stats.getDataStreamStats()[0].getMaxTimestamp());
        assertNotEquals(0L, stats.getDataStreamStats()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreamStats()[0].getStoreSize().getBytes());
    }

    public void testStatsRolledDataStream() throws Exception {
        String dataStreamName = createDataStream();
        createDocument(dataStreamName);
        assertTrue(client().admin().indices().rolloverIndex(new RolloverRequest(dataStreamName, null)).get().isAcknowledged());
        long timestamp = createDocument(dataStreamName);

        DataStreamStatsAction.Response stats = getDataStreamsStats();
        assertEquals(2, stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(1, stats.getStreams());
        assertEquals(2, stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(1, stats.getDataStreamStats().length);
        assertEquals(dataStreamName, stats.getDataStreamStats()[0].getDataStreamName());
        assertEquals(timestamp, stats.getDataStreamStats()[0].getMaxTimestamp());
        assertNotEquals(0L, stats.getDataStreamStats()[0].getStoreSize().getBytes());
        assertEquals(stats.getTotalStoreSize().getBytes(), stats.getDataStreamStats()[0].getStoreSize().getBytes());
    }

    public void testStatsMultipleDataStreams() throws Exception {
        for (int dataStreamCount = 0; dataStreamCount < randomInt(5); dataStreamCount++) {
            createDataStream();
        }

        // Create a number of documents in each data stream
        Map<String, Long> maxTimestamps = new HashMap<>();
        for (String createdDataStream : createdDataStreams) {
            for (int documentCount = 0; documentCount < randomInt(10); documentCount++) {
                long ts = createDocument(createdDataStream);
                long maxTS = Math.max(maxTimestamps.getOrDefault(createdDataStream, 0L), ts);
                maxTimestamps.put(createdDataStream, maxTS);
            }
        }

        DataStreamStatsAction.Response stats = getDataStreamsStats();
        assertEquals(createdDataStreams.size(), stats.getSuccessfulShards());
        assertEquals(0, stats.getFailedShards());
        assertEquals(createdDataStreams.size(), stats.getStreams());
        assertEquals(createdDataStreams.size(), stats.getBackingIndices());
        assertNotEquals(0L, stats.getTotalStoreSize().getBytes());
        assertEquals(createdDataStreams.size(), stats.getDataStreamStats().length);
        for (DataStreamStatsAction.DataStreamStats dataStreamStats : stats.getDataStreamStats()) {
            long expectedMaxTS = maxTimestamps.get(dataStreamStats.getDataStreamName());
            assertEquals(1, dataStreamStats.getBackingIndices());
            assertEquals(expectedMaxTS, dataStreamStats.getMaxTimestamp());
            assertNotEquals(0L, dataStreamStats.getStoreSize().getBytes());
        }
    }

    private String createDataStream() throws Exception {
        String dataStreamName = randomAlphaOfLength(10).toLowerCase(Locale.getDefault());
        Template idxTemplate = new Template(null, new CompressedXContent("{\"properties\":{\"" +
            timestampFieldName + "\":{\"type\":\"date\"},\"data\":{\"type\":\"keyword\"}}}"), null);
        ComposableIndexTemplate template = new ComposableIndexTemplate(List.of(dataStreamName+"*"), idxTemplate, null, null, null, null,
            new ComposableIndexTemplate.DataStreamTemplate(timestampFieldName));
        assertTrue(client().execute(PutComposableIndexTemplateAction.INSTANCE, new PutComposableIndexTemplateAction.Request(dataStreamName +
            "_template").indexTemplate(template)).actionGet().isAcknowledged());
        assertTrue(client().execute(CreateDataStreamAction.INSTANCE, new CreateDataStreamAction.Request(dataStreamName)).get()
            .isAcknowledged());
        createdDataStreams.add(dataStreamName);
        return dataStreamName;
    }

    private long createDocument(String dataStreamName) throws Exception {
        // Get some randomized but reasonable timestamps on the data since not all of it is guaranteed to arrive in order.
        long timeSeed = System.currentTimeMillis();
        long timestamp = randomLongBetween(timeSeed - TimeUnit.HOURS.toMillis(5), timeSeed);
        client().index(new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE).source(JsonXContent.contentBuilder()
            .startObject().field(timestampFieldName, timestamp).field("data", randomAlphaOfLength(25)).endObject())).get();
        client().admin().indices().refresh(new RefreshRequest(".ds-" + dataStreamName + "*")
            .indicesOptions(IndicesOptions.lenientExpandOpenHidden())).get();
        return timestamp;
    }

    private DataStreamStatsAction.Response getDataStreamsStats() throws Exception {
        return client().execute(DataStreamStatsAction.INSTANCE, new DataStreamStatsAction.Request()).get();
    }

    private void deleteDataStream(String dataStreamName) throws InterruptedException, java.util.concurrent.ExecutionException {
        assertTrue(client().execute(DeleteDataStreamAction.INSTANCE, new DeleteDataStreamAction.Request(dataStreamName)).get()
            .isAcknowledged());
        assertTrue(client().execute(DeleteComposableIndexTemplateAction.INSTANCE, new DeleteComposableIndexTemplateAction.Request(
            dataStreamName + "_template")).actionGet().isAcknowledged());
    }
}
