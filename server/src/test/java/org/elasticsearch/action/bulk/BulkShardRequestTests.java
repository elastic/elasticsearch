/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.MockBytesRefRecycler;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BulkShardRequestTests extends ESTestCase {
    public void testToString() {
        String index = randomSimpleString(random(), 10);
        int count = between(2, 100);
        final ShardId shardId = new ShardId(index, "ignored", 0);
        BulkShardRequest r = new BulkShardRequest(
            shardId,
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            new BulkItemRequest[count]
        );
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0]", r.getDescription());

        r = new BulkShardRequest(shardId, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.IMMEDIATE, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests and a refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[IMMEDIATE]", r.getDescription());

        r = new BulkShardRequest(shardId, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count]);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());

        r = new BulkShardRequest(shardId, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count], true);
        assertEquals(
            "BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh, simulated",
            r.toString()
        );
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());

        r = new BulkShardRequest(shardId, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.WAIT_UNTIL, new BulkItemRequest[count], false);
        assertEquals("BulkShardRequest [" + shardId + "] containing [" + count + "] requests blocking until refresh", r.toString());
        assertEquals("requests[" + count + "], index[" + index + "][0], refresh[WAIT_UNTIL]", r.getDescription());
    }

    public void testSerialization() throws IOException {
        // Note: BulkShardRequest does not implement equals or hashCode, so we can't test serialization in the usual way for a Writable
        BulkShardRequest bulkShardRequest = randomBulkShardRequest();
        BulkShardRequest copy = copyWriteable(bulkShardRequest, null, BulkShardRequest::new);
        assertThat(bulkShardRequest.items().length, equalTo(copy.items().length));
        assertThat(bulkShardRequest.isSimulated(), equalTo(copy.isSimulated()));
        assertThat(bulkShardRequest.getRefreshPolicy(), equalTo(copy.getRefreshPolicy()));
    }

    /**
     * The request owns its batch: the batch stays open while any reference to the request remains, and the last release closes it.
     * Closing it a second time would trip the batch's own assertion, so the release happens exactly once.
     */
    public void testLastDecRefClosesBatch() throws IOException {
        try (MockBytesRefRecycler recycler = new MockBytesRefRecycler()) {
            BulkShardRequest request = requestWithBatch(recycler);
            SourceBatch batch = request.getBulkShardBatch().getBatch();
            int extraRefs = between(1, 3);
            for (int i = 0; i < extraRefs; i++) {
                request.incRef();
            }
            for (int i = 0; i < extraRefs; i++) {
                assertFalse(request.decRef());
                assertThat("batch pages are held while the request is referenced", recycler.activePageCount(), greaterThan(0));
                assertNotNull(batch.row(0));
            }

            assertTrue(request.decRef());
            assertFalse(request.hasReferences());
            assertThat(recycler.activePageCount(), equalTo(0));
            expectThrows(IllegalStateException.class, () -> batch.row(0));
        }
    }

    /** A request without a batch, like most requests that never reach their last release, has nothing to close when released. */
    public void testDecRefWithoutBatch() {
        BulkShardRequest request = randomBulkShardRequest();
        assertNull(request.getBulkShardBatch());
        assertTrue(request.decRef());
        assertFalse(request.hasReferences());
    }

    /** Replacing or detaching the attached batch closes the batch that the request owned. */
    public void testReplacingBatchClosesPreviousBatch() throws IOException {
        try (MockBytesRefRecycler recycler = new MockBytesRefRecycler()) {
            BulkShardRequest request = requestWithBatch(recycler);
            SourceBatch previous = request.getBulkShardBatch().getBatch();
            // a caller that detaches the batch must first stop the items from referencing its rows
            for (BulkItemRequest item : request.items()) {
                ((IndexRequest) item.request()).indexSource().ensureInlineSource();
            }
            BulkShardBatch replacement = randomBoolean() ? null : new BulkShardBatch(encodeBatch(recycler, request.items().length));
            request.setBulkShardBatch(replacement);
            expectThrows(IllegalStateException.class, () -> previous.row(0));
            if (replacement == null) {
                assertThat(recycler.activePageCount(), equalTo(0));
            } else {
                assertThat(recycler.activePageCount(), greaterThan(0));
            }

            assertTrue(request.decRef());
            assertThat(recycler.activePageCount(), equalTo(0));
        }
    }

    /**
     * A request deserialized on a primary or replica owns the batch it read and closes it on its last release. Like the transport layer,
     * the test reads it from recycled pages and recycles them before using the batch, which works because the batch's bytes are a heap
     * copy rather than a slice of the inbound buffer, so closing the batch has nothing to release and only makes later reads fail.
     */
    public void testDeserializedRequestClosesBatchOnLastDecRef() throws IOException {
        try (MockBytesRefRecycler recycler = new MockBytesRefRecycler()) {
            BulkShardRequest original = requestWithBatch(recycler);
            BulkShardRequest copy;
            try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler)) {
                out.setTransportVersion(TransportVersion.current());
                original.writeTo(out);
                try (ReleasableBytesReference bytes = out.moveToBytesReference(); StreamInput in = bytes.streamInput()) {
                    in.setTransportVersion(TransportVersion.current());
                    copy = new BulkShardRequest(in);
                }
            }

            SourceBatch originalBatch = original.getBulkShardBatch().getBatch();
            SourceBatch copiedBatch = copy.getBulkShardBatch().getBatch();
            assertNotSame(originalBatch, copiedBatch);
            for (BulkItemRequest item : copy.items()) {
                IndexSource source = ((IndexRequest) item.request()).indexSource();
                assertTrue(source.hasSourceRow());
                assertEquals(rowAsMap(originalBatch, source.rowIndex()), rowAsMap(copiedBatch, source.rowIndex()));
            }
            // the sender's batch is its own
            assertTrue(original.decRef());
            assertThat(recycler.activePageCount(), equalTo(0));
            assertNotNull(copiedBatch.row(0));

            assertTrue(copy.decRef());
            assertFalse(copy.hasReferences());
            expectThrows(IllegalStateException.class, () -> copiedBatch.row(0));
        }
    }

    /** A request whose items are the rows of a batch on {@code recycler}, as the coordinating node builds it for batch indexing. */
    private static BulkShardRequest requestWithBatch(MockBytesRefRecycler recycler) throws IOException {
        int docCount = between(1, 10);
        EscfBatch batch = encodeBatch(recycler, docCount);
        BulkItemRequest[] items = new BulkItemRequest[docCount];
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest("index").id("id_" + i);
            indexRequest.indexSource().setSourceRow(batch, i, XContentType.JSON);
            items[i] = new BulkItemRequest(i, indexRequest);
        }
        BulkShardRequest request = new BulkShardRequest(
            new ShardId("index", "_na_", 0),
            SplitShardCountSummary.fromInt(randomInt(1024)),
            RefreshPolicy.NONE,
            items
        );
        request.setBulkShardBatch(new BulkShardBatch(batch));
        return request;
    }

    private static EscfBatch encodeBatch(MockBytesRefRecycler recycler, int docCount) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (int i = 0; i < docCount; i++) {
                String doc = "{\"i\":" + i + ",\"s\":\"" + randomAlphaOfLength(between(0, 64)) + "\"}";
                encoder.addDocument(new BytesArray(doc), XContentType.JSON, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private static Map<String, Object> rowAsMap(SourceBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    protected BulkShardRequest randomBulkShardRequest() {
        String indexName = randomAlphaOfLength(100);
        ShardId shardId = new ShardId(indexName, randomAlphaOfLength(50), randomInt());
        RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(0, 100)];
        for (int i = 0; i < items.length; i++) {
            final DocWriteRequest<?> request = switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> new IndexRequest(indexName).id("id_" + i);
                case CREATE -> new IndexRequest(indexName).id("id_" + i).create(true);
                case UPDATE -> new UpdateRequest(indexName, "id_" + i);
                case DELETE -> new DeleteRequest(indexName, "id_" + i);
            };
            items[i] = new BulkItemRequest(i, request);
        }
        return new BulkShardRequest(shardId, SplitShardCountSummary.fromInt(randomInt(1024)), refreshPolicy, items, randomBoolean());
    }
}
