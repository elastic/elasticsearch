/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class ShardBatchIndexerTests extends IndexShardTestCase {

    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "title":   { "type": "text" },
            "count":   { "type": "integer" },
            "tag":     { "type": "keyword" }
          }
        }""";

    private static final Settings SYNTHETIC_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        "index.mapping.source.mode",
        "synthetic"
    ).build();

    private static final Settings STORED_SOURCE_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).build();

    private final List<IndexShard> trackedShards = new ArrayList<>();

    @Override
    public void tearDown() throws Exception {
        try {
            for (IndexShard shard : trackedShards) {
                try {
                    closeShardNoCheck(shard);
                } catch (Exception e) {
                    // Shard may already have been closed by the test body — swallow so we still clean up the rest.
                }
            }
        } finally {
            trackedShards.clear();
            super.tearDown();
        }
    }

    private IndexShard newMappedPrimaryShard() throws IOException {
        return newMappedPrimaryShard(SYNTHETIC_SOURCE_SETTINGS);
    }

    private IndexShard newMappedPrimaryShard(Settings settings) throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index").putMapping(MAPPING).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        trackedShards.add(shard);
        recoverShardFromStore(shard);
        return shard;
    }

    private IndexShard newMappedReplicaShard() throws IOException {
        return newMappedReplicaShard(SYNTHETIC_SOURCE_SETTINGS);
    }

    private IndexShard newMappedReplicaShard(Settings settings) throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index").putMapping(MAPPING).settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), false, "n1", metadata, null);
        trackedShards.add(shard);
        recoveryEmptyReplica(shard, true);
        return shard;
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id).source(XContentType.JSON, "title", "hello", "count", 42, "tag", "bulk");
    }

    /** Builds an EirfBatch with the given number of docs, each with title/count/tag fields. */
    private static EirfBatch buildBatch(int numDocs) {
        EirfRowBuilder builder = new EirfRowBuilder();
        for (int i = 0; i < numDocs; i++) {
            builder.startDocument();
            builder.setString("title", "doc-" + i);
            builder.setInt("count", i);
            builder.setString("tag", "batch");
            builder.endDocument();
        }
        return builder.build();
    }

    public void testBatchIndexOnPrimarySingleDoc() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());
        BulkItemResponse response = items[0].getPrimaryResponse();
        assertThat(response, notNullValue());
        assertFalse(response.isFailed());
        assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(response.getResponse().getSeqNo(), greaterThanOrEqualTo(0L));

        closeShards(shard);
    }

    public void testBatchIndexOnPrimaryMultipleDocs() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        int numDocs = randomIntBetween(2, 20);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());
        for (int i = 0; i < numDocs; i++) {
            BulkItemResponse response = items[i].getPrimaryResponse();
            assertThat(response, notNullValue());
            assertFalse(response.isFailed());
            assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
        }

        closeShards(shard);
    }

    public void testBatchIndexOnPrimaryChunking() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        int numDocs = ShardBatchIndexer.BATCH_CHUNK_SIZE + randomIntBetween(1, 32);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());
        for (int i = 0; i < numDocs; i++) {
            BulkItemResponse response = items[i].getPrimaryResponse();
            assertThat("doc " + i + " should have a response", response, notNullValue());
            assertFalse("doc " + i + " should not have failed", response.isFailed());
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
        }

        closeShards(shard);
    }

    public void testBatchIndexOnPrimaryDuplicateUidsTriggersEarlyReturn() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("same-id")),
            new BulkItemRequest(1, indexRequest("same-id")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        // Early return means context still has items to process (falls back to sequential)
        assertTrue(context.hasMoreOperationsToExecute());

        closeShards(shard);
    }

    public void testBatchIndexOnPrimaryAbortedItem() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };

        items[0].setPrimaryResponse(
            BulkItemResponse.failure(
                0,
                DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure("index", "1", new RuntimeException("aborted"), true)
            )
        );

        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());

        closeShards(shard);
    }

    public void testBatchIndexOnPrimaryStoredSource() throws Exception {
        IndexShard shard = newMappedPrimaryShard(STORED_SOURCE_SETTINGS);

        int numDocs = randomIntBetween(2, 10);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());
        for (int i = 0; i < numDocs; i++) {
            BulkItemResponse response = items[i].getPrimaryResponse();
            assertThat(response, notNullValue());
            assertFalse(response.isFailed());
            assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
        }

        closeShards(shard);
    }

    public void testBatchIndexOnReplicaSingleDoc() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
            assertFalse(context.hasMoreOperationsToExecute());

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = ShardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            assertThat(result.processedItems(), equalTo(1));
            assertThat(result.location(), notNullValue());

            closeShards(shard, replica);
        }
    }

    public void testBatchIndexOnReplicaMultipleDocs() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        int numDocs = randomIntBetween(2, 20);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
            assertFalse(context.hasMoreOperationsToExecute());

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = ShardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            assertThat(result.processedItems(), equalTo(numDocs));
            assertThat(result.location(), notNullValue());

            replica.refresh("test");
            try (Engine.Searcher searcher = replica.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
            }

            closeShards(shard, replica);
        }
    }

    public void testBatchIndexOnReplicaFailedPrimaryResponse() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Override the second item's response with a failure
            items[1].setPrimaryResponse(
                BulkItemResponse.failure(
                    1,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure("index", "2", new RuntimeException())
                )
            );

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = ShardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            assertThat(result.processedItems(), equalTo(1));

            closeShards(shard, replica);
        }
    }

    private static final String NESTED_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "host": {
              "properties": {
                "name":   { "type": "keyword" },
                "ip":     { "type": "ip" }
              }
            },
            "message": { "type": "text" }
          }
        }""";

    private static final String ARRAY_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "tags":    { "type": "keyword" },
            "scores":  { "type": "integer" },
            "message": { "type": "text" }
          }
        }""";

    private static final String NESTED_ARRAY_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "host": {
              "properties": {
                "name": { "type": "keyword" },
                "tags": { "type": "keyword" }
              }
            },
            "message": { "type": "text" }
          }
        }""";

    private IndexShard newPrimaryShardWithMapping(String mapping) throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index")
            .putMapping(mapping)
            .settings(SYNTHETIC_SOURCE_SETTINGS)
            .primaryTerm(0, 1)
            .build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        trackedShards.add(shard);
        recoverShardFromStore(shard);
        return shard;
    }

    public void testBatchIndexWithNestedFields() throws Exception {
        IndexShard shard = newPrimaryShardWithMapping(NESTED_MAPPING);

        int numDocs = randomIntBetween(2, 10);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        List<BytesReference> sources = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject();
                b.startObject("host");
                b.field("name", "host-" + i);
                b.field("ip", "10.0.0." + i);
                b.endObject();
                b.field("message", "hello from " + i);
                b.endObject();
                sources.add(BytesReference.bytes(b));
            }
        }

        try (EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            assertFalse(context.hasMoreOperationsToExecute());
            for (int i = 0; i < numDocs; i++) {
                BulkItemResponse response = items[i].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse("doc " + i + " should not have failed", response.isFailed());
                assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            }

            shard.refresh("test");
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
            }
        }

        closeShards(shard);
    }

    public void testBatchIndexWithArrayFields() throws Exception {
        IndexShard shard = newPrimaryShardWithMapping(ARRAY_MAPPING);

        int numDocs = randomIntBetween(2, 10);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        List<BytesReference> sources = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject();
                b.array("tags", "tag-" + i + "-a", "tag-" + i + "-b");
                b.array("scores", i * 10, i * 20);
                b.field("message", "doc-" + i);
                b.endObject();
                sources.add(BytesReference.bytes(b));
            }
        }

        try (EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            assertFalse(context.hasMoreOperationsToExecute());
            for (int i = 0; i < numDocs; i++) {
                BulkItemResponse response = items[i].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse("doc " + i + " should not have failed", response.isFailed());
                assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            }

            shard.refresh("test");
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
            }
        }

        closeShards(shard);
    }

    public void testBatchIndexWithNestedFieldsAndArrays() throws Exception {
        IndexShard shard = newPrimaryShardWithMapping(NESTED_ARRAY_MAPPING);

        int numDocs = randomIntBetween(2, 10);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        List<BytesReference> sources = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, indexRequest(Integer.toString(i)));
            try (XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent())) {
                b.startObject();
                b.startObject("host");
                b.field("name", "host-" + i);
                b.array("tags", "env-" + i, "prod");
                b.endObject();
                b.field("message", "combined test " + i);
                b.endObject();
                sources.add(BytesReference.bytes(b));
            }
        }

        try (EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            assertFalse(context.hasMoreOperationsToExecute());
            for (int i = 0; i < numDocs; i++) {
                BulkItemResponse response = items[i].getPrimaryResponse();
                assertThat(response, notNullValue());
                assertFalse("doc " + i + " should not have failed", response.isFailed());
                assertThat(response.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            }

            shard.refresh("test");
            try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs(), equalTo(numDocs));
            }
        }

        closeShards(shard);
    }

    public void testBatchIndexOnReplicaNoopResponse() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, items);
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (EirfBatch batch = buildBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Override the first item to be a NOOP
            UpdateResponse noopResponse = new UpdateResponse(shard.shardId(), "1", 0, 1, 1, DocWriteResponse.Result.NOOP);
            items[0].setPrimaryResponse(BulkItemResponse.success(0, DocWriteRequest.OpType.INDEX, noopResponse));

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = ShardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            // Both items processed: first was skipped (NOOP), second was indexed
            assertThat(result.processedItems(), equalTo(2));

            closeShards(shard, replica);
        }
    }
}
