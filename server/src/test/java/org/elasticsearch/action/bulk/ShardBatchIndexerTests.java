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
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ShardBatchIndexerTests extends IndexShardTestCase {

    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "title":   { "type": "keyword" },
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

    @After
    public void closeTrackedShards() throws Exception {
        for (IndexShard shard : trackedShards) {
            try {
                closeShardNoCheck(shard);
            } catch (Exception e) {
                // Shard may already have been closed by the test body — swallow so we still clean up the rest.
            }
        }
        trackedShards.clear();
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

    private static SourceBatch buildBatch(int numDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            sources.add(new BytesArray("{\"title\":\"doc-" + i + "\",\"count\":" + i + ",\"tag\":\"batch\"}"));
        }
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    // TODO(columnar): add tests for the following scenarios once FieldMapper#supportsColumnarParse()
    // is implemented for real field mappers so that ShardBatchMapper.mapColumnBatch does not fall back:
    // - testBatchIndexOnPrimarySingleDoc
    // - testBatchIndexOnPrimaryMultipleDocs
    // - testBatchIndexOnPrimaryChunking (batch exceeds BATCH_CHUNK_SIZE)
    // - testBatchIndexOnPrimaryDuplicateUids (duplicate UIDs split across sub-batches)
    // - testBatchIndexOnPrimaryStoredSource
    // - testBatchIndexOnReplicaSingleDoc
    // - testBatchIndexOnReplicaMultipleDocs
    // - testBatchIndexOnReplicaFailedPrimaryResponse
    // - testBatchIndexWithNestedFields
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

        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        assertFalse(context.hasMoreOperationsToExecute());

        closeShards(shard);
    }

    private static final String NESTED_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "host": {
              "properties": {
                "name":   { "type": "keyword" },
                "ip":     { "type": "keyword" }
              }
            },
            "message": { "type": "keyword" }
          }
        }""";

    private static final String ARRAY_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "tags":    { "type": "keyword" },
            "scores":  { "type": "integer" },
            "message": { "type": "keyword" }
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
            "message": { "type": "keyword" }
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

    public void testBatchIndexWithArrayFieldsFallsBack() throws Exception {
        // Array-valued columns are outside the v1 batch support matrix (each leaf column is
        // expected to be a scalar). The batch path must return early via fallback rather than
        // throwing, leaving the items for the sequential path to process.
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

        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                shard.shardId(),
                SplitShardCountSummary.IRRELEVANT,
                RefreshPolicy.NONE,
                items
            );
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Fallback contract: no per-item responses produced, items remain queued for the
            // caller's sequential path.
            assertTrue(context.hasMoreOperationsToExecute());
        }

        closeShards(shard);
    }

    public void testBatchIndexWithNestedFieldsAndArraysFallsBack() throws Exception {
        // Same as above but nested under an object mapper.
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

        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                shard.shardId(),
                SplitShardCountSummary.IRRELEVANT,
                RefreshPolicy.NONE,
                items
            );
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            assertTrue(context.hasMoreOperationsToExecute());
        }

        closeShards(shard);
    }

    public void testBatchIndexOnReplicaNoopResponse() throws Exception {
        IndexShard shard = newMappedPrimaryShard();

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            ShardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Override the first item to be a NOOP
            UpdateResponse noopResponse = new UpdateResponse(shard.shardId(), "1", 0, 1, 1, DocWriteResponse.Result.NOOP);
            items[0].setPrimaryResponse(BulkItemResponse.success(0, DocWriteRequest.OpType.INDEX, noopResponse));

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = ShardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            // A batch is written as a single contiguous Translog.IndexBatch record, so a NOOP ends the batch where it
            // is encountered. With the NOOP at the leading item, nothing is batched and the NOOP plus the remaining
            // items are left to the serial fallback path (which resumes from processedItems).
            assertThat(result.processedItems(), equalTo(0));

            closeShards(shard, replica);
        }
    }
}
