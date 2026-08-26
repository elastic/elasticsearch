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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

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

    /**
     * COLUMNAR mode with synthetic recovery source. Synthetic recovery source satisfies
     * {@link org.elasticsearch.index.mapper.SourceFieldMapper#supportsColumnarParse} (only a size
     * estimate is stored, not the full source), while keeping recovery source enabled so that
     * {@code RecoverySourceHandler} can open a changes snapshot for replica recovery.
     */
    private static final Settings COLUMNAR_SETTINGS = indexSettings(IndexVersion.current(), 1, 0).put(
        IndexSettings.MODE.getKey(),
        IndexMode.COLUMNAR.getName()
    ).put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true).build();

    /** Keyword-only mapping used for columnar batch tests. */
    private static final String COLUMNAR_KEYWORD_MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "title": { "type": "keyword" },
            "tag":   { "type": "keyword" }
          }
        }""";

    private final ShardBatchIndexer shardBatchIndexer = new ShardBatchIndexer(
        new BatchIndexingEnabled(ClusterSettings.createBuiltInClusterSettings()),
        BytesRefRecycler.NON_RECYCLING_INSTANCE
    );

    private final List<IndexShard> trackedShards = new ArrayList<>();

    @After
    public void closeTrackedShards() {
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
        return new IndexRequest("index").id(id);
    }

    private static SourceBatch buildBatch(int numDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            sources.add(new BytesArray("{\"title\":\"doc-" + i + "\",\"count\":" + i + ",\"tag\":\"batch\"}"));
        }
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    private IndexShard newColumnarPrimaryShard() throws IOException {
        return newColumnarPrimaryShardWithMapping(COLUMNAR_KEYWORD_MAPPING);
    }

    private IndexShard newColumnarPrimaryShardWithMapping(String mapping) throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index").putMapping(mapping).settings(COLUMNAR_SETTINGS).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        trackedShards.add(shard);
        recoverShardFromStore(shard);
        return shard;
    }

    private IndexShard newColumnarReplicaShard() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index")
            .putMapping(COLUMNAR_KEYWORD_MAPPING)
            .settings(COLUMNAR_SETTINGS)
            .primaryTerm(0, 1)
            .build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), false, "n1", metadata, null);
        trackedShards.add(shard);
        recoveryEmptyReplica(shard, true);
        return shard;
    }

    /** Index request whose source only contains fields present in {@link #COLUMNAR_KEYWORD_MAPPING}. */
    private static IndexRequest columnarIndexRequest(String id) {
        return new IndexRequest("index").id(id).source(XContentType.JSON, "title", "hello", "tag", "bulk");
    }

    private static SourceBatch buildColumnarBatch(int numDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            sources.add(new BytesArray("{\"title\":\"doc-" + i + "\",\"tag\":\"batch\"}"));
        }
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    // TODO(columnar): add testBatchIndexOnPrimaryStoredSource once stored source mode enables the
    // columnar path; add tests for long, date, double, etc. once those mappers support columnar.
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
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            assertTrue(context.hasMoreOperationsToExecute());
        }

        closeShards(shard);
    }

    public void testBatchIndexOnPrimarySingleDoc() throws Exception {
        IndexShard shard = newColumnarPrimaryShard();

        BulkItemRequest[] items = { new BulkItemRequest(0, columnarIndexRequest("1")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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
        IndexShard shard = newColumnarPrimaryShard();

        int numDocs = randomIntBetween(2, 20);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, columnarIndexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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
        IndexShard shard = newColumnarPrimaryShard();

        int numDocs = ShardBatchIndexer.BATCH_CHUNK_SIZE + randomIntBetween(1, 32);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, columnarIndexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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

    public void testBatchIndexOnPrimaryDuplicateUids() throws Exception {
        IndexShard shard = newColumnarPrimaryShard();

        BulkItemRequest[] items = {
            new BulkItemRequest(0, columnarIndexRequest("same-id")),
            new BulkItemRequest(1, columnarIndexRequest("same-id")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
        }

        // Both operations complete: duplicate UIDs are split across sub-batches, so the second
        // overwrites the first rather than triggering a fallback to the sequential path.
        assertFalse(context.hasMoreOperationsToExecute());
        assertFalse(items[0].getPrimaryResponse().isFailed());
        assertFalse(items[1].getPrimaryResponse().isFailed());

        shard.refresh("test");
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
        }

        closeShards(shard);
    }

    public void testBatchIndexOnReplicaSingleDoc() throws Exception {
        IndexShard shard = newColumnarPrimaryShard();

        BulkItemRequest[] items = { new BulkItemRequest(0, columnarIndexRequest("1")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(1)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
            assertFalse(context.hasMoreOperationsToExecute());

            IndexShard replica = newColumnarReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = shardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            assertThat(result.processedItems(), equalTo(1));
            assertThat(result.location(), notNullValue());

            closeShards(shard, replica);
        }
    }

    public void testBatchIndexOnReplicaMultipleDocs() throws Exception {
        IndexShard shard = newColumnarPrimaryShard();

        int numDocs = randomIntBetween(2, 20);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, columnarIndexRequest(Integer.toString(i)));
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(numDocs)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();
            assertFalse(context.hasMoreOperationsToExecute());

            IndexShard replica = newColumnarReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = shardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
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
        IndexShard shard = newColumnarPrimaryShard();

        BulkItemRequest[] items = { new BulkItemRequest(0, columnarIndexRequest("1")), new BulkItemRequest(1, columnarIndexRequest("2")) };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildColumnarBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Override the second item's response with a failure.
            items[1].setPrimaryResponse(
                BulkItemResponse.failure(
                    1,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure("index", "2", new RuntimeException())
                )
            );

            IndexShard replica = newColumnarReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = shardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            assertThat(result.processedItems(), equalTo(1));

            closeShards(shard, replica);
        }
    }

    public void testBatchIndexWithNestedFields() throws Exception {
        // Nested keyword-only object fields resolve to leaf paths (host.name, host.ip, message).
        IndexShard shard = newColumnarPrimaryShardWithMapping(NESTED_MAPPING);

        int numDocs = randomIntBetween(2, 10);
        BulkItemRequest[] items = new BulkItemRequest[numDocs];
        List<BytesReference> sources = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            items[i] = new BulkItemRequest(i, new IndexRequest("index").id(Integer.toString(i)));
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

        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                shard.shardId(),
                SplitShardCountSummary.IRRELEVANT,
                RefreshPolicy.NONE,
                items
            );
            BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
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
        BulkShardRequest bulkShardRequest = new BulkShardRequest(
            shard.shardId(),
            SplitShardCountSummary.IRRELEVANT,
            RefreshPolicy.NONE,
            items
        );
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);

        try (SourceBatch batch = buildBatch(2)) {
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            shardBatchIndexer.performBatchIndexOnPrimary(items, batch, context, future);
            future.actionGet();

            // Override the first item to be a NOOP
            UpdateResponse noopResponse = new UpdateResponse(shard.shardId(), "1", 0, 1, 1, DocWriteResponse.Result.NOOP);
            items[0].setPrimaryResponse(BulkItemResponse.success(0, DocWriteRequest.OpType.INDEX, noopResponse));

            IndexShard replica = newMappedReplicaShard();

            ShardBatchIndexer.ReplicaBatchResult result = shardBatchIndexer.performBatchIndexOnReplica(items, batch, replica);
            // A batch is written as a single contiguous Translog.IndexBatch record, so a NOOP ends the batch where it
            // is encountered. With the NOOP at the leading item, nothing is batched and the NOOP plus the remaining
            // items are left to the serial fallback path (which resumes from processedItems).
            assertThat(result.processedItems(), equalTo(0));

            closeShards(shard, replica);
        }
    }
}
