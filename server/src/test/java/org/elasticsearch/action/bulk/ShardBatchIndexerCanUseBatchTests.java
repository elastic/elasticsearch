/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Logic-only tests for {@link ShardBatchIndexer#canUseBatchIndexing(BulkShardRequest)}. Separated out
 * to avoid creating temp directories for every test as happens in {@link ShardBatchIndexerTests}
 */
public class ShardBatchIndexerCanUseBatchTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

    private static ShardBatchIndexer indexer(boolean enabled) {
        if (enabled) {
            assumeTrue("batch indexing requires the batch_indexing feature flag", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        }
        Settings s = Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), enabled).build();
        return new ShardBatchIndexer(
            new BatchIndexingEnabled(new ClusterSettings(s, Set.of(BatchIndexingEnabled.BATCH_INDEXING))),
            BytesRefRecycler.NON_RECYCLING_INSTANCE
        );
    }

    private static IndexRequest indexRequest(String id) {
        return new IndexRequest("index").id(id).source(XContentType.JSON, "title", "hello", "count", 42, "tag", "bulk");
    }

    private static EscfBatch buildBatch(int numDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            sources.add(new BytesArray("{\"title\":\"doc-" + i + "\",\"count\":" + i + ",\"tag\":\"batch\"}"));
        }
        return EscfEncoder.encode(sources, XContentType.JSON);
    }

    private static BulkShardRequest requestWithBatch(BulkItemRequest[] items, SourceBatch batch) {
        BulkShardRequest request = new BulkShardRequest(SHARD_ID, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.NONE, items);
        request.setBulkShardBatch(new BulkShardBatch(batch));
        return request;
    }

    private static BulkShardRequest requestWithoutBatch(BulkItemRequest[] items) {
        return new BulkShardRequest(SHARD_ID, SplitShardCountSummary.IRRELEVANT, RefreshPolicy.NONE, items);
    }

    public void testCanUseBatchIndexingAllIndex() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        try (EscfBatch batch = buildBatch(2)) {
            assertTrue(indexer(true).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingAllCreate() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1").create(true)),
            new BulkItemRequest(1, indexRequest("2").create(true)) };
        try (EscfBatch batch = buildBatch(2)) {
            assertTrue(indexer(true).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingMixedIndexAndCreate() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2").create(true)) };
        try (EscfBatch batch = buildBatch(2)) {
            assertTrue(indexer(true).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingContainsDelete() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, new DeleteRequest("index", "2")) };
        try (EscfBatch batch = buildBatch(2)) {
            assertFalse(indexer(true).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingContainsUpdate() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, new UpdateRequest("index", "2")) };
        try (EscfBatch batch = buildBatch(2)) {
            assertFalse(indexer(true).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingDisabled() throws IOException {
        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EscfBatch batch = buildBatch(1)) {
            assertFalse(indexer(false).canUseBatchIndexing(requestWithBatch(items, batch)));
        }
    }

    public void testCanUseBatchIndexingRequiresBatch() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        assertFalse(indexer(true).canUseBatchIndexing(requestWithoutBatch(items)));
    }

    public void testDynamicSettingUpdate() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(BatchIndexingEnabled.BATCH_INDEXING));
        BatchIndexingEnabled gate = new BatchIndexingEnabled(clusterSettings);
        ShardBatchIndexer batchIndexer = new ShardBatchIndexer(gate, BytesRefRecycler.NON_RECYCLING_INSTANCE);

        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EscfBatch batch = buildBatch(1)) {
            BulkShardRequest request = requestWithBatch(items, batch);

            assertFalse(batchIndexer.canUseBatchIndexing(request));

            if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
                clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
                assertTrue(batchIndexer.canUseBatchIndexing(request));

                clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), false).build());
                assertFalse(batchIndexer.canUseBatchIndexing(request));
            } else {
                IllegalArgumentException ex = expectThrows(
                    IllegalArgumentException.class,
                    () -> clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build())
                );
                assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("batch_indexing feature flag"));
            }
        }
    }
}
