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
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

/**
 * Logic-only tests for {@link ShardBatchIndexer#canUseBatchIndexing(BulkShardRequest, boolean)}. Separated out
 * to avoid creating temp directories for every test as happens in {@link ShardBatchIndexerTests}
 */
public class ShardBatchIndexerCanUseBatchTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

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

    private static BulkShardRequest requestWithBatch(BulkItemRequest[] items, EirfBatch batch) {
        BulkShardRequest request = new BulkShardRequest(SHARD_ID, RefreshPolicy.NONE, items);
        request.setBulkShardBatch(new BulkShardBatch(batch));
        return request;
    }

    private static BulkShardRequest requestWithoutBatch(BulkItemRequest[] items) {
        return new BulkShardRequest(SHARD_ID, RefreshPolicy.NONE, items);
    }

    public void testCanUseBatchIndexingAllIndex() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        try (EirfBatch batch = buildBatch(2)) {
            assertTrue(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), true));
        }
    }

    public void testCanUseBatchIndexingAllCreate() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1").create(true)),
            new BulkItemRequest(1, indexRequest("2").create(true)) };
        try (EirfBatch batch = buildBatch(2)) {
            assertTrue(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), true));
        }
    }

    public void testCanUseBatchIndexingMixedIndexAndCreate() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2").create(true)) };
        try (EirfBatch batch = buildBatch(2)) {
            assertTrue(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), true));
        }
    }

    public void testCanUseBatchIndexingContainsDelete() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, new DeleteRequest("index", "2")) };
        try (EirfBatch batch = buildBatch(2)) {
            assertFalse(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), true));
        }
    }

    public void testCanUseBatchIndexingContainsUpdate() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, new UpdateRequest("index", "2")) };
        try (EirfBatch batch = buildBatch(2)) {
            assertFalse(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), true));
        }
    }

    public void testCanUseBatchIndexingDisabled() {
        BulkItemRequest[] items = new BulkItemRequest[] { new BulkItemRequest(0, indexRequest("1")) };
        try (EirfBatch batch = buildBatch(1)) {
            assertFalse(ShardBatchIndexer.canUseBatchIndexing(requestWithBatch(items, batch), false));
        }
    }

    public void testCanUseBatchIndexingRequiresEirfBatch() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, indexRequest("1")),
            new BulkItemRequest(1, indexRequest("2")) };
        assertFalse(ShardBatchIndexer.canUseBatchIndexing(requestWithoutBatch(items), true));
    }
}
