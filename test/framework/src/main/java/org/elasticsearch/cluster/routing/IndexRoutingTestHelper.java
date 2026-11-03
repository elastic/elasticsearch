/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;

/**
 * Helpers for tests that need documents to land on specific shards.
 * <p>
 * A document's shard is {@code murmur3(_id) % numShards}, and auto-generated ids are not derived
 * from the test seed, so a test that indexes a random number of documents into a multi-shard index
 * cannot assume that every shard receives one. With two shards and three documents, all three land
 * on the same shard about 25% of the time, leaving the other shard empty. Tests that assert
 * per-shard behaviour must choose ids deliberately.
 */
public final class IndexRoutingTestHelper {

    private IndexRoutingTestHelper() {}

    /**
     * Returns a document id that routes to {@code shardId}, by sampling random ids until one hashes
     * to that shard. Expected cost is {@code numShards} attempts per id.
     * <p>
     * Obtain {@code indexRouting} from the index's current metadata, or from
     * {@code ReshardingTestHelpers#postSplitRouting} for a post-split shard count:
     * <pre>{@code
     * var metadata = clusterService.state().metadata().indexMetadata(index);
     * var routing = IndexRouting.fromIndexMetadata(metadata);
     * for (int shard = 0; shard < metadata.getNumberOfShards(); shard++) {
     *     var id = makeIdThatRoutesToShard(routing, shard);
     *     bulk.add(new IndexRequest(indexName).id(id).source("field", "value"));
     * }
     * }</pre>
     */
    public static String makeIdThatRoutesToShard(IndexRouting indexRouting, int shardId) {
        return makeIdThatRoutesToShard(indexRouting, shardId, "");
    }

    /**
     * As {@link #makeIdThatRoutesToShard(IndexRouting, int)}, prepending {@code prefix} to the id
     * so it can be identified in assertions. The prefix is part of the id and therefore part of the
     * hash, so it labels the document without steering its routing.
     */
    public static String makeIdThatRoutesToShard(IndexRouting indexRouting, int shardId, String prefix) {
        while (true) {
            String documentId = prefix + ESTestCase.randomAlphaOfLength(5);
            int routedShard = indexRouting.indexShard(new IndexRequest().id(documentId).routing(null));
            if (routedShard == shardId) {
                return documentId;
            }
        }
    }
}
