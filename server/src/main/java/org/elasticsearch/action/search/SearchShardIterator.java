/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;

/**
 * Extension of {@link PlainShardIterator} used in the search api, which also holds the {@link OriginalIndices}
 * of the search request. Useful especially with cross cluster search, as each cluster has its own set of original indices.
 */
public final class SearchShardIterator extends PlainShardIterator {

    private final OriginalIndices originalIndices;
    private String clusterAlias;
    private boolean skip = false;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param shardId shard id of the group
     * @param shards  shards to iterate
     */
    public SearchShardIterator(String clusterAlias, ShardId shardId, List<ShardRouting> shards, OriginalIndices originalIndices) {
        super(shardId, shards);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
    }

    /**
     * Returns the original indices associated with this shard iterator, specifically with the cluster that this shard belongs to.
     */
    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Reset the iterator and mark it as skippable
     * @see #skip()
     */
    void resetAndSkip() {
        reset();
        skip = true;
    }

    /**
     * Returns <code>true</code> if the search execution should skip this shard since it can not match any documents given the query.
     */
    boolean skip() {
        return skip;
    }
}
