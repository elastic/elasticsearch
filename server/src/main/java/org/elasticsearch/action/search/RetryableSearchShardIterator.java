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

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.Countable;
import org.elasticsearch.common.util.PlainIterator;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

class RetryableSearchShardIterator implements Comparable<RetryableSearchShardIterator>, Countable {
    private final OriginalIndices originalIndices;
    private final String clusterAlias;
    private final ShardId shardId;
    private boolean skip = false;

    private final PlainIterator<ShardRouting> targetNodesIterator;

    // TODO, use SearchShardIterator if possible
    RetryableSearchShardIterator(@Nullable String clusterAlias,
                                        ShardId shardId,
                                        List<ShardRouting> shardRoutings,
                                        OriginalIndices originalIndices) {
        this.shardId = shardId;
        this.targetNodesIterator = new PlainIterator<>(shardRoutings);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
    }

    ShardId shardId() {
        return shardId;
    }

    @Nullable
    String getClusterAlias() {
        return clusterAlias;
    }

    SearchShardTarget nextOrNull() {
        final ShardRouting shardRouting = targetNodesIterator.nextOrNull();
        if (shardRouting != null) {
            return new SearchShardTarget(shardRouting.currentNodeId(), shardId, clusterAlias, originalIndices, shardRouting);
        }
        return null;
    }

    RetryableSearchShardIterator withUpdatedRoutings(List<ShardRouting> updatedRoutings) {
        return new RetryableSearchShardIterator(clusterAlias, shardId, updatedRoutings, originalIndices);
    }

    void reset() {
        targetNodesIterator.reset();
    }

    boolean skip() {
        return skip;
    }

    @Override
    public int size() {
        return targetNodesIterator.size();
    }

    @Override
    public int compareTo(RetryableSearchShardIterator o) {
        return Comparator.comparing(RetryableSearchShardIterator::shardId)
            .thenComparing(RetryableSearchShardIterator::getClusterAlias, Comparator.nullsFirst(String::compareTo))
            .compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RetryableSearchShardIterator that = (RetryableSearchShardIterator) o;
        return skip == that.skip &&
            Objects.equals(originalIndices, that.originalIndices) &&
            Objects.equals(clusterAlias, that.clusterAlias) &&
            Objects.equals(shardId, that.shardId) &&
            Objects.equals(targetNodesIterator, that.targetNodesIterator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalIndices, clusterAlias, shardId, skip, targetNodesIterator);
    }
}
