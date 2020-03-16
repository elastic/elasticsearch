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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A listener that allows to track progress of the {@link SearchAction}.
 */
abstract class SearchProgressListener {
    private static final Logger logger = LogManager.getLogger(SearchProgressListener.class);

    public static final SearchProgressListener NOOP = new SearchProgressListener() {};

    private List<SearchShard> shards;

    /**
     * Executed when shards are ready to be queried.
     *
     * @param shards The list of shards to query.
     * @param fetchPhase <code>true</code> if the search needs a fetch phase, <code>false</code> otherwise.
     **/
    public void onListShards(List<SearchShard> shards, boolean fetchPhase) {}

    /**
     * Executed when a shard returns a query result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards(List, boolean)} )}.
     */
    public void onQueryResult(int shardIndex) {}

    /**
     * Executed when a shard reports a query failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards(List, boolean)})}.
     * @param exc The cause of the failure.
     */
    public void onQueryFailure(int shardIndex, Exception exc) {}

    /**
     * Executed when a partial reduce is created. The number of partial reduce can be controlled via
     * {@link SearchRequest#setBatchedReduceSize(int)}.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The partial result for aggregations.
     * @param version The version number for this reduce.
     */
    public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int version) {}

    /**
     * Executed once when the final reduce is created.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The final result for aggregations.
     */
    public void onReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs) {}

    /**
     * Executed when a shard returns a fetch result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards(List, boolean)})}.
     */
    public void onFetchResult(int shardIndex) {}

    /**
     * Executed when a shard reports a fetch failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards(List, boolean)})}.
     * @param exc The cause of the failure.
     */
    public void onFetchFailure(int shardIndex, Exception exc) {}

    final void notifyListShards(List<SearchShard> shards, boolean fetchPhase) {
        this.shards = shards;
        try {
            onListShards(shards, fetchPhase);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on list shards"), e);
        }
    }

    final void notifyQueryResult(int shardIndex) {
        try {
            onQueryResult(shardIndex);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] Failed to execute progress listener on query result",
                shards.get(shardIndex)), e);
        }
    }

    final void notifyQueryFailure(int shardIndex, Exception exc) {
        try {
            onQueryFailure(shardIndex, exc);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] Failed to execute progress listener on query failure",
                shards.get(shardIndex)), e);
        }
    }

    final void notifyPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int version) {
        try {
            onPartialReduce(shards, totalHits, aggs, version);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on partial reduce"), e);
        }
    }

    final void notifyReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs) {
        try {
            onReduce(shards, totalHits, aggs);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on reduce"), e);
        }
    }

    final void notifyFetchResult(int shardIndex) {
        try {
            onFetchResult(shardIndex);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] Failed to execute progress listener on fetch result",
                shards.get(shardIndex)), e);
        }
    }

    final void notifyFetchFailure(int shardIndex, Exception exc) {
        try {
            onFetchFailure(shardIndex, exc);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] Failed to execute progress listener on fetch failure",
                shards.get(shardIndex)), e);
        }
    }

    final List<SearchShard> searchShards(List<? extends SearchPhaseResult> results) {
        return results.stream()
            .filter(Objects::nonNull)
            .map(SearchPhaseResult::getSearchShardTarget)
            .map(e -> new SearchShard(e.getClusterAlias(), e.getShardId()))
            .collect(Collectors.toUnmodifiableList());
    }

    final List<SearchShard> searchShards(SearchShardTarget[] results) {
        return Arrays.stream(results)
            .filter(Objects::nonNull)
            .map(e -> new SearchShard(e.getClusterAlias(), e.getShardId()))
            .collect(Collectors.toUnmodifiableList());
    }

    final List<SearchShard> searchShards(GroupShardsIterator<SearchShardIterator> its) {
        return StreamSupport.stream(its.spliterator(), false)
            .map(e -> new SearchShard(e.getClusterAlias(), e.shardId()))
            .collect(Collectors.toUnmodifiableList());
    }
}
