/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse.Clusters;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * A listener that allows to track progress of the {@link SearchAction}.
 */
public abstract class SearchProgressListener {
    private static final Logger logger = LogManager.getLogger(SearchProgressListener.class);

    public static final SearchProgressListener NOOP = new SearchProgressListener() {
    };

    private List<SearchShard> shards;

    /**
     * Executed when shards are ready to be queried.
     *
     * @param shards The list of shards to query.
     * @param skippedShards The list of skipped shards.
     * @param clusters The statistics for remote clusters included in the search.
     * @param fetchPhase <code>true</code> if the search needs a fetch phase, <code>false</code> otherwise.
     **/
    protected void onListShards(List<SearchShard> shards, List<SearchShard> skippedShards, Clusters clusters, boolean fetchPhase) {}

    /**
     * Executed when a shard returns a query result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards} )}.
     */
    protected void onQueryResult(int shardIndex) {}

    /**
     * Executed when a shard reports a query failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     * @param shardTarget The last shard target that thrown an exception.
     * @param exc The cause of the failure.
     */
    protected void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    /**
     * Executed when a partial reduce is created. The number of partial reduce can be controlled via
     * {@link SearchRequest#setBatchedReduceSize(int)}.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The partial result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    protected void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {}

    /**
     * Executed once when the final reduce is created.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The final result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {}

    /**
     * Executed when a shard returns a fetch result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     */
    protected void onFetchResult(int shardIndex) {}

    /**
     * Executed when a shard reports a fetch failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     * @param shardTarget The last shard target that thrown an exception.
     * @param exc The cause of the failure.
     */
    protected void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

    final void notifyListShards(List<SearchShard> shards, List<SearchShard> skippedShards, Clusters clusters, boolean fetchPhase) {
        this.shards = shards;
        try {
            onListShards(shards, skippedShards, clusters, fetchPhase);
        } catch (Exception e) {
            logger.warn("Failed to execute progress listener on list shards", e);
        }
    }

    final void notifyQueryResult(int shardIndex) {
        try {
            onQueryResult(shardIndex);
        } catch (Exception e) {
            logger.warn(() -> "[" + shards.get(shardIndex) + "] Failed to execute progress listener on query result", e);
        }
    }

    final void notifyQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        try {
            onQueryFailure(shardIndex, shardTarget, exc);
        } catch (Exception e) {
            logger.warn(() -> "[" + shards.get(shardIndex) + "] Failed to execute progress listener on query failure", e);
        }
    }

    final void notifyPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        try {
            onPartialReduce(shards, totalHits, aggs, reducePhase);
        } catch (Exception e) {
            logger.warn("Failed to execute progress listener on partial reduce", e);
        }
    }

    protected final void notifyFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        try {
            onFinalReduce(shards, totalHits, aggs, reducePhase);
        } catch (Exception e) {
            logger.warn("Failed to execute progress listener on reduce", e);
        }
    }

    final void notifyFetchResult(int shardIndex) {
        try {
            onFetchResult(shardIndex);
        } catch (Exception e) {
            logger.warn(() -> "[" + shards.get(shardIndex) + "] Failed to execute progress listener on fetch result", e);
        }
    }

    final void notifyFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
        try {
            onFetchFailure(shardIndex, shardTarget, exc);
        } catch (Exception e) {
            logger.warn(() -> "[" + shards.get(shardIndex) + "] Failed to execute progress listener on fetch failure", e);
        }
    }

    static List<SearchShard> buildSearchShards(List<? extends SearchPhaseResult> results) {
        return results.stream()
            .filter(Objects::nonNull)
            .map(SearchPhaseResult::getSearchShardTarget)
            .map(e -> new SearchShard(e.getClusterAlias(), e.getShardId()))
            .toList();
    }

    static List<SearchShard> buildSearchShards(GroupShardsIterator<SearchShardIterator> its) {
        return StreamSupport.stream(its.spliterator(), false).map(e -> new SearchShard(e.getClusterAlias(), e.shardId())).toList();
    }
}
