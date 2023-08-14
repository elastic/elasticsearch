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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Use this progress listener for cross-cluster searches where a single
 * coordinator is used for all clusters (minimize_roundtrips=false).
 * It updates state in the SearchResponse.Clusters object as the search
 * progresses so that the metadata required for the _clusters/details
 * section in the SearchResponse is accurate.
 */
public class CCSSingleCoordinatorSearchProgressListener extends SearchProgressListener {

    private static final Logger logger = LogManager.getLogger(CCSSingleCoordinatorSearchProgressListener.class);
    private SearchResponse.Clusters clusters;
    private TransportSearchAction.SearchTimeProvider timeProvider;

    /**
     * Executed when shards are ready to be queried (after can-match)
     *
     * @param shards The list of shards to query.
     * @param skipped The list of skipped shards.
     * @param clusters The statistics for remote clusters included in the search.
     * @param fetchPhase <code>true</code> if the search needs a fetch phase, <code>false</code> otherwise.
     **/
    @Override
    public void onListShards(
        List<SearchShard> shards,
        List<SearchShard> skipped,
        SearchResponse.Clusters clusters,
        boolean fetchPhase,
        TransportSearchAction.SearchTimeProvider timeProvider
    ) {
        logger.warn("XXX SSS CCSProgListener onListShards: shards size: {}; shards: {}", shards.size(), shards);
        logger.warn("XXX SSS CCSProgListener onListShards: skipped size: {}; skipped: {}", skipped.size(), skipped);
        logger.warn("XXX SSS CCSProgListener onListShards: clusters: {}", clusters);
        logger.warn("XXX SSS CCSProgListener onListShards: fetchPhase: {}", fetchPhase);
        assert clusters.isCcsMinimizeRoundtrips() == false : "minimize_roundtrips must be false to use this SearchListener";

        this.clusters = clusters;
        this.timeProvider = timeProvider;

        // Partition by clusterAlias and get counts
        Map<String, Integer> totalByClusterAlias = Stream.concat(shards.stream(), skipped.stream()).collect(Collectors.groupingBy(shard -> {
            String clusterAlias = shard.clusterAlias();
            return clusterAlias != null ? clusterAlias : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }, Collectors.reducing(0, e -> 1, Integer::sum)));
        Map<String, Integer> skippedByClusterAlias = skipped.stream().collect(Collectors.groupingBy(shard -> {
            String clusterAlias = shard.clusterAlias();
            return clusterAlias != null ? clusterAlias : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }, Collectors.reducing(0, e -> 1, Integer::sum)));

        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();
            AtomicReference<SearchResponse.Cluster> clusterRef = clusters.getCluster(clusterAlias);
            assert clusterRef.get().getTotalShards() == null : "total shards should not be set on a Cluster before onListShards";

            Integer totalCount = entry.getValue();
            Integer skippedCount = skippedByClusterAlias.get(clusterAlias);
            if (skippedCount == null) {
                skippedCount = 0;
            }
            TimeValue took = null;

            System.err.println("XXX SSS CCSProgListener onListShards totalCount for " + clusterAlias + " = " + totalCount);
            System.err.println("XXX SSS CCSProgListener onListShards skippedCount for " + clusterAlias + " = " + skippedCount);

            boolean swapped;
            do {
                SearchResponse.Cluster curr = clusterRef.get();
                SearchResponse.Cluster.Status status = curr.getStatus();
                assert status == SearchResponse.Cluster.Status.RUNNING : "should have RUNNING status during onListShards but has " + status;
                if (skippedCount != null && skippedCount == totalCount) {
                    took = new TimeValue(timeProvider.buildTookInMillis());
                    status = SearchResponse.Cluster.Status.SUCCESSFUL;
                }
                SearchResponse.Cluster updated = new SearchResponse.Cluster(
                    curr.getClusterAlias(),
                    curr.getIndexExpression(),
                    curr.isSkipUnavailable(),
                    status,
                    totalCount,
                    skippedCount,
                    skippedCount,
                    0,
                    curr.getFailures(),
                    took,
                    false
                );
                swapped = clusterRef.compareAndSet(curr, updated);
                assert swapped : "compareAndSet in onListShards should never fail due to race condition";
                logger.warn("XXX SSS CCSProgListener onListShards DEBUG 66 swapped: {} ;; new cluster: {}", swapped, updated);
            } while (swapped == false);
        }
    }

    /**
     * Executed when a shard returns a query result.
     *
     * @param shardIndex  The index of the shard in the list provided by {@link SearchProgressListener#onListShards} )}.
     * @param queryResult QuerySearchResult holding the result for a SearchShardTarget
     */
    @Override
    public void onQueryResult(int shardIndex, QuerySearchResult queryResult) {
        // logger.warn("XXX __Q__ CCSProgListener onQueryResult for : {}", queryResult.getSearchShardTarget());
        if (queryResult.searchTimedOut() && clusters.hasClusterObjects()) {
            logger.warn("XXX __Q__ CCSProgListener onQueryResult TIMED_OUT on target: {}", queryResult.getSearchShardTarget());
            SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
            String clusterAlias = shardTarget.getClusterAlias();
            if (clusterAlias == null) {
                clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }
            AtomicReference<SearchResponse.Cluster> clusterRef = clusters.getCluster(clusterAlias);
            boolean swapped;
            do {
                SearchResponse.Cluster curr = clusterRef.get();
                if (curr.isTimedOut()) {
                    break; // already marked as timed out on some other shard
                }
                if (curr.getStatus() == SearchResponse.Cluster.Status.FAILED || curr.getStatus() == SearchResponse.Cluster.Status.SKIPPED) {
                    break; // safety check to make sure it hasn't hit a terminal FAILED/SKIPPED state where timeouts don't matter
                }
                SearchResponse.Cluster updated = new SearchResponse.Cluster(
                    curr.getClusterAlias(),
                    curr.getIndexExpression(),
                    curr.isSkipUnavailable(),
                    curr.getStatus(),
                    curr.getTotalShards(),
                    curr.getSuccessfulShards(),
                    curr.getSkippedShards(),
                    curr.getFailedShards(),
                    curr.getFailures(),
                    curr.getTook(),
                    true
                );
                swapped = clusterRef.compareAndSet(curr, updated);
                logger.warn("XXX __Q__ onQueryResult swapped: {} ;; new cluster: {}", swapped, updated);
            } while (swapped == false);
        }
    }

    /**
     * Executed when a shard reports a query failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     * @param shardTarget The last shard target that thrown an exception.
     * @param e The cause of the failure.
     */
    @Override
    public void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
        // logger.warn("XXX __L__ CCSProgListener onQueryFailure shardTarget: {}; exc: {}", shardTarget, e);

        if (clusters.hasClusterObjects() == false) {
            return;
        }
        String clusterAlias = shardTarget.getClusterAlias();
        if (clusterAlias == null) {
            clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }
        AtomicReference<SearchResponse.Cluster> clusterRef = clusters.getCluster(clusterAlias);
        boolean swapped;
        do {
            TimeValue took = null;
            SearchResponse.Cluster curr = clusterRef.get();
            SearchResponse.Cluster.Status status = SearchResponse.Cluster.Status.RUNNING;
            int numFailedShards = curr.getFailedShards() == null ? 1 : curr.getFailedShards() + 1;

            assert curr.getTotalShards() != null : "total shards should be set on the Cluster but not for " + clusterAlias;
            if (curr.getTotalShards() == numFailedShards) {
                if (curr.isSkipUnavailable()) {
                    logger.warn("XXX __L__ onQueryFailure SETTING SKIPPED status bcs total=failed_shards; skipun=true !");
                    status = SearchResponse.Cluster.Status.SKIPPED;
                } else {
                    logger.warn("XXX __L__ onQueryFailure SETTING FAILED status bcs total=failed_shards; skipun=false !");
                    status = SearchResponse.Cluster.Status.FAILED;
                    // TODO in the fail-fast ticket, should we throw an exception here to stop the search?
                }
            } else if (curr.getTotalShards() == numFailedShards + curr.getSuccessfulShards()) {
                status = SearchResponse.Cluster.Status.PARTIAL;
                took = new TimeValue(timeProvider.buildTookInMillis());
            }

            List<ShardSearchFailure> failures = new ArrayList<>();
            curr.getFailures().forEach(failures::add);
            failures.add(new ShardSearchFailure(e, shardTarget));
            SearchResponse.Cluster updated = new SearchResponse.Cluster(
                curr.getClusterAlias(),
                curr.getIndexExpression(),
                curr.isSkipUnavailable(),
                status,
                curr.getTotalShards(),
                curr.getSuccessfulShards(),
                curr.getSkippedShards(),
                numFailedShards,
                failures,
                took,
                curr.isTimedOut()
            );
            swapped = clusterRef.compareAndSet(curr, updated);
            logger.warn("XXX __L__ onQueryFailure swapped: {} ;;;; new cluster: {}", swapped, updated);
        } while (swapped == false);
    }

    /**
     * Executed when a partial reduce is created. The number of partial reduce can be controlled via
     * {@link SearchRequest#setBatchedReduceSize(int)}.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The partial result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    @Override
    public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        // logger.warn(
        // "XXX __P__ CCSProgListener onPartialReduce. shards {}, totalHits: {}; reducePhase: {}",
        // shards,
        // totalHits.value,
        // reducePhase
        // );

        Map<String, Integer> totalByClusterAlias = shards.stream().collect(Collectors.groupingBy(shard -> {
            String clusterAlias = shard.clusterAlias();
            return clusterAlias != null ? clusterAlias : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }, Collectors.reducing(0, e -> 1, Integer::sum)));

        System.err.println("XXX __P__ CCSProgListener onPartialReduce: " + totalByClusterAlias);
        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();
            int successfulCount = entry.getValue().intValue();

            AtomicReference<SearchResponse.Cluster> clusterRef = clusters.getCluster(clusterAlias);
            boolean swapped;
            do {
                SearchResponse.Cluster curr = clusterRef.get();
                SearchResponse.Cluster.Status status = curr.getStatus();
                if (status != SearchResponse.Cluster.Status.RUNNING) {
                    // don't swap in a new Cluster if the final state has already been set
                    break;
                }
                TimeValue took = null;
                int successfulShards = successfulCount + curr.getSkippedShards();
                if (successfulShards == curr.getTotalShards()) {
                    status = curr.isTimedOut() ? SearchResponse.Cluster.Status.PARTIAL : SearchResponse.Cluster.Status.SUCCESSFUL;
                    took = new TimeValue(timeProvider.buildTookInMillis());
                } else if (successfulShards + curr.getFailedShards() == curr.getTotalShards()) {
                    status = SearchResponse.Cluster.Status.PARTIAL;
                    took = new TimeValue(timeProvider.buildTookInMillis());
                }
                SearchResponse.Cluster updated = new SearchResponse.Cluster(
                    curr.getClusterAlias(),
                    curr.getIndexExpression(),
                    curr.isSkipUnavailable(),
                    status,
                    curr.getTotalShards(),
                    successfulShards,
                    curr.getSkippedShards(),
                    curr.getFailedShards(),
                    curr.getFailures(),
                    took,
                    curr.isTimedOut()
                );
                swapped = clusterRef.compareAndSet(curr, updated);
                logger.warn("XXX __P__ DEBUG 33 onPartialReduce swapped: {} ;; new cluster: {}", updated);
            } while (swapped == false);
        }
    }

    /**
     * Executed once when the final reduce is created.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The final result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    @Override
    public void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        // logger.warn(
        // "XXX __L__ CCSProgListener onFinalReduce. shards {}, totalHits: {}; reducePhase: {}",
        // shards,
        // totalHits.value,
        // reducePhase
        // );

        if (clusters.hasClusterObjects() == false) {
            return;
        }

        Map<String, Integer> totalByClusterAlias = shards.stream().collect(Collectors.groupingBy(shard -> {
            String clusterAlias = shard.clusterAlias();
            return clusterAlias != null ? clusterAlias : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }, Collectors.reducing(0, e -> 1, Integer::sum)));

        System.err.println("XXX __L__ CCSProgListener onFinalReduce: " + totalByClusterAlias);
        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();
            int successfulCount = entry.getValue().intValue();

            AtomicReference<SearchResponse.Cluster> clusterRef = clusters.getCluster(clusterAlias);
            boolean swapped;
            do {
                SearchResponse.Cluster curr = clusterRef.get();
                SearchResponse.Cluster.Status status = curr.getStatus();
                if (status != SearchResponse.Cluster.Status.RUNNING) {
                    // don't swap in a new Cluster if the final state has already been set
                    break;
                }
                TimeValue took = new TimeValue(timeProvider.buildTookInMillis());
                int successfulShards = successfulCount + curr.getSkippedShards();
                assert successfulShards + curr.getFailedShards() == curr.getTotalShards()
                    : "successfulShards("
                        + successfulShards
                        + ") + failedShards("
                        + curr.getFailedShards()
                        + ") != totalShards ("
                        + curr.getTotalShards()
                        + ')';
                if (curr.isTimedOut() || successfulShards < curr.getTotalShards()) {
                    status = SearchResponse.Cluster.Status.PARTIAL;
                } else {
                    assert successfulShards == curr.getTotalShards()
                        : "successful (" + successfulShards + ") should equal total(" + curr.getTotalShards() + ") if get here";
                    status = SearchResponse.Cluster.Status.SUCCESSFUL;
                }
                SearchResponse.Cluster updated = new SearchResponse.Cluster(
                    curr.getClusterAlias(),
                    curr.getIndexExpression(),
                    curr.isSkipUnavailable(),
                    status,
                    curr.getTotalShards(),
                    successfulShards,
                    curr.getSkippedShards(),
                    curr.getFailedShards(),
                    curr.getFailures(),
                    took,
                    curr.isTimedOut()
                );
                swapped = clusterRef.compareAndSet(curr, updated);
                logger.warn("XXX __L__ DEBUG 44 onFinalReduce swapped: {} ;; new cluster: {}", updated);
            } while (swapped == false);
        }
    }

    /**
     * Executed when a shard returns a fetch result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     */
    @Override
    public void onFetchResult(int shardIndex) {}

    /**
     * Executed when a shard reports a fetch failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     * @param shardTarget The last shard target that thrown an exception.
     * @param exc The cause of the failure.
     */
    @Override
    public void onFetchFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}
}
