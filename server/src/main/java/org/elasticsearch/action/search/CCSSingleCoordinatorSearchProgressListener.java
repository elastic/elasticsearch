/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Use this progress listener for cross-cluster searches where a single
 * coordinator is used for all clusters (minimize_roundtrips=false).
 * It updates state in the SearchResponse.Clusters object as the search
 * progresses so that the metadata required for the _clusters/details
 * section in the SearchResponse is accurate.
 */
public class CCSSingleCoordinatorSearchProgressListener extends SearchProgressListener {

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
        assert clusters.isCcsMinimizeRoundtrips() == false : "minimize_roundtrips must be false to use this SearchListener";

        this.clusters = clusters;
        this.timeProvider = timeProvider;

        // Partition by clusterAlias and get counts
        Map<String, Integer> skippedByClusterAlias = partitionCountsByClusterAlias(skipped);
        // the 'shards' list does not include the shards in the 'skipped' list, so combine counts from both to get total
        Map<String, Integer> totalByClusterAlias = partitionCountsByClusterAlias(shards);
        skippedByClusterAlias.forEach((cluster, count) -> totalByClusterAlias.merge(cluster, count, Integer::sum));

        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();

            clusters.swapCluster(clusterAlias, (k, v) -> {
                assert v.getTotalShards() == null : "total shards should not be set on a Cluster before onListShards";

                int totalCount = entry.getValue();
                int skippedCount = skippedByClusterAlias.getOrDefault(k, 0);
                TimeValue took = null;

                SearchResponse.Cluster.Status status = v.getStatus();
                assert status == SearchResponse.Cluster.Status.RUNNING : "should have RUNNING status during onListShards but has " + status;

                // if all shards are marked as skipped, the search is done - mark as SUCCESSFUL
                if (skippedCount == totalCount) {
                    took = new TimeValue(timeProvider.buildTookInMillis());
                    status = SearchResponse.Cluster.Status.SUCCESSFUL;
                }
                return new SearchResponse.Cluster.Builder(v).setStatus(status)
                    .setTotalShards(totalCount)
                    .setSuccessfulShards(skippedCount)
                    .setSkippedShards(skippedCount)
                    .setFailedShards(0)
                    .setTook(took)
                    .setTimedOut(false)
                    .build();
            });
        }
    }

    /**
     * Executed when a shard returns a query result.
     *
     * @param shardIndex  The index of the shard in the list provided by {@link SearchProgressListener#onListShards}.
     * @param queryResult QuerySearchResult holding the result for a SearchShardTarget
     */
    @Override
    public void onQueryResult(int shardIndex, QuerySearchResult queryResult) {
        // we only need to update Cluster state here if the search has timed out, since:
        // 1) this is the only callback that gets search timedOut info and
        // 2) the onFinalReduce will get all these shards again so the final accounting can be done there
        // for queries that did not time out
        if (queryResult.searchTimedOut() && clusters.hasClusterObjects()) {
            SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
            String clusterAlias = shardTarget.getClusterAlias();
            if (clusterAlias == null) {
                clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }

            clusters.swapCluster(clusterAlias, (k, v) -> {
                if (v.isTimedOut()) {
                    return v; // cluster has already been marked as timed out on some other shard
                }
                if (v.getStatus() == SearchResponse.Cluster.Status.FAILED || v.getStatus() == SearchResponse.Cluster.Status.SKIPPED) {
                    return v; // safety check to make sure it hasn't hit a terminal FAILED/SKIPPED state where timeouts don't matter
                }
                return new SearchResponse.Cluster.Builder(v).setTimedOut(true).build();
            });
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
        if (clusters.hasClusterObjects() == false) {
            return;
        }
        String clusterAlias = shardTarget.getClusterAlias();
        if (clusterAlias == null) {
            clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }

        clusters.swapCluster(clusterAlias, (k, v) -> {
            TimeValue took;
            SearchResponse.Cluster.Status status;
            int numFailedShards = v.getFailedShards() == null ? 1 : v.getFailedShards() + 1;

            assert v.getTotalShards() != null : "total shards should be set on the Cluster but not for " + k;
            if (v.getTotalShards() == numFailedShards) {
                took = null;
                if (v.isSkipUnavailable()) {
                    status = SearchResponse.Cluster.Status.SKIPPED;
                } else {
                    status = SearchResponse.Cluster.Status.FAILED;
                    // TODO in the fail-fast ticket, should we throw an exception here to stop the search?
                }
            } else if (v.getTotalShards() == numFailedShards + v.getSuccessfulShards()) {
                status = SearchResponse.Cluster.Status.PARTIAL;
                took = new TimeValue(timeProvider.buildTookInMillis());
            } else {
                took = null;
                status = SearchResponse.Cluster.Status.RUNNING;
            }
            return new SearchResponse.Cluster.Builder(v).setStatus(status)
                .setFailedShards(numFailedShards)
                .setFailures(CollectionUtils.appendToCopy(v.getFailures(), new ShardSearchFailure(e, shardTarget)))
                .setTook(took)
                .build();
        });
    }

    /**
     * Executed when a partial reduce is created. The number of partial reduce can be controlled via
     * {@link SearchRequest#setBatchedReduceSize(int)}.
     *
     * Note that onPartialReduce and onFinalReduce are called with cumulative data so far.
     * For example if the first call to onPartialReduce has 5 shards, the second call will
     * have those same 5 shards plus the new batch. onFinalReduce will see all those
     * shards one final time.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The partial result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    @Override
    public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        Map<String, Integer> totalByClusterAlias = partitionCountsByClusterAlias(shards);

        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();
            int successfulCount = entry.getValue();

            clusters.swapCluster(clusterAlias, (k, v) -> {
                SearchResponse.Cluster.Status status = v.getStatus();
                if (status != SearchResponse.Cluster.Status.RUNNING) {
                    // don't swap in a new Cluster if the final state has already been set
                    return v;
                }
                TimeValue took = null;
                int successfulShards = successfulCount + v.getSkippedShards();
                if (successfulShards == v.getTotalShards()) {
                    status = v.isTimedOut() ? SearchResponse.Cluster.Status.PARTIAL : SearchResponse.Cluster.Status.SUCCESSFUL;
                    took = new TimeValue(timeProvider.buildTookInMillis());
                } else if (successfulShards + v.getFailedShards() == v.getTotalShards()) {
                    status = SearchResponse.Cluster.Status.PARTIAL;
                    took = new TimeValue(timeProvider.buildTookInMillis());
                }
                return new SearchResponse.Cluster.Builder(v).setStatus(status).setSuccessfulShards(successfulShards).setTook(took).build();
            });
        }
    }

    /**
     * Executed once when the final reduce is created.
     *
     * Note that his will see all the shards, even if they have been passed to the onPartialReduce
     * method already.
     *
     * @param shards The list of shards that are part of this reduce.
     * @param totalHits The total number of hits in this reduce.
     * @param aggs The final result for aggregations.
     * @param reducePhase The version number for this reduce.
     */
    @Override
    public void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        if (clusters.hasClusterObjects() == false) {
            return;
        }

        Map<String, Integer> totalByClusterAlias = partitionCountsByClusterAlias(shards);

        for (Map.Entry<String, Integer> entry : totalByClusterAlias.entrySet()) {
            String clusterAlias = entry.getKey();
            int successfulCount = entry.getValue();

            clusters.swapCluster(clusterAlias, (k, v) -> {
                SearchResponse.Cluster.Status status = v.getStatus();
                if (status != SearchResponse.Cluster.Status.RUNNING) {
                    // don't swap in a new Cluster if the final state has already been set
                    return v;
                }
                TimeValue took = new TimeValue(timeProvider.buildTookInMillis());
                int successfulShards = successfulCount + v.getSkippedShards();
                assert successfulShards + v.getFailedShards() == v.getTotalShards()
                    : "successfulShards("
                        + successfulShards
                        + ") + failedShards("
                        + v.getFailedShards()
                        + ") != totalShards ("
                        + v.getTotalShards()
                        + ')';
                if (v.isTimedOut() || successfulShards < v.getTotalShards()) {
                    status = SearchResponse.Cluster.Status.PARTIAL;
                } else {
                    assert successfulShards == v.getTotalShards()
                        : "successful (" + successfulShards + ") should equal total(" + v.getTotalShards() + ") if get here";
                    status = SearchResponse.Cluster.Status.SUCCESSFUL;
                }
                return new SearchResponse.Cluster.Builder(v).setStatus(status).setSuccessfulShards(successfulShards).setTook(took).build();
            });
        }
    }

    /**
     * Executed when a shard returns a rank feature result.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     */
    @Override
    public void onRankFeatureResult(int shardIndex) {}

    /**
     * Executed when a shard reports a rank feature failure.
     *
     * @param shardIndex The index of the shard in the list provided by {@link SearchProgressListener#onListShards})}.
     * @param shardTarget The last shard target that thrown an exception.
     * @param exc The cause of the failure.
     */
    @Override
    public void onRankFeatureFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {}

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

    private Map<String, Integer> partitionCountsByClusterAlias(List<SearchShard> shards) {
        final Map<String, Integer> res = new HashMap<>();
        for (SearchShard shard : shards) {
            res.merge(Objects.requireNonNullElse(shard.clusterAlias(), RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY), 1, Integer::sum);
        }
        return res;
    }
}
