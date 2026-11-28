/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public final class SearchHitUtils {

    public static String qualifiedIndex(SearchHit hit) {
        return buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex());
    }

    /** Deduplicates failures after the query client applies the appropriate CPS or CCS policy. */
    public static void addShardFailures(Map<String, ShardSearchFailure> shardFailures, List<ShardSearchFailure> failures) {
        for (ShardSearchFailure failure : failures) {
            shardFailures.put(failure.toString(), failure);
        }
    }

    /** Includes skipped projects that are not already represented by top-level shard failures. */
    public static List<ShardSearchFailure> shardFailures(SearchResponse response) {
        List<ShardSearchFailure> skipped = skippedClusterFailures(response.getClusters(), response.getShardFailures());
        if (skipped.isEmpty()) {
            return response.getShardFailures();
        }
        List<ShardSearchFailure> failures = new ArrayList<>(response.getShardFailures());
        failures.addAll(skipped);
        return failures;
    }

    /** Retains the project alias even when the failure occurred before any shard was selected. */
    public static List<ShardSearchFailure> skippedClusterFailures(SearchResponse.Clusters clusters) {
        return skippedClusterFailures(clusters, List.of());
    }

    private static List<ShardSearchFailure> skippedClusterFailures(
        SearchResponse.Clusters clusters,
        List<ShardSearchFailure> reportedFailures
    ) {
        if (clusters.getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED) == 0) {
            return List.of();
        }
        Set<String> reportedClusters = new HashSet<>();
        for (ShardSearchFailure failure : reportedFailures) {
            if (failure.shard() != null) {
                reportedClusters.add(failure.shard().getClusterAlias());
            }
        }
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (String alias : clusters.getClusterAliases()) {
            SearchResponse.Cluster cluster = clusters.getCluster(alias);
            if (cluster.getStatus() != SearchResponse.Cluster.Status.SKIPPED || reportedClusters.contains(alias)) {
                continue;
            }
            SearchShardTarget target = new SearchShardTarget(null, new ShardId(cluster.getIndexExpression(), "_na_", -1), alias);
            if (cluster.getFailures().isEmpty()) {
                failures.add(
                    new ShardSearchFailure(
                        new ElasticsearchStatusException("project [" + alias + "] was skipped", RestStatus.SERVICE_UNAVAILABLE),
                        target
                    )
                );
            } else {
                for (ShardSearchFailure failure : cluster.getFailures()) {
                    failures.add(
                        new ShardSearchFailure(
                            new ElasticsearchStatusException("project [" + alias + "] was skipped", failure.status(), failure.getCause()),
                            target
                        )
                    );
                }
            }
        }
        return failures;
    }
}
