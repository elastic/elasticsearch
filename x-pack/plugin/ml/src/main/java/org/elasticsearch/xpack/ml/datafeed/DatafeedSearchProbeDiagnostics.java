/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.rest.RestStatus;

/**
 * Diagnoses security-related failures in validate-before-mint search probes.
 * Mirrors transforms {@code SourceAccessDiagnostics} for cluster (CCS/CPS) and top-level shard failures.
 */
final class DatafeedSearchProbeDiagnostics {

    private DatafeedSearchProbeDiagnostics() {}

    /**
     * Returns a human-readable error when a security failure is positively identified, or {@code null} otherwise.
     */
    static String diagnoseSearchProbeFailure(SearchResponse response) {
        String clusterSecurityError = findClusterSecurityFailure(response);
        if (clusterSecurityError != null) {
            return clusterSecurityError;
        }

        String shardSecurityError = findShardSecurityFailure(response);
        if (shardSecurityError != null) {
            return shardSecurityError;
        }

        return null;
    }

    private static String findClusterSecurityFailure(SearchResponse response) {
        SearchResponse.Clusters clusters = response.getClusters();
        if (clusters == null || clusters.getTotal() == 0) {
            return null;
        }

        for (String alias : clusters.getClusterAliases()) {
            SearchResponse.Cluster cluster = clusters.getCluster(alias);
            if (cluster == null) {
                continue;
            }
            if (cluster.getStatus() != SearchResponse.Cluster.Status.SKIPPED
                && cluster.getStatus() != SearchResponse.Cluster.Status.FAILED) {
                continue;
            }
            for (ShardSearchFailure failure : cluster.getFailures()) {
                if (isSecurityFailure(failure)) {
                    return "User lacks the required permissions to read datafeed indices on project [" + alias + "].";
                }
            }
        }

        return null;
    }

    private static String findShardSecurityFailure(SearchResponse response) {
        for (ShardSearchFailure failure : response.getShardFailures()) {
            if (isSecurityFailure(failure)) {
                String index = failure.index();
                if (index != null) {
                    return "User lacks the required permissions to read datafeed index [" + index + "].";
                }
                return "User lacks the required permissions to read from the datafeed indices.";
            }
        }
        return null;
    }

    private static boolean isSecurityFailure(ShardSearchFailure failure) {
        if (failure.getCause() instanceof ElasticsearchSecurityException) {
            return true;
        }
        return failure.status() == RestStatus.FORBIDDEN || failure.status() == RestStatus.UNAUTHORIZED;
    }
}
