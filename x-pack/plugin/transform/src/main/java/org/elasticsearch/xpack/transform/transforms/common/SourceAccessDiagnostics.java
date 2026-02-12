/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.rest.RestStatus;

/**
 * Diagnoses the cause of an empty or failed search response against transform source indices.
 * Inspects CCS cluster-level and shard-level failures to distinguish permission errors
 * from genuinely missing or closed indices.
 */
final class SourceAccessDiagnostics {

    static final String SOURCE_INDICES_MISSING = "Source indices have been deleted or closed.";

    private SourceAccessDiagnostics() {}

    /**
     * Inspects a {@link SearchResponse} that returned null aggregations and produces
     * a human-readable error message identifying the likely cause.
     *
     * <p>The method checks, in order:
     * <ol>
     *   <li>CCS cluster-level failures for security exceptions (SKIPPED or FAILED clusters)</li>
     *   <li>Top-level shard failures for security exceptions</li>
     *   <li>Falls back to a generic "deleted or closed" message</li>
     * </ol>
     */
    static String diagnoseSourceAccessFailure(SearchResponse response) {
        String clusterSecurityError = findClusterSecurityFailure(response);
        if (clusterSecurityError != null) {
            return clusterSecurityError;
        }

        String shardSecurityError = findShardSecurityFailure(response);
        if (shardSecurityError != null) {
            return shardSecurityError;
        }

        return SOURCE_INDICES_MISSING;
    }

    /**
     * Checks CCS cluster-level information for clusters that were SKIPPED or FAILED
     * due to a security exception (e.g., user lacks permissions on the remote cluster).
     */
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
                    return "User lacks the required permissions to read source indices on cluster [" + alias + "].";
                }
            }
        }

        return null;
    }

    /**
     * Checks top-level shard failures for security exceptions (e.g., user lacks
     * permissions on a specific index).
     */
    private static String findShardSecurityFailure(SearchResponse response) {
        for (ShardSearchFailure failure : response.getShardFailures()) {
            if (isSecurityFailure(failure)) {
                String index = failure.index();
                if (index != null) {
                    return "User lacks the required permissions to read source index [" + index + "].";
                }
                return "User lacks the required permissions to read from the source indices.";
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
