/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.index.IndexResolution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsqlSessionCCSUtils {

    private EsqlSessionCCSUtils() {}

    static String createIndexExpressionFromAvailableClusters(EsqlExecutionInfo executionInfo) {
        StringBuilder sb = new StringBuilder();
        for (String clusterAlias : executionInfo.clusterAliases()) {
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(clusterAlias);
            if (cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                if (cluster.getClusterAlias().equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                    sb.append(executionInfo.getCluster(clusterAlias).getIndexExpression()).append(',');
                } else {
                    String indexExpression = executionInfo.getCluster(clusterAlias).getIndexExpression();
                    for (String index : indexExpression.split(",")) {
                        sb.append(clusterAlias).append(':').append(index).append(',');
                    }
                }
            }
        }

        if (sb.length() > 0) {
            return sb.substring(0, sb.length() - 1);
        } else {
            return "";
        }
    }

    static void updateExecutionInfoWithUnavailableClusters(EsqlExecutionInfo execInfo, Map<String, FieldCapabilitiesFailure> unavailable) {
        for (Map.Entry<String, FieldCapabilitiesFailure> entry : unavailable.entrySet()) {
            String clusterAlias = entry.getKey();
            boolean skipUnavailable = execInfo.getCluster(clusterAlias).isSkipUnavailable();
            RemoteTransportException e = new RemoteTransportException(
                Strings.format("Remote cluster [%s] (with setting skip_unavailable=%s) is not available", clusterAlias, skipUnavailable),
                entry.getValue().getException()
            );
            if (skipUnavailable) {
                execInfo.swapCluster(
                    clusterAlias,
                    (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(EsqlExecutionInfo.Cluster.Status.SKIPPED)
                        .setTotalShards(0)
                        .setSuccessfulShards(0)
                        .setSkippedShards(0)
                        .setFailedShards(0)
                        .setFailures(List.of(new ShardSearchFailure(e)))
                        .build()
                );
            } else {
                throw e;
            }
        }
    }

    static void updateExecutionInfoWithClustersWithNoMatchingIndices(EsqlExecutionInfo executionInfo, IndexResolution indexResolution) {
        Set<String> clustersWithResolvedIndices = new HashSet<>();
        // determine missing clusters
        for (String indexName : indexResolution.get().indexNameWithModes().keySet()) {
            clustersWithResolvedIndices.add(RemoteClusterAware.parseClusterAlias(indexName));
        }
        Set<String> clustersRequested = executionInfo.clusterAliases();
        Set<String> clustersWithNoMatchingIndices = Sets.difference(clustersRequested, clustersWithResolvedIndices);
        clustersWithNoMatchingIndices.removeAll(indexResolution.getUnavailableClusters().keySet());
        /*
         * These are clusters in the original request that are not present in the field-caps response. They were
         * specified with an index or indices that do not exist, so the search on that cluster is done.
         * Mark it as SKIPPED with 0 shards searched and took=0.
         */
        for (String c : clustersWithNoMatchingIndices) {
            // TODO: in a follow-on PR, throw a Verification(400 status code) for local and remotes with skip_unavailable=false if
            // they were requested with one or more concrete indices
            // for now we never mark the local cluster as SKIPPED
            final var status = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(c)
                ? EsqlExecutionInfo.Cluster.Status.SUCCESSFUL
                : EsqlExecutionInfo.Cluster.Status.SKIPPED;
            executionInfo.swapCluster(
                c,
                (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setStatus(status)
                    .setTook(new TimeValue(0))
                    .setTotalShards(0)
                    .setSuccessfulShards(0)
                    .setSkippedShards(0)
                    .setFailedShards(0)
                    .build()
            );
        }
    }

    static void updateExecutionInfoAtEndOfPlanning(EsqlExecutionInfo execInfo) {
        // TODO: this logic assumes a single phase execution model, so it may need to altered once INLINESTATS is made CCS compatible
        if (execInfo.isCrossClusterSearch()) {
            execInfo.markEndPlanning();
            for (String clusterAlias : execInfo.clusterAliases()) {
                EsqlExecutionInfo.Cluster cluster = execInfo.getCluster(clusterAlias);
                if (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                    execInfo.swapCluster(
                        clusterAlias,
                        (k, v) -> new EsqlExecutionInfo.Cluster.Builder(v).setTook(execInfo.planningTookTime())
                            .setTotalShards(0)
                            .setSuccessfulShards(0)
                            .setSkippedShards(0)
                            .setFailedShards(0)
                            .build()
                    );
                }
            }
        }
    }

    static Map<String, FieldCapabilitiesFailure> determineUnavailableRemoteClusters(List<FieldCapabilitiesFailure> failures) {
        Map<String, FieldCapabilitiesFailure> unavailableRemotes = new HashMap<>();
        for (FieldCapabilitiesFailure failure : failures) {
            if (ExceptionsHelper.isRemoteUnavailableException(failure.getException())) {
                for (String indexExpression : failure.getIndices()) {
                    if (indexExpression.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) > 0) {
                        unavailableRemotes.put(RemoteClusterAware.parseClusterAlias(indexExpression), failure);
                    }
                }
            }
        }
        return unavailableRemotes;
    }
}
