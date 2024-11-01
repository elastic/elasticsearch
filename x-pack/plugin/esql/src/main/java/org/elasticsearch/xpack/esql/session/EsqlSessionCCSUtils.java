/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class EsqlSessionCCSUtils {

    private EsqlSessionCCSUtils() {}

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

    /**
     * ActionListener that receives LogicalPlan or error from logical planning.
     * Any Exception sent to onFailure stops processing, but not all are fatal (return a 4xx or 5xx), so
     * the onFailure handler determines whether to return an empty successful result or a 4xx/5xx error.
     */
    abstract static class CssPartialErrorsActionListener implements ActionListener<LogicalPlan> {
        private final EsqlExecutionInfo executionInfo;
        private final ActionListener<Result> listener;

        CssPartialErrorsActionListener(EsqlExecutionInfo executionInfo, ActionListener<Result> listener) {
            this.executionInfo = executionInfo;
            this.listener = listener;
        }

        @Override
        public void onFailure(Exception e) {
            if (returnSuccessWithEmptyResult(executionInfo, e)) {
                updateExecutionInfoToReturnEmptyResult(executionInfo, e);
                listener.onResponse(new Result(Analyzer.NO_FIELDS, Collections.emptyList(), Collections.emptyList(), executionInfo));
            } else {
                listener.onFailure(e);
            }
        }
    }

    /**
     * Whether to return an empty result (HTTP status 200) for a CCS rather than a top level 4xx/5xx error.
     *
     * For cases where field-caps had no indices to search and the remotes were unavailable, we
     * return an empty successful response (200) if all remotes are marked with skip_unavailable=true.
     *
     * Note: a follow-on PR will expand this logic to handle cases where no indices could be found to match
     * on any of the requested clusters.
     */
    static boolean returnSuccessWithEmptyResult(EsqlExecutionInfo executionInfo, Exception e) {
        if (executionInfo.isCrossClusterSearch() == false) {
            return false;
        }

        if (e instanceof NoClustersToSearchException || ExceptionsHelper.isRemoteUnavailableException(e)) {
            for (String clusterAlias : executionInfo.clusterAliases()) {
                if (executionInfo.isSkipUnavailable(clusterAlias) == false
                    && clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static void updateExecutionInfoToReturnEmptyResult(EsqlExecutionInfo executionInfo, Exception e) {
        executionInfo.markEndQuery();
        Exception exceptionForResponse;
        if (e instanceof ConnectTransportException) {
            // when field-caps has no field info (since no clusters could be connected to or had matching indices)
            // it just throws the first exception in its list, so this odd special handling is here is to avoid
            // having one specific remote alias name in all failure lists in the metadata response
            exceptionForResponse = new RemoteTransportException("connect_transport_exception - unable to connect to remote cluster", null);
        } else {
            exceptionForResponse = e;
        }
        for (String clusterAlias : executionInfo.clusterAliases()) {
            executionInfo.swapCluster(clusterAlias, (k, v) -> {
                EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(v).setTook(executionInfo.overallTook())
                    .setTotalShards(0)
                    .setSuccessfulShards(0)
                    .setSkippedShards(0)
                    .setFailedShards(0);
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
                    // never mark local cluster as skipped
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                } else {
                    builder.setStatus(EsqlExecutionInfo.Cluster.Status.SKIPPED);
                    // add this exception to the failures list only if there is no failure already recorded there
                    if (v.getFailures() == null || v.getFailures().size() == 0) {
                        builder.setFailures(List.of(new ShardSearchFailure(exceptionForResponse)));
                    }
                }
                return builder.build();
            });
        }
    }

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

        /**
         * The rules to determine whether missing indices is an error or not are:
         * 1. no matching indices on any cluster: error   MP TODO: I think this is handled elsewhere? Where? Check this
         * ✓ 2. skip_unavailable=false remotes with no matching indices: error
         * 3. missing concrete index expression:
         *  (a) error for local and skip_unavailable=false remotes
         *  (b) not an error for skip_unavailable=true remotes as long as there is at least one other matching index expression in the query (for any cluster)
         * 4. wildcard index expressions, if at least one matches: not an error
         */

        String fatalErrorMessage = null;
        /*
         * MP TODO: update this code comment
         * These are clusters in the original request that are not present in the field-caps response. They were
         * specified with an index or indices that do not exist, so the search on that cluster is done.
         * Mark it as SKIPPED with 0 shards searched and took=0.
         */
        for (String c : clustersWithNoMatchingIndices) {
            if (executionInfo.isSkipUnavailable(c) == false)) {
                String error = "No matching indices for [" + executionInfo.getCluster(c).getIndexExpression() + "]";
                if (c.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false) {
                    error += " on remote cluster [" + c + "]";
                }
                if (fatalErrorMessage == null) {
                    fatalErrorMessage = error;
                } else {
                    fatalErrorMessage += "; " + error;
                }
            } else {
                EsqlExecutionInfo.Cluster.Status status = c.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) ?
                    EsqlExecutionInfo.Cluster.Status.SUCCESSFUL : EsqlExecutionInfo.Cluster.Status.SKIPPED;
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
        if (fatalErrorMessage != null) {
            throw new VerificationException(fatalErrorMessage);
        }
    }

    // MP TODO: is there a better method for doing this, say in RemoteClusterService or RemoteClusterAware?
    private static boolean concreteIndexRequested(String indexExpression) {
        return indexExpression.indexOf('*') < 0;
    }

    // visible for testing
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
}
