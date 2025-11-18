/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo.Cluster;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public class EsqlCCSUtils {

    private EsqlCCSUtils() {}

    static Map<String, List<FieldCapabilitiesFailure>> groupFailuresPerCluster(List<FieldCapabilitiesFailure> failures) {
        Map<String, List<FieldCapabilitiesFailure>> perCluster = new HashMap<>();
        for (FieldCapabilitiesFailure failure : failures) {
            String cluster = RemoteClusterAware.parseClusterAlias(failure.getIndices()[0]);
            perCluster.computeIfAbsent(cluster, k -> new ArrayList<>()).add(failure);
        }
        return perCluster;
    }

    static Map<String, FieldCapabilitiesFailure> determineUnavailableRemoteClusters(Map<String, List<FieldCapabilitiesFailure>> failures) {
        Map<String, FieldCapabilitiesFailure> unavailableRemotes = new HashMap<>(failures.size());
        for (var e : failures.entrySet()) {
            if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(e.getKey())) {
                continue;
            }
            if (e.getValue().stream().allMatch(f -> ExceptionsHelper.isRemoteUnavailableException(f.getException()))) {
                unavailableRemotes.put(e.getKey(), e.getValue().get(0));
            }
        }
        return unavailableRemotes;
    }

    /**
     * ActionListener that receives LogicalPlan or error from logical planning.
     * Any Exception sent to onFailure stops processing, but not all are fatal (return a 4xx or 5xx), so
     * the onFailure handler determines whether to return an empty successful result or a 4xx/5xx error.
     */
    abstract static class CssPartialErrorsActionListener implements ActionListener<Versioned<LogicalPlan>> {
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
                listener.onResponse(new Result(Analyzer.NO_FIELDS, Collections.emptyList(), DriverCompletionInfo.EMPTY, executionInfo));
            } else {
                listener.onFailure(e);
            }
        }
    }

    /**
     * Whether to return an empty result (HTTP status 200) for a CCS rather than a top level 4xx/5xx error.
     * <p>
     * For cases where field-caps had no indices to search and the remotes were unavailable, we
     * return an empty successful response (200) if all remotes are marked with skip_unavailable=true.
     * <p>
     * Note: a follow-on PR will expand this logic to handle cases where no indices could be found to match
     * on any of the requested clusters.
     */
    static boolean returnSuccessWithEmptyResult(EsqlExecutionInfo executionInfo, Exception e) {
        if (executionInfo.isCrossClusterSearch() == false) {
            return false;
        }

        if (e instanceof NoClustersToSearchException || ExceptionsHelper.isRemoteUnavailableException(e)) {
            for (String clusterAlias : executionInfo.clusterAliases()) {
                // Check if we have any remotes that can't be skipped on failure.
                if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false
                    && executionInfo.shouldSkipOnFailure(clusterAlias) == false) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    static void updateExecutionInfoToReturnEmptyResult(EsqlExecutionInfo executionInfo, Exception e) {
        // This applies even for subplans - if we had an error and have to skip a cluster, then it will remain skipped.
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
                    if (v.getFailures().isEmpty()) {
                        builder.addFailures(List.of(new ShardSearchFailure(exceptionForResponse)));
                    }
                }
                return builder.build();
            });
        }
    }

    static String createQualifiedLookupIndexExpressionFromAvailableClusters(EsqlExecutionInfo executionInfo, String localPattern) {
        if (executionInfo.getClusters().isEmpty()) {
            return localPattern;
        }
        return executionInfo.getRunningClusterAliases()
            .map(clusterAlias -> RemoteClusterAware.buildRemoteIndexName(clusterAlias, localPattern))
            .collect(joining(","));
    }

    static void updateExecutionInfoWithUnavailableClusters(
        EsqlExecutionInfo execInfo,
        Map<String, List<FieldCapabilitiesFailure>> failures
    ) {
        Map<String, FieldCapabilitiesFailure> unavailable = determineUnavailableRemoteClusters(failures);
        for (Map.Entry<String, FieldCapabilitiesFailure> entry : unavailable.entrySet()) {
            String clusterAlias = entry.getKey();
            boolean skipUnavailable = execInfo.getCluster(clusterAlias).isSkipUnavailable();
            RemoteTransportException e = new RemoteTransportException(
                Strings.format("Remote cluster [%s] (with setting skip_unavailable=%s) is not available", clusterAlias, skipUnavailable),
                entry.getValue().getException()
            );
            if (skipUnavailable) {
                markClusterWithFinalStateAndNoShards(execInfo, clusterAlias, EsqlExecutionInfo.Cluster.Status.SKIPPED, e);
            } else {
                throw e;
            }
        }
    }

    static void updateExecutionInfoWithClustersWithNoMatchingIndices(
        EsqlExecutionInfo executionInfo,
        Collection<IndexResolution> indexResolutions,
        boolean usedFilter
    ) {
        if (executionInfo.clusterInfo.isEmpty()) {
            return;
        }
        // Get the clusters which are still running, and we will check whether they have any matching indices.
        // NOTE: we assume that updateExecutionInfoWithUnavailableClusters() was already run and took care of unavailable clusters.
        final Set<String> clustersWithNoMatchingIndices = executionInfo.getRunningClusterAliases().collect(toSet());
        for (IndexResolution indexResolution : indexResolutions) {
            for (String indexName : indexResolution.resolvedIndices()) {
                clustersWithNoMatchingIndices.remove(RemoteClusterAware.parseClusterAlias(indexName));
            }
        }
        /*
         * Rules enforced at planning time around non-matching indices
         * 1. fail query if no matching indices on any cluster (VerificationException) - that is handled elsewhere
         * 2. fail query if a cluster has no matching indices *and* a concrete index was specified - handled here
         */
        String fatalErrorMessage = null;
        /*
         * These are clusters in the original request that are not present in the field-caps response. They were
         * specified with an index expression that matched no indices, so the search on that cluster is done.
         * Mark it as SKIPPED with 0 shards searched and took=0.
         */
        for (String c : clustersWithNoMatchingIndices) {
            final String indexExpression = executionInfo.getCluster(c).getIndexExpression();
            if (concreteIndexRequested(executionInfo.getCluster(c).getIndexExpression())) {
                String error = Strings.format(
                    "Unknown index [%s]",
                    (c.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) ? indexExpression : c + ":" + indexExpression)
                );
                if (executionInfo.shouldSkipOnFailure(c) == false || usedFilter) {
                    if (fatalErrorMessage == null) {
                        fatalErrorMessage = error;
                    } else {
                        fatalErrorMessage += "; " + error;
                    }
                }
                if (usedFilter == false) {
                    // We check for filter since the filter may be the reason why the index is missing, and then we don't want to mark yet
                    markClusterWithFinalStateAndNoShards(
                        executionInfo,
                        c,
                        executionInfo.shouldSkipOnFailure(c) ? Cluster.Status.SKIPPED : Cluster.Status.FAILED,
                        new VerificationException(error)
                    );
                }
            } else {
                // We check for the valid resolution because if we have empty resolution it's still an error.
                for (IndexResolution indexResolution : indexResolutions) {
                    if (indexResolution.isValid()) {
                        List<FieldCapabilitiesFailure> failures = indexResolution.failures().getOrDefault(c, List.of());
                        // No matching indices, no concrete index requested, and no error in field-caps; just mark as done.
                        if (failures.isEmpty()) {
                            markClusterWithFinalStateAndNoShards(executionInfo, c, Cluster.Status.SUCCESSFUL, null);
                        } else {
                            // skip reporting index_not_found exceptions to avoid spamming users with such errors
                            // when queries use a remote cluster wildcard, e.g., `*:my-logs*`.
                            Exception nonIndexNotFound = failures.stream()
                                .map(FieldCapabilitiesFailure::getException)
                                .filter(ex -> ExceptionsHelper.unwrap(ex, IndexNotFoundException.class) == null)
                                .findAny()
                                .orElse(null);
                            markClusterWithFinalStateAndNoShards(executionInfo, c, Cluster.Status.SKIPPED, nonIndexNotFound);
                        }
                    }
                }
            }
        }
        if (fatalErrorMessage != null) {
            throw new VerificationException(fatalErrorMessage);
        }
    }

    // Filter-less version, mainly for testing where we don't need filter support
    static void updateExecutionInfoWithClustersWithNoMatchingIndices(
        EsqlExecutionInfo executionInfo,
        Set<IndexResolution> indexResolutions
    ) {
        updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolutions, false);
    }

    // visible for testing
    static boolean concreteIndexRequested(String indexExpression) {
        if (Strings.isNullOrBlank(indexExpression)) {
            return false;
        }
        for (String expr : indexExpression.split(",")) {
            if (expr.charAt(0) == '<' || expr.startsWith("-<")) {
                // skip date math expressions
                continue;
            }
            if (expr.indexOf('*') < 0) {
                return true;
            }
        }
        return false;
    }

    // visible for testing
    static void updateExecutionInfoAtEndOfPlanning(EsqlExecutionInfo execInfo) {
        // TODO: this logic assumes a single phase execution model, so it may need to altered once INLINE STATS is made CCS compatible
        execInfo.markEndPlanning();
        if (execInfo.isCrossClusterSearch() || execInfo.includeExecutionMetadata() == EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS) {
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

    /**
     * Checks the index expression for the presence of remote clusters.
     * If found, it will ensure that the caller has a valid Enterprise (or Trial) license on the querying cluster
     * as well as initialize the corresponding cluster state in execution info.
     * @throws org.elasticsearch.ElasticsearchStatusException if the license is not valid (or present) for ES|QL CCS search.
     */
    public static void initCrossClusterState(
        IndicesExpressionGrouper indicesGrouper,
        XPackLicenseState licenseState,
        Set<IndexPattern> indexPatterns,
        EsqlExecutionInfo executionInfo
    ) throws ElasticsearchStatusException {
        if (indexPatterns.isEmpty()) {
            return;
        }
        try {
            for (IndexPattern indexPattern : indexPatterns) {
                var groupedIndices = indicesGrouper.groupIndices(
                    IndicesOptions.DEFAULT,
                    Strings.splitStringByCommaToArray(indexPattern.indexPattern()),
                    false
                );

                executionInfo.clusterInfoInitializing(true);
                // initialize the cluster entries in EsqlExecutionInfo before throwing the invalid license error
                // so that the CCS telemetry handler can recognize that this error is CCS-related
                try {
                    groupedIndices.forEach((clusterAlias, indices) -> {
                        executionInfo.swapCluster(clusterAlias, (k, v) -> {
                            var indexExpr = Strings.arrayToCommaDelimitedString(indices.indices());
                            if (v != null) {
                                indexExpr = v.getIndexExpression() + "," + indexExpr;
                            }
                            return new EsqlExecutionInfo.Cluster(clusterAlias, indexExpr, executionInfo.shouldSkipOnFailure(clusterAlias));
                        });
                    });
                } finally {
                    executionInfo.clusterInfoInitializing(false);
                }
            }

            if (executionInfo.isCrossClusterSearch() && EsqlLicenseChecker.isCcsAllowed(licenseState) == false) {
                throw EsqlLicenseChecker.invalidLicenseForCcsException(licenseState);
            }
        } catch (NoSuchRemoteClusterException e) {
            if (EsqlLicenseChecker.isCcsAllowed(licenseState)) {
                throw e;
            } else {
                throw EsqlLicenseChecker.invalidLicenseForCcsException(licenseState);
            }
        }
    }

    /**
     * Mark cluster with a final status (success or failure).
     * Most metrics are set to 0 if not set yet, except for "took" which is set to the total time taken so far.
     * The status must be the final status of the cluster, not RUNNING.
     */
    public static void markClusterWithFinalStateAndNoShards(
        EsqlExecutionInfo executionInfo,
        String clusterAlias,
        Cluster.Status status,
        @Nullable Exception ex
    ) {
        assert status != Cluster.Status.RUNNING : "status must be a final state, not RUNNING";
        executionInfo.swapCluster(clusterAlias, (k, v) -> {
            Cluster.Builder builder = new Cluster.Builder(v).setStatus(status)
                .setTook(executionInfo.tookSoFar())
                .setTotalShards(Objects.requireNonNullElse(v.getTotalShards(), 0))
                .setSuccessfulShards(Objects.requireNonNullElse(v.getSuccessfulShards(), 0))
                .setSkippedShards(Objects.requireNonNullElse(v.getSkippedShards(), 0))
                .setFailedShards(Objects.requireNonNullElse(v.getFailedShards(), 0));
            if (ex != null) {
                builder.addFailures(List.of(new ShardSearchFailure(ex)));
            }
            return builder.build();
        });
    }

    /**
     * Check whether this exception can be tolerated when partial results are on, or should be treated as fatal.
     * @return true if the exception can be tolerated, false if it should be treated as fatal.
     */
    public static boolean canAllowPartial(Exception e) {
        Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
        if (unwrapped instanceof IndexNotFoundException || unwrapped instanceof ElasticsearchSecurityException) {
            return false;
        }
        return true;
    }

    public static String inClusterName(String clusterAlias) {
        if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias)) {
            return "in local cluster";
        } else {
            return "in remote cluster [" + clusterAlias + "]";
        }
    }
}
