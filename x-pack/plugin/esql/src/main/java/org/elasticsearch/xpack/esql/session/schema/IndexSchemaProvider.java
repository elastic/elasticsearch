/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.EsqlSession.PreAnalysisResult;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;

/**
 * Schema for the index-backed abstractions (concrete indices, aliases, data streams) — produced by the
 * field-caps fetch behind {@link IndexResolver}. The provider owns the full index-resolution orchestration:
 * the cross-cluster vs cross-project dispatch, the per-pattern field-caps calls, the concrete time-series
 * retry, and the lookup-index resolution and validation. The session hands it a {@link SchemaContext} and a
 * {@link PreAnalysisResult} and gets the accumulated resolution back.
 */
final class IndexSchemaProvider {

    private static final Logger LOGGER = LogManager.getLogger(IndexSchemaProvider.class);

    private static final TransportVersion LOOKUP_JOIN_CCS = TransportVersion.fromName("lookup_join_ccs");

    private final IndexResolver indexResolver;
    private final RemoteClusterService remoteClusterService;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final IndicesExpressionGrouper indicesExpressionGrouper;
    private final PlanTelemetry planTelemetry;
    private final Verifier verifier;

    IndexSchemaProvider(
        IndexResolver indexResolver,
        RemoteClusterService remoteClusterService,
        CrossProjectModeDecider crossProjectModeDecider,
        IndicesExpressionGrouper indicesExpressionGrouper,
        PlanTelemetry planTelemetry,
        Verifier verifier
    ) {
        this.indexResolver = indexResolver;
        this.remoteClusterService = remoteClusterService;
        this.crossProjectModeDecider = crossProjectModeDecider;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
        this.planTelemetry = planTelemetry;
        this.verifier = verifier;
    }

    /**
     * Perform a field caps request for each index pattern and determine the minimum transport version of all clusters with matching
     * indices.
     */
    void resolveMainIndices(SchemaContext ctx, PreAnalysisResult result, ActionListener<PreAnalysisResult> listener) {
        PreAnalyzer.PreAnalysis preAnalysis = ctx.preAnalysis();
        EsqlExecutionInfo executionInfo = ctx.executionInfo();
        QueryBuilder requestFilter = ctx.requestFilter();
        boolean trackUnmappedFieldIndices = ctx.trackUnmappedFieldIndices();
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ
        );
        if (crossProjectModeDecider.crossProjectEnabled() == false) {
            EsqlCCSUtils.initCrossClusterState(
                indicesExpressionGrouper,
                verifier.licenseState(),
                preAnalysis.indexes().keySet(),
                executionInfo
            );
            // The main index pattern dictates on which nodes the query can be executed,
            // so we use the minimum transport version from this field caps request.
            forAll(
                preAnalysis.indexes().entrySet().iterator(),
                result,
                (e, r, l) -> preAnalyzeMainIndices(
                    e.getKey(),
                    e.getValue(),
                    preAnalysis,
                    executionInfo,
                    trackUnmappedFieldIndices,
                    r,
                    requestFilter,
                    l
                ),
                listener
            );
        } else {
            // Strict pass first: resolves the user-typed UnresolvedRelation patterns and initialises
            // cross-cluster state. After it completes we run the lenient pass over any
            // ViewShadowRelation patterns (CPS-only) so their results land in
            // result.optionalLinkedResolution() — empty iterator → no-op when there are no shadows.
            forAll(
                preAnalysis.indexes().entrySet().iterator(),
                result,
                (e, r, l) -> preAnalyzeFlatMainIndices(
                    e.getKey(),
                    e.getValue(),
                    ctx.configuration().projectRouting(),
                    preAnalysis,
                    executionInfo,
                    trackUnmappedFieldIndices,
                    r,
                    requestFilter,
                    l
                ),
                listener.delegateFailureAndWrap(
                    (l, strictResult) -> forAll(
                        preAnalysis.linkedIndices().iterator(),
                        strictResult,
                        (sp, r, ll) -> preAnalyzeLinkedIndices(
                            sp,
                            ctx.configuration().projectRouting(),
                            preAnalysis,
                            executionInfo,
                            trackUnmappedFieldIndices,
                            r,
                            requestFilter,
                            ll
                        ),
                        l
                    )
                )
            );
        }
    }

    private void preAnalyzeMainIndices(
        IndexPattern indexPattern,
        IndexMode indexMode,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        boolean trackUnmappedFieldIndices,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        if (executionInfo.clusterAliases().isEmpty()) {
            // return empty resolution if the expression is pure CCS and resolved no remote clusters (like no-such-cluster*:index)
            listener.onResponse(result.withIndices(indexPattern, IndexResolution.empty(indexPattern.indexPattern())));
        } else {
            executionInfo.queryProfile().incFieldCapsCalls();
            indexResolver.resolveMainIndicesVersioned(
                indexPattern.indexPattern(),
                result.fieldNames(),
                createQueryFilter(indexMode, requestFilter),
                indexMode == IndexMode.TIME_SERIES,
                // TODO: In case of subqueries, the different main index resolutions don't know about each other's minimum version.
                // This is bad because `FROM (FROM remote1:*) (FROM remote2:*)` can have different minimum versions
                // while resolving each subquery's main index pattern. We'll determine the correct overall minimum transport version
                // in the end because we keep updating the PreAnalysisResult after each resolution; but the EsIndex objects may be
                // inconsistent with this version:
                // The main index pattern from a subquery that we resolve first may have a higher min version in the field caps response
                // than an index pattern that we resolve later.
                // Thus, the EsIndex for `FROM remote1:*` may contain data types that aren't supported on the overall minimum version
                // if we only find out that the overall version is actually lower when resolving `FROM remote2:*`.
                result.minimumTransportVersion(),
                preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
                preAnalysis.useDenseVectorWhenNotSupported(),
                preAnalysis.hasTimeSeriesAggregation(),
                trackUnmappedFieldIndices,
                indicesExpressionGrouper,
                listener.delegateFailureAndWrap((l, indexResolution) -> {
                    EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.inner().failures());
                    EsqlCCSUtils.checkForViewErrors(indexResolution.inner().failures());
                    maybeRetryConcreteTimeSeriesResolution(indexPattern, indexMode, result, indexResolution, l, retryListener -> {
                        executionInfo.queryProfile().incFieldCapsCalls();
                        indexResolver.resolveMainIndicesVersioned(
                            indexPattern.indexPattern(),
                            result.fieldNames(),
                            requestFilter,
                            false,
                            indexResolution.minimumVersion(),
                            preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
                            preAnalysis.useDenseVectorWhenNotSupported(),
                            false,
                            trackUnmappedFieldIndices,
                            indicesExpressionGrouper,
                            retryListener
                        );
                    });
                })
            );
        }
    }

    /**
     * This performs field caps resolutions for linkedIndexPatterns
     * in order to resolve optional and required linked indices shadowed by local views.
     */
    private void preAnalyzeLinkedIndices(
        LinkedIndexPattern linkedIndexPattern,
        String projectRouting,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        boolean trackUnmappedFieldIndices,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        executionInfo.queryProfile().incFieldCapsCalls();
        indexResolver.resolveFlatIndicesVersioned(
            linkedIndexPattern.kind() == LinkedIndexPattern.Kind.OPTIONAL,
            linkedIndexPattern.pattern().indexPattern(),
            projectRouting,
            result.fieldNames(),
            createQueryFilter(IndexMode.STANDARD, requestFilter),
            false /* not time-series — shadows are always STANDARD */,
            result.minimumTransportVersion(),
            preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
            preAnalysis.useDenseVectorWhenNotSupported(),
            preAnalysis.hasTimeSeriesAggregation(),
            trackUnmappedFieldIndices,
            listener.delegateFailureAndWrap((l, indexResolution) -> {
                EsqlCCSUtils.initCrossClusterState(indexResolution.inner(), executionInfo);
                EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.inner().failures());
                EsqlCCSUtils.checkForViewErrors(indexResolution.inner().failures());
                EsqlCCSUtils.validateCcsLicense(verifier.licenseState(), executionInfo);
                // TODO count distinct linked projects
                l.onResponse(result.withWithLinkedIndices(linkedIndexPattern, indexResolution.inner()));
            })
        );
    }

    private void preAnalyzeFlatMainIndices(
        IndexPattern indexPattern,
        IndexMode indexMode,
        String projectRouting,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        boolean trackUnmappedFieldIndices,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        executionInfo.queryProfile().incFieldCapsCalls();
        indexResolver.resolveFlatIndicesVersioned(
            false /* lenient */,
            indexPattern.indexPattern(),
            projectRouting,
            result.fieldNames(),
            createQueryFilter(indexMode, requestFilter),
            indexMode == IndexMode.TIME_SERIES,
            // TODO: Same problem with subqueries as preAnalyzeMainIndices, see above.
            result.minimumTransportVersion(),
            preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
            preAnalysis.useDenseVectorWhenNotSupported(),
            preAnalysis.hasTimeSeriesAggregation(),
            trackUnmappedFieldIndices,
            listener.delegateFailureAndWrap((l, indexResolution) -> {
                EsqlCCSUtils.initCrossClusterState(indexResolution.inner(), executionInfo);
                EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.inner().failures());
                EsqlCCSUtils.checkForViewErrors(indexResolution.inner().failures());
                EsqlCCSUtils.validateCcsLicense(verifier.licenseState(), executionInfo);
                planTelemetry.linkedProjectsCount(executionInfo.clusterInfo.size());
                maybeRetryConcreteTimeSeriesResolution(indexPattern, indexMode, result, indexResolution, l, retryListener -> {
                    executionInfo.queryProfile().incFieldCapsCalls();
                    indexResolver.resolveFlatIndicesVersioned(
                        false /* lenient */,
                        indexPattern.indexPattern(),
                        projectRouting,
                        result.fieldNames(),
                        requestFilter,
                        false,
                        indexResolution.minimumVersion(),
                        preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
                        preAnalysis.useDenseVectorWhenNotSupported(),
                        false,
                        trackUnmappedFieldIndices,
                        retryListener
                    );
                });
            })
        );
    }

    private static QueryBuilder createQueryFilter(IndexMode indexMode, QueryBuilder requestFilter) {
        return switch (indexMode) {
            case IndexMode.TIME_SERIES -> {
                var indexModeFilter = new TermQueryBuilder(IndexModeFieldMapper.NAME, IndexMode.TIME_SERIES.getName());
                yield requestFilter != null ? new BoolQueryBuilder().filter(requestFilter).filter(indexModeFilter) : indexModeFilter;
            }
            default -> requestFilter;
        };
    }

    // visible for testing
    static boolean shouldRetryConcreteTimeSeriesResolution(IndexMode indexMode, IndexResolution resolution, IndexPattern indexPattern) {
        return indexMode == IndexMode.TIME_SERIES
            && resolution.isValid()
            && resolution.resolvedIndices().isEmpty()
            && EsqlCCSUtils.concreteIndexRequested(indexPattern.indexPattern());
    }

    // visible for testing
    static IndexResolution refineConcreteTimeSeriesResolution(
        IndexPattern indexPattern,
        IndexResolution originalResolution,
        IndexResolution retryResolution
    ) {
        return resolvedConcreteIndexWithoutTimeSeriesFilter(retryResolution)
            ? IndexResolution.invalid("[" + indexPattern.indexPattern() + "] is not a time series index. Use FROM command instead")
            : originalResolution;
    }

    private static boolean resolvedConcreteIndexWithoutTimeSeriesFilter(IndexResolution retryResolution) {
        return retryResolution.isValid() && retryResolution.resolvedIndices().isEmpty() == false;
    }

    private void maybeRetryConcreteTimeSeriesResolution(
        IndexPattern indexPattern,
        IndexMode indexMode,
        PreAnalysisResult result,
        Versioned<IndexResolution> indexResolution,
        ActionListener<PreAnalysisResult> listener,
        Consumer<ActionListener<Versioned<IndexResolution>>> resolveWithoutModeFilter
    ) {
        IndexResolution originalResolution = indexResolution.inner();
        if (shouldRetryConcreteTimeSeriesResolution(indexMode, originalResolution, indexPattern) == false) {
            listener.onResponse(
                result.withIndices(indexPattern, originalResolution).withMinimumTransportVersion(indexResolution.minimumVersion())
            );
            return;
        }
        resolveWithoutModeFilter.accept(ActionListener.wrap(retryResolution -> {
            IndexResolution finalResolution = refineConcreteTimeSeriesResolution(indexPattern, originalResolution, retryResolution.inner());
            TransportVersion finalMinimumVersion = finalResolution == originalResolution
                ? indexResolution.minimumVersion()
                : retryResolution.minimumVersion();
            listener.onResponse(result.withIndices(indexPattern, finalResolution).withMinimumTransportVersion(finalMinimumVersion));
        }, e -> {
            LOGGER.debug("Retry without TIME_SERIES filter failed for [{}]: {}", indexPattern.indexPattern(), e.getMessage());
            listener.onResponse(
                result.withIndices(indexPattern, originalResolution).withMinimumTransportVersion(indexResolution.minimumVersion())
            );
        }));
    }

    /**
     * Perform a field caps request for each lookup index. Does not update the minimum transport version.
     */
    void resolveLookupIndices(SchemaContext ctx, PreAnalysisResult result, ActionListener<PreAnalysisResult> listener) {
        EsqlExecutionInfo executionInfo = ctx.executionInfo();
        forAll(
            ctx.preAnalysis().lookupIndices().iterator(),
            result,
            (lookupIndex, r, l) -> preAnalyzeLookupIndex(lookupIndex, r, executionInfo, l),
            listener
        );
    }

    private void preAnalyzeLookupIndex(
        IndexPattern lookupIndexPattern,
        PreAnalysisResult result,
        EsqlExecutionInfo executionInfo,
        ActionListener<PreAnalysisResult> listener
    ) {
        String localPattern = lookupIndexPattern.indexPattern();
        assert RemoteClusterAware.isRemoteIndexName(localPattern) == false
            : "Lookup index name should not include remote, but got: " + localPattern;
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ
        );
        // No need to update the minimum transport version in the PreAnalysisResult,
        // it should already have been determined during the main index resolution.
        executionInfo.queryProfile().incFieldCapsCalls();
        indexResolver.resolveLookupIndices(
            EsqlCCSUtils.createQualifiedLookupIndexExpressionFromAvailableClusters(executionInfo, localPattern),
            result.wildcardJoinIndices().contains(localPattern) ? IndexResolver.ALL_FIELDS : result.fieldNames(),
            // We use the minimum version determined in the main index resolution, because for remote LOOKUP JOIN, we're only considering
            // remote lookup indices in the field caps request - but the coordinating cluster must be considered, too!
            // The main index resolution should already have taken the version of the coordinating cluster into account and this should
            // be reflected in result.minimumTransportVersion().
            result.minimumTransportVersion(),
            listener.map(indexResolution -> receiveLookupIndexResolution(result, localPattern, executionInfo, indexResolution))
        );
    }

    private void skipClusterOrError(String clusterAlias, EsqlExecutionInfo executionInfo, String message) {
        skipClusterOrError(clusterAlias, executionInfo, new VerificationException(message));
    }

    private void skipClusterOrError(String clusterAlias, EsqlExecutionInfo executionInfo, ElasticsearchException error) {
        // If we can, skip the cluster and mark it as such
        if (executionInfo.shouldSkipOnFailure(clusterAlias)) {
            EsqlCCSUtils.markClusterWithFinalStateAndNoShards(executionInfo, clusterAlias, EsqlExecutionInfo.Cluster.Status.SKIPPED, error);
        } else {
            throw error;
        }
    }

    /**
     * Receive and process lookup index resolutions from resolveAsMergedMapping.
     * This processes the lookup index data for a single index, updates and returns the {@link PreAnalysisResult} result
     */
    private PreAnalysisResult receiveLookupIndexResolution(
        PreAnalysisResult result,
        String index,
        EsqlExecutionInfo executionInfo,
        IndexResolution lookupIndexResolution
    ) {
        EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, lookupIndexResolution.failures());
        if (lookupIndexResolution.isValid() == false) {
            // If the index resolution is invalid, don't bother with the rest of the analysis
            return result.addLookupIndexResolution(index, lookupIndexResolution);
        }
        if (executionInfo.getClusters().isEmpty() || executionInfo.isCrossClusterSearch() == false) {
            // Local only case, still do some checks, since we moved analysis checks here
            if (lookupIndexResolution.get().indexNameWithModes().isEmpty()) {
                // This is not OK, but we proceed with it as we do with invalid resolution, and it will fail on the verification
                // because lookup field will be missing.
                return result.addLookupIndexResolution(index, lookupIndexResolution);
            }
            if (lookupIndexResolution.get().indexNameWithModes().size() > 1) {
                throw new VerificationException(
                    "Lookup Join requires a single lookup mode index; [" + index + "] resolves to multiple indices"
                );
            }
            var indexModeEntry = lookupIndexResolution.get().indexNameWithModes().entrySet().iterator().next();
            if (indexModeEntry.getValue() != IndexMode.LOOKUP) {
                throw new VerificationException(
                    "Lookup Join requires a single lookup mode index; ["
                        + index
                        + "] resolves to ["
                        + indexModeEntry.getKey()
                        + "] in ["
                        + indexModeEntry.getValue()
                        + "] mode"
                );
            }

            return result.addLookupIndexResolution(index, lookupIndexResolution);
        }

        if (lookupIndexResolution.get().indexNameWithModes().isEmpty() && lookupIndexResolution.resolvedIndices().isEmpty() == false) {
            // This is a weird situation - we have empty index list but non-empty resolution. This is likely because IndexResolver
            // got an empty map and pretends to have an empty resolution. This means this query will fail, since lookup fields will not
            // match, but here we can pretend it's ok to pass it on to the verifier and generate a correct error message.
            // Note this only happens if the map is completely empty, which means it's going to error out anyway, since we should have
            // at least the key field there.
            return result.addLookupIndexResolution(index, lookupIndexResolution);
        }

        // Collect resolved clusters from the index resolution, verify that each cluster has a single resolution for the lookup index
        Map<String, String> clustersWithResolvedIndices = new HashMap<>(lookupIndexResolution.resolvedIndices().size());
        lookupIndexResolution.get().indexNameWithModes().forEach((indexName, indexMode) -> {
            String clusterAlias = RemoteClusterAware.splitIndexName(indexName).getClusterGroupingKey();
            // Check that all indices are in lookup mode
            if (indexMode != IndexMode.LOOKUP) {
                skipClusterOrError(
                    clusterAlias,
                    executionInfo,
                    "Lookup Join requires a single lookup mode index; ["
                        + index
                        + "] resolves to ["
                        + indexName
                        + "] in ["
                        + indexMode
                        + "] mode"
                );
            }
            // Each cluster should have only one resolution for the lookup index
            if (clustersWithResolvedIndices.containsKey(clusterAlias)) {
                skipClusterOrError(
                    clusterAlias,
                    executionInfo,
                    "Lookup Join requires a single lookup mode index; ["
                        + index
                        + "] resolves to multiple indices "
                        + EsqlCCSUtils.inClusterName(clusterAlias)
                );
            } else {
                clustersWithResolvedIndices.put(clusterAlias, indexName);
            }
        });

        // These are clusters that are still in the running, we need to have the index on all of them
        // Verify that all active clusters have the lookup index resolved
        executionInfo.getRunningClusterAliases().forEach(clusterAlias -> {
            if (clustersWithResolvedIndices.containsKey(clusterAlias) == false) {
                // Missing cluster resolution
                skipClusterOrError(clusterAlias, executionInfo, findFailure(lookupIndexResolution.failures(), index, clusterAlias));
            }
        });

        return result.addLookupIndexResolution(
            index,
            checkSingleIndex(index, executionInfo, lookupIndexResolution, clustersWithResolvedIndices.values())
        );
    }

    private ElasticsearchException findFailure(Map<String, List<FieldCapabilitiesFailure>> failures, String index, String clusterAlias) {
        if (failures.containsKey(clusterAlias)) {
            var exc = failures.get(clusterAlias).stream().findFirst().map(FieldCapabilitiesFailure::getException);
            if (exc.isPresent()) {
                return new VerificationException(
                    "lookup failed " + EsqlCCSUtils.inClusterName(clusterAlias) + " for index [" + index + "]",
                    ExceptionsHelper.unwrapCause(exc.get())
                );
            }
        }
        return new VerificationException("lookup index [" + index + "] is not available " + EsqlCCSUtils.inClusterName(clusterAlias));
    }

    /**
     * Check whether the lookup index resolves to a single concrete index on all clusters or not.
     * If it's a single index, we are compatible with old pre-9.2 LOOKUP JOIN code and just need to send the same resolution as we did.
     * If there are multiple index names (e.g. due to aliases) then pre-9.2 clusters won't be able to handle it so we need to skip them.
     *
     * @return An updated `IndexResolution` object if the index resolves to a single concrete index,
     * or the original `lookupIndexResolution` if no changes are needed.
     */
    private IndexResolution checkSingleIndex(
        String index,
        EsqlExecutionInfo executionInfo,
        IndexResolution lookupIndexResolution,
        Collection<String> indexNames
    ) {
        // If all indices resolve to the same name, we can use that for BWC
        // Older clusters only can handle one name in LOOKUP JOIN
        var localIndexNames = indexNames.stream().map(n -> RemoteClusterAware.splitIndexName(n).indexExpression()).collect(toSet());
        if (localIndexNames.size() == 1) {
            String indexName = localIndexNames.iterator().next();
            EsIndex newIndex = new EsIndex(
                index,
                lookupIndexResolution.get().mapping(),
                Map.of(indexName, IndexMode.LOOKUP),
                Map.of(),
                Map.of()
            );
            return IndexResolution.valid(newIndex, newIndex.concreteQualifiedIndices(), lookupIndexResolution.failures());
        }
        // validate remotes to be able to handle multiple indices in LOOKUP JOIN
        validateRemoteVersions(executionInfo);
        return lookupIndexResolution;
    }

    /**
     * Older clusters can only handle one name in LOCAL JOIN - verify that all the remotes involved
     * are recent enough to be able to handle multiple indices.
     * This is only checked if there are actually multiple indices, which happens when remotes have a different
     * concrete indices aliased to the same index name.
     */
    private void validateRemoteVersions(EsqlExecutionInfo executionInfo) {
        executionInfo.getRunningClusterAliases().forEach(clusterAlias -> {
            if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false) {
                // No need to check local, obviously
                var connection = remoteClusterService.getConnection(clusterAlias);
                if (connection != null && connection.getTransportVersion().supports(LOOKUP_JOIN_CCS) == false) {
                    skipClusterOrError(
                        clusterAlias,
                        executionInfo,
                        "remote cluster ["
                            + clusterAlias
                            + "] has version ["
                            + connection.getTransportVersion()
                            + "] that does not support multiple indices in LOOKUP JOIN, skipping"
                    );
                }
            }
        });
    }

    private static <T> void forAll(
        Iterator<T> iterator,
        PreAnalysisResult result,
        TriConsumer<T, PreAnalysisResult, ActionListener<PreAnalysisResult>> consumer,
        ActionListener<PreAnalysisResult> listener
    ) {
        if (iterator.hasNext()) {
            consumer.apply(iterator.next(), result, listener.delegateFailureAndWrap((l, r) -> forAll(iterator, r, consumer, l)));
        } else {
            listener.onResponse(result);
        }
    }
}
