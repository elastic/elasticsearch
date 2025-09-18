/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.planner.premapper.PreMapper;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin.firstSubPlan;

public class EsqlSession {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSession.class);

    /**
     * Interface for running the underlying plan.
     * Abstracts away the underlying execution engine.
     */
    public interface PlanRunner {
        void run(PhysicalPlan plan, ActionListener<Result> listener);
    }

    private static final TransportVersion LOOKUP_JOIN_CCS = TransportVersion.fromName("lookup_join_ccs");

    private final String sessionId;
    private final Configuration configuration;
    private final IndexResolver indexResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;

    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final EsqlFunctionRegistry functionRegistry;
    private final LogicalPlanPreOptimizer logicalPlanPreOptimizer;
    private final LogicalPlanOptimizer logicalPlanOptimizer;
    private final PreMapper preMapper;

    private final Mapper mapper;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final PlanTelemetry planTelemetry;
    private final IndicesExpressionGrouper indicesExpressionGrouper;
    private final InferenceService inferenceService;
    private final RemoteClusterService remoteClusterService;

    private boolean explainMode;
    private String parsedPlanString;
    private String optimizedLogicalPlanString;

    public EsqlSession(
        String sessionId,
        Configuration configuration,
        IndexResolver indexResolver,
        EnrichPolicyResolver enrichPolicyResolver,
        PreAnalyzer preAnalyzer,
        LogicalPlanPreOptimizer logicalPlanPreOptimizer,
        EsqlFunctionRegistry functionRegistry,
        LogicalPlanOptimizer logicalPlanOptimizer,
        Mapper mapper,
        Verifier verifier,
        PlanTelemetry planTelemetry,
        IndicesExpressionGrouper indicesExpressionGrouper,
        TransportActionServices services
    ) {
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.indexResolver = indexResolver;
        this.enrichPolicyResolver = enrichPolicyResolver;
        this.preAnalyzer = preAnalyzer;
        this.logicalPlanPreOptimizer = logicalPlanPreOptimizer;
        this.verifier = verifier;
        this.functionRegistry = functionRegistry;
        this.mapper = mapper;
        this.logicalPlanOptimizer = logicalPlanOptimizer;
        this.physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
        this.planTelemetry = planTelemetry;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
        this.inferenceService = services.inferenceService();
        this.preMapper = new PreMapper(services);
        this.remoteClusterService = services.transportService().getRemoteClusterService();
    }

    public String sessionId() {
        return sessionId;
    }

    /**
     * Execute an ESQL request.
     */
    public void execute(EsqlQueryRequest request, EsqlExecutionInfo executionInfo, PlanRunner planRunner, ActionListener<Result> listener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
        assert executionInfo != null : "Null EsqlExecutionInfo";
        LOGGER.debug("ESQL query:\n{}", request.query());
        EsqlStatement statement = parse(request.query(), request.params());
        LogicalPlan plan = statement.plan();
        if (plan instanceof Explain explain) {
            explainMode = true;
            plan = explain.query();
            parsedPlanString = plan.toString();
        }
        analyzedPlan(plan, executionInfo, request.filter(), new EsqlCCSUtils.CssPartialErrorsActionListener(executionInfo, listener) {
            @Override
            public void onResponse(LogicalPlan analyzedPlan) {
                assert ThreadPool.assertCurrentThreadPool(
                    ThreadPool.Names.SEARCH,
                    ThreadPool.Names.SEARCH_COORDINATION,
                    ThreadPool.Names.SYSTEM_READ
                );
                SubscribableListener.<LogicalPlan>newForked(l -> preOptimizedPlan(analyzedPlan, l))
                    .<LogicalPlan>andThen((l, p) -> preMapper.preMapper(optimizedPlan(p), l))
                    .<Result>andThen((l, p) -> executeOptimizedPlan(request, executionInfo, planRunner, p, l))
                    .addListener(listener);
            }
        });
    }

    /**
     * Execute an analyzed plan. Most code should prefer calling {@link #execute} but
     * this is public for testing.
     */
    public void executeOptimizedPlan(
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        LogicalPlan optimizedPlan,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ
        );
        if (explainMode) {// TODO: INLINE STATS come back to the explain mode branch and reevaluate
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request);
            String physicalPlanString = physicalPlan.toString();
            List<Attribute> fields = List.of(
                new ReferenceAttribute(EMPTY, null, "role", DataType.KEYWORD),
                new ReferenceAttribute(EMPTY, null, "type", DataType.KEYWORD),
                new ReferenceAttribute(EMPTY, null, "plan", DataType.KEYWORD)
            );
            List<List<Object>> values = new ArrayList<>();
            values.add(List.of("coordinator", "parsedPlan", parsedPlanString));
            values.add(List.of("coordinator", "optimizedLogicalPlan", optimizedLogicalPlanString));
            values.add(List.of("coordinator", "optimizedPhysicalPlan", physicalPlanString));
            var blocks = BlockUtils.fromList(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values);
            physicalPlan = new LocalSourceExec(Source.EMPTY, fields, LocalSupplier.of(blocks));
            planRunner.run(physicalPlan, listener);
        } else {
            // TODO: this could be snuck into the underlying listener
            EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);
            // execute any potential subplans
            executeSubPlans(optimizedPlan, planRunner, executionInfo, request, listener);
        }
    }

    private void executeSubPlans(
        LogicalPlan optimizedPlan,
        PlanRunner runner,
        EsqlExecutionInfo executionInfo,
        EsqlQueryRequest request,
        ActionListener<Result> listener
    ) {
        var subPlan = firstSubPlan(optimizedPlan);

        // TODO: merge into one method
        if (subPlan != null) {
            // code-path to execute subplans
            executeSubPlan(
                new DriverCompletionInfo.Accumulator(),
                optimizedPlan,
                subPlan,
                executionInfo,
                runner,
                request,
                // Ensure we don't have subplan flag stuck in there on failure
                ActionListener.runAfter(listener, executionInfo::finishSubPlans)
            );
        } else {
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request);
            // execute main plan
            runner.run(physicalPlan, listener);
        }
    }

    private void executeSubPlan(
        DriverCompletionInfo.Accumulator completionInfoAccumulator,
        LogicalPlan optimizedPlan,
        InlineJoin.LogicalPlanTuple subPlans,
        EsqlExecutionInfo executionInfo,
        PlanRunner runner,
        EsqlQueryRequest request,
        ActionListener<Result> listener
    ) {
        LOGGER.debug("Executing subplan:\n{}", subPlans.stubReplacedSubPlan());
        // Create a physical plan out of the logical sub-plan
        var physicalSubPlan = logicalPlanToPhysicalPlan(subPlans.stubReplacedSubPlan(), request);

        executionInfo.startSubPlans();

        runner.run(physicalSubPlan, listener.delegateFailureAndWrap((next, result) -> {
            try {
                // Translate the subquery into a separate, coordinator based plan and the results 'broadcasted' as a local relation
                completionInfoAccumulator.accumulate(result.completionInfo());
                LocalRelation resultWrapper = resultToPlan(subPlans.stubReplacedSubPlan(), result);

                // replace the original logical plan with the backing result
                LogicalPlan newLogicalPlan = optimizedPlan.transformUp(
                    InlineJoin.class,
                    // use object equality since the right-hand side shouldn't have changed in the optimizedPlan at this point
                    // and equals would have ignored name IDs anyway
                    ij -> ij.right() == subPlans.originalSubPlan() ? InlineJoin.inlineData(ij, resultWrapper) : ij
                );
                // TODO: INLINE STATS can we do better here and further optimize the plan AFTER one of the subplans executed?
                newLogicalPlan.setOptimized();
                LOGGER.debug("Plan after previous subplan execution:\n{}", newLogicalPlan);
                // look for the next inlinejoin plan
                var newSubPlan = firstSubPlan(newLogicalPlan);

                if (newSubPlan == null) {// run the final "main" plan
                    executionInfo.finishSubPlans();
                    LOGGER.debug("Executing final plan:\n{}", newLogicalPlan);
                    var newPhysicalPlan = logicalPlanToPhysicalPlan(newLogicalPlan, request);
                    runner.run(newPhysicalPlan, next.delegateFailureAndWrap((finalListener, finalResult) -> {
                        completionInfoAccumulator.accumulate(finalResult.completionInfo());
                        finalListener.onResponse(
                            new Result(finalResult.schema(), finalResult.pages(), completionInfoAccumulator.finish(), executionInfo)
                        );
                    }));
                } else {// continue executing the subplans
                    executeSubPlan(completionInfoAccumulator, newLogicalPlan, newSubPlan, executionInfo, runner, request, listener);
                }
            } finally {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(result.pages().iterator(), p -> p::releaseBlocks)));
            }
        }));
    }

    private static LocalRelation resultToPlan(LogicalPlan plan, Result result) {
        List<Page> pages = result.pages();
        List<Attribute> schema = result.schema();
        // if (pages.size() > 1) {
        Block[] blocks = SessionUtils.fromPages(schema, pages);
        return new LocalRelation(plan.source(), schema, LocalSupplier.of(blocks));
    }

    private EsqlStatement parse(String query, QueryParams params) {
        var parsed = new EsqlParser().createQuery(query, params, planTelemetry, configuration);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Parsed logical plan:\n{}", parsed.plan());
            LOGGER.debug("Parsed settings:\n[{}]", parsed.settings().stream().map(QuerySetting::toString).collect(joining("; ")));
        }
        QuerySettings.validate(parsed, remoteClusterService);
        return parsed;
    }

    /**
     * Associates errors that occurred during field-caps with the cluster info in the execution info.
     * - Skips clusters that are no longer running, as they have already been marked as successful, skipped, or failed.
     * - If allow_partial_results or skip_unavailable is enabled, stores the failures in the cluster info but allows execution to continue.
     * - Otherwise, aborts execution with the failures.
     */
    static void handleFieldCapsFailures(
        boolean allowPartialResults,
        EsqlExecutionInfo executionInfo,
        Map<String, List<FieldCapabilitiesFailure>> failures
    ) throws Exception {
        FailureCollector failureCollector = new FailureCollector();
        for (var e : failures.entrySet()) {
            String clusterAlias = e.getKey();
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(clusterAlias);
            if (cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                assert cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.SUCCESSFUL : "can't mark a cluster success with failures";
                continue;
            }
            if (allowPartialResults == false && executionInfo.shouldSkipOnFailure(clusterAlias) == false) {
                for (FieldCapabilitiesFailure failure : e.getValue()) {
                    failureCollector.unwrapAndCollect(failure.getException());
                }
            } else if (cluster.getFailures().isEmpty()) {
                var shardFailures = e.getValue().stream().map(f -> {
                    ShardId shardId = null;
                    if (ExceptionsHelper.unwrapCause(f.getException()) instanceof ElasticsearchException es) {
                        shardId = es.getShardId();
                    }
                    if (shardId != null) {
                        return new ShardSearchFailure(f.getException(), new SearchShardTarget(null, shardId, clusterAlias));
                    } else {
                        return new ShardSearchFailure(f.getException());
                    }
                }).toList();
                executionInfo.swapCluster(
                    clusterAlias,
                    (k, curr) -> new EsqlExecutionInfo.Cluster.Builder(cluster).addFailures(shardFailures).build()
                );
            }
        }
        Exception failure = failureCollector.getFailure();
        if (failure != null) {
            throw failure;
        }
    }

    public void analyzedPlan(
        LogicalPlan parsed,
        EsqlExecutionInfo executionInfo,
        QueryBuilder requestFilter,
        ActionListener<LogicalPlan> logicalPlanListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
        if (parsed.analyzed()) {
            logicalPlanListener.onResponse(parsed);
            return;
        }

        var preAnalysis = preAnalyzer.preAnalyze(parsed);
        EsqlCCSUtils.initCrossClusterState(indicesExpressionGrouper, verifier.licenseState(), preAnalysis.index(), executionInfo);

        SubscribableListener. //
        <EnrichResolution>newForked(l -> enrichPolicyResolver.resolvePolicies(preAnalysis.enriches(), executionInfo, l))
            .<PreAnalysisResult>andThenApply(enrichResolution -> FieldNameUtils.resolveFieldNames(parsed, enrichResolution))
            .<PreAnalysisResult>andThen((l, r) -> resolveInferences(parsed, r, l))
            .<PreAnalysisResult>andThen((l, r) -> preAnalyzeMainIndices(preAnalysis, executionInfo, r, requestFilter, l))
            .<PreAnalysisResult>andThen((l, r) -> preAnalyzeLookupIndices(preAnalysis.lookupIndices().iterator(), r, executionInfo, l))
            .<LogicalPlan>andThen((l, r) -> analyzeWithRetry(parsed, requestFilter, preAnalysis, executionInfo, r, l))
            .addListener(logicalPlanListener);
    }

    private void preAnalyzeLookupIndices(
        Iterator<IndexPattern> lookupIndices,
        PreAnalysisResult preAnalysisResult,
        EsqlExecutionInfo executionInfo,
        ActionListener<PreAnalysisResult> listener
    ) {
        if (lookupIndices.hasNext()) {
            preAnalyzeLookupIndex(lookupIndices.next(), preAnalysisResult, executionInfo, listener.delegateFailureAndWrap((l, r) -> {
                preAnalyzeLookupIndices(lookupIndices, r, executionInfo, l);
            }));
        } else {
            listener.onResponse(preAnalysisResult);
        }
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
        Set<String> fieldNames = result.wildcardJoinIndices().contains(localPattern) ? IndexResolver.ALL_FIELDS : result.fieldNames;

        String patternWithRemotes;

        if (executionInfo.getClusters().isEmpty()) {
            patternWithRemotes = localPattern;
        } else {
            // convert index -> cluster1:index,cluster2:index, etc.for each running cluster
            patternWithRemotes = executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING)
                .map(c -> RemoteClusterAware.buildRemoteIndexName(c.getClusterAlias(), localPattern))
                .collect(Collectors.joining(","));
        }
        if (patternWithRemotes.isEmpty()) {
            return;
        }
        // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
        indexResolver.resolveAsMergedMapping(
            patternWithRemotes,
            fieldNames,
            null,
            false,
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
            String clusterAlias = RemoteClusterAware.parseClusterAlias(indexName);
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
        Stream<EsqlExecutionInfo.Cluster> clusters = executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING);
        // Verify that all active clusters have the lookup index resolved
        clusters.forEach(cluster -> {
            String clusterAlias = cluster.getClusterAlias();
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
     * @return An updated `IndexResolution` object if the index resolves to a single concrete index,
     *         or the original `lookupIndexResolution` if no changes are needed.
     */
    private IndexResolution checkSingleIndex(
        String index,
        EsqlExecutionInfo executionInfo,
        IndexResolution lookupIndexResolution,
        Collection<String> indexNames
    ) {
        // If all indices resolve to the same name, we can use that for BWC
        // Older clusters only can handle one name in LOOKUP JOIN
        var localIndexNames = indexNames.stream().map(n -> RemoteClusterAware.splitIndexName(n)[1]).collect(toSet());
        if (localIndexNames.size() == 1) {
            String indexName = localIndexNames.iterator().next();
            EsIndex newIndex = new EsIndex(index, lookupIndexResolution.get().mapping(), Map.of(indexName, IndexMode.LOOKUP));
            return IndexResolution.valid(newIndex, newIndex.concreteIndices(), lookupIndexResolution.failures());
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
        Stream<EsqlExecutionInfo.Cluster> clusters = executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING);
        clusters.forEach(cluster -> {
            String clusterAlias = cluster.getClusterAlias();
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

    private void preAnalyzeMainIndices(
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ
        );
        if (preAnalysis.index() != null) {
            String indexExpressionToResolve = EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            if (indexExpressionToResolve.isEmpty()) {
                // if this was a pure remote CCS request (no local indices) and all remotes are offline, return an empty IndexResolution
                listener.onResponse(
                    result.withIndexResolution(IndexResolution.valid(new EsIndex(preAnalysis.index().indexPattern(), Map.of(), Map.of())))
                );
            } else {
                boolean includeAllDimensions = false;
                // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
                if (preAnalysis.indexMode() == IndexMode.TIME_SERIES) {
                    includeAllDimensions = true;
                    // TODO: Maybe if no indices are returned, retry without index mode and provide a clearer error message.
                    var indexModeFilter = new TermQueryBuilder(IndexModeFieldMapper.NAME, IndexMode.TIME_SERIES.getName());
                    if (requestFilter != null) {
                        requestFilter = new BoolQueryBuilder().filter(requestFilter).filter(indexModeFilter);
                    } else {
                        requestFilter = indexModeFilter;
                    }
                }
                indexResolver.resolveAsMergedMapping(
                    indexExpressionToResolve,
                    result.fieldNames,
                    requestFilter,
                    includeAllDimensions,
                    listener.delegateFailure((l, indexResolution) -> {
                        l.onResponse(result.withIndexResolution(indexResolution));
                    })
                );
            }
        } else {
            // occurs when dealing with local relations (row a = 1)
            listener.onResponse(result.withIndexResolution(IndexResolution.invalid("[none specified]")));
        }
    }

    private void analyzeWithRetry(
        LogicalPlan parsed,
        QueryBuilder requestFilter,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        ActionListener<LogicalPlan> listener
    ) {
        if (result.indices.isValid()) {
            EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, result.indices.failures());
            if (executionInfo.isCrossClusterSearch()
                && executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING).findAny().isEmpty()) {
                // for a CCS, if all clusters have been marked as SKIPPED, nothing to search so send a sentinel Exception
                // to let the LogicalPlanActionListener decide how to proceed
                LOGGER.debug("No more clusters to search, ending analysis stage");
                listener.onFailure(new NoClustersToSearchException());
                return;
            }
        }

        var description = requestFilter == null ? "the only attempt without filter" : "first attempt with filter";
        LOGGER.debug("Analyzing the plan ({})", description);

        try {
            if (result.indices.isValid() || requestFilter != null) {
                // We won't run this check with no filter and no valid indices since this may lead to false positive - missing index report
                // when the resolution result is not valid for a different reason.
                if (executionInfo.clusterInfo.isEmpty() == false) {
                    EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, result.indices, requestFilter != null);
                }
            }
            LogicalPlan plan = analyzedPlan(parsed, result, executionInfo);
            LOGGER.debug("Analyzed plan ({}):\n{}", description, plan);
            // the analysis succeeded from the first attempt, irrespective if it had a filter or not, just continue with the planning
            listener.onResponse(plan);
        } catch (VerificationException ve) {
            LOGGER.debug("Analyzing the plan ({}) failed with {}", description, ve.getDetailedMessage());
            if (requestFilter == null) {
                // if the initial request didn't have a filter, then just pass the exception back to the user
                listener.onFailure(ve);
            } else {
                // retrying and make the index resolution work without any index filtering.
                preAnalyzeMainIndices(preAnalysis, executionInfo, result, null, listener.delegateFailure((l, r) -> {
                    LOGGER.debug("Analyzing the plan (second attempt, without filter)");
                    try {
                        // the order here is tricky - if the cluster has been filtered and later became unavailable,
                        // do we want to declare it successful or skipped? For now, unavailability takes precedence.
                        EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, r.indices.failures());
                        EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, r.indices, false);
                        LogicalPlan plan = analyzedPlan(parsed, r, executionInfo);
                        LOGGER.debug("Analyzed plan (second attempt without filter):\n{}", plan);
                        l.onResponse(plan);
                    } catch (Exception e) {
                        l.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void resolveInferences(LogicalPlan plan, PreAnalysisResult preAnalysisResult, ActionListener<PreAnalysisResult> l) {
        inferenceService.inferenceResolver().resolveInferenceIds(plan, l.map(preAnalysisResult::withInferenceResolution));
    }

    private PhysicalPlan logicalPlanToPhysicalPlan(LogicalPlan optimizedPlan, EsqlQueryRequest request) {
        PhysicalPlan physicalPlan = optimizedPhysicalPlan(optimizedPlan);
        physicalPlan = physicalPlan.transformUp(FragmentExec.class, f -> {
            QueryBuilder filter = request.filter();
            if (filter != null) {
                var fragmentFilter = f.esFilter();
                // TODO: have an ESFilter and push down to EsQueryExec / EsSource
                // This is an ugly hack to push the filter parameter to Lucene
                // TODO: filter integration testing
                filter = fragmentFilter != null ? boolQuery().filter(fragmentFilter).must(filter) : filter;
                LOGGER.debug("Fold filter {} to EsQueryExec", filter);
                f = f.withFilter(filter);
            }
            return f;
        });
        return EstimatesRowSize.estimateRowSize(0, physicalPlan);
    }

    private LogicalPlan analyzedPlan(LogicalPlan parsed, PreAnalysisResult r, EsqlExecutionInfo executionInfo) throws Exception {
        handleFieldCapsFailures(configuration.allowPartialResults(), executionInfo, r.indices.failures());
        Analyzer analyzer = new Analyzer(
            new AnalyzerContext(configuration, functionRegistry, r.indices, r.lookupIndices, r.enrichResolution, r.inferenceResolution),
            verifier
        );
        LogicalPlan plan = analyzer.analyze(parsed);
        plan.setAnalyzed();
        return plan;
    }

    public LogicalPlan optimizedPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.preOptimized() == false) {
            throw new IllegalStateException("Expected pre-optimized plan");
        }
        var plan = logicalPlanOptimizer.optimize(logicalPlan);
        LOGGER.debug("Optimized logicalPlan plan:\n{}", plan);
        return plan;
    }

    public void preOptimizedPlan(LogicalPlan logicalPlan, ActionListener<LogicalPlan> listener) {
        logicalPlanPreOptimizer.preOptimize(logicalPlan, listener);
    }

    public PhysicalPlan physicalPlan(LogicalPlan optimizedPlan) {
        if (optimizedPlan.optimized() == false) {
            throw new IllegalStateException("Expected optimized plan");
        }
        optimizedLogicalPlanString = optimizedPlan.toString();
        var plan = mapper.map(optimizedPlan);
        LOGGER.debug("Physical plan:\n{}", plan);
        return plan;
    }

    public PhysicalPlan optimizedPhysicalPlan(LogicalPlan optimizedPlan) {
        var plan = physicalPlanOptimizer.optimize(physicalPlan(optimizedPlan));
        LOGGER.debug("Optimized physical plan:\n{}", plan);
        return plan;
    }

    public record PreAnalysisResult(
        IndexResolution indices,
        Map<String, IndexResolution> lookupIndices,
        EnrichResolution enrichResolution,
        Set<String> fieldNames,
        Set<String> wildcardJoinIndices,
        InferenceResolution inferenceResolution
    ) {

        public PreAnalysisResult(EnrichResolution enrichResolution, Set<String> fieldNames, Set<String> wildcardJoinIndices) {
            this(null, new HashMap<>(), enrichResolution, fieldNames, wildcardJoinIndices, InferenceResolution.EMPTY);
        }

        PreAnalysisResult withInferenceResolution(InferenceResolution newInferenceResolution) {
            return new PreAnalysisResult(
                indices(),
                lookupIndices(),
                enrichResolution(),
                fieldNames(),
                wildcardJoinIndices(),
                newInferenceResolution
            );
        }

        PreAnalysisResult withIndexResolution(IndexResolution newIndexResolution) {
            return new PreAnalysisResult(
                newIndexResolution,
                lookupIndices(),
                enrichResolution(),
                fieldNames(),
                wildcardJoinIndices(),
                inferenceResolution()
            );
        }

        PreAnalysisResult addLookupIndexResolution(String index, IndexResolution newIndexResolution) {
            lookupIndices.put(index, newIndexResolution);
            return this;
        }
    }
}
