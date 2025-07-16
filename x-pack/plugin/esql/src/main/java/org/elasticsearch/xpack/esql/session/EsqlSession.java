/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.CheckedFunction;
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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.EmptyAttribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.index.MappingException;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
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

    private final String sessionId;
    private final Configuration configuration;
    private final IndexResolver indexResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;

    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final EsqlFunctionRegistry functionRegistry;
    private final LogicalPlanOptimizer logicalPlanOptimizer;
    private final PreMapper preMapper;

    private final Mapper mapper;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final PlanTelemetry planTelemetry;
    private final IndicesExpressionGrouper indicesExpressionGrouper;
    private Set<String> configuredClusters;
    private final InferenceRunner inferenceRunner;
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
        this.verifier = verifier;
        this.functionRegistry = functionRegistry;
        this.mapper = mapper;
        this.logicalPlanOptimizer = logicalPlanOptimizer;
        this.physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration));
        this.planTelemetry = planTelemetry;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
        this.inferenceRunner = services.inferenceRunner();
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
        assert executionInfo != null : "Null EsqlExecutionInfo";
        LOGGER.debug("ESQL query:\n{}", request.query());
        LogicalPlan parsed = parse(request.query(), request.params());
        if (parsed instanceof Explain explain) {
            explainMode = true;
            parsed = explain.query();
            parsedPlanString = parsed.toString();
        }
        analyzedPlan(parsed, executionInfo, request.filter(), new EsqlCCSUtils.CssPartialErrorsActionListener(executionInfo, listener) {
            @Override
            public void onResponse(LogicalPlan analyzedPlan) {
                preMapper.preMapper(
                    analyzedPlan,
                    listener.delegateFailureAndWrap((l, p) -> executeOptimizedPlan(request, executionInfo, planRunner, optimizedPlan(p), l))
                );
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
        if (explainMode) {// TODO: INLINESTATS come back to the explain mode branch and reevaluate
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request);
            String physicalPlanString = physicalPlan.toString();
            List<Attribute> fields = List.of(
                new ReferenceAttribute(EMPTY, "role", DataType.KEYWORD),
                new ReferenceAttribute(EMPTY, "type", DataType.KEYWORD),
                new ReferenceAttribute(EMPTY, "plan", DataType.KEYWORD)
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
            executeSubPlan(new DriverCompletionInfo.Accumulator(), optimizedPlan, subPlan, executionInfo, runner, request, listener);
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
                // TODO: INLINESTATS can we do better here and further optimize the plan AFTER one of the subplans executed?
                newLogicalPlan.setOptimized();
                LOGGER.debug("Plan after previous subplan execution:\n{}", newLogicalPlan);
                // look for the next inlinejoin plan
                var newSubPlan = firstSubPlan(newLogicalPlan);

                if (newSubPlan == null) {// run the final "main" plan
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

    private LogicalPlan parse(String query, QueryParams params) {
        var parsed = new EsqlParser().createStatement(query, params, planTelemetry, configuration);
        LOGGER.debug("Parsed logical plan:\n{}", parsed);
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
            if (allowPartialResults == false && executionInfo.isSkipUnavailable(clusterAlias) == false) {
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
        if (parsed.analyzed()) {
            logicalPlanListener.onResponse(parsed);
            return;
        }

        CheckedFunction<PreAnalysisResult, LogicalPlan, Exception> analyzeAction = (l) -> {
            handleFieldCapsFailures(configuration.allowPartialResults(), executionInfo, l.indices.failures());
            Analyzer analyzer = new Analyzer(
                new AnalyzerContext(configuration, functionRegistry, l.indices, l.lookupIndices, l.enrichResolution, l.inferenceResolution),
                verifier
            );
            LogicalPlan plan = analyzer.analyze(parsed);
            plan.setAnalyzed();
            return plan;
        };
        // Capture configured remotes list to ensure consistency throughout the session
        configuredClusters = Set.copyOf(indicesExpressionGrouper.getConfiguredClusters());

        PreAnalyzer.PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        var unresolvedPolicies = preAnalysis.enriches.stream()
            .map(
                e -> new EnrichPolicyResolver.UnresolvedPolicy(
                    BytesRefs.toString(e.policyName().fold(FoldContext.small() /* TODO remove me*/)),
                    e.mode()
                )
            )
            .collect(Collectors.toSet());
        final List<IndexPattern> indices = preAnalysis.indices;

        EsqlCCSUtils.checkForCcsLicense(executionInfo, indices, indicesExpressionGrouper, configuredClusters, verifier.licenseState());
        initializeClusterData(indices, executionInfo);

        var listener = SubscribableListener.<EnrichResolution>newForked(
            l -> enrichPolicyResolver.resolvePolicies(unresolvedPolicies, executionInfo, l)
        )
            .<PreAnalysisResult>andThen((l, enrichResolution) -> resolveFieldNames(parsed, enrichResolution, l))
            .<PreAnalysisResult>andThen((l, preAnalysisResult) -> resolveInferences(preAnalysis.inferencePlans, preAnalysisResult, l));
        // first resolve the lookup indices, then the main indices
        for (var index : preAnalysis.lookupIndices) {
            listener = listener.andThen((l, preAnalysisResult) -> preAnalyzeLookupIndex(index, preAnalysisResult, executionInfo, l));
        }
        listener.<PreAnalysisResult>andThen((l, result) -> {
            // resolve the main indices
            preAnalyzeMainIndices(preAnalysis, executionInfo, result, requestFilter, l);
        }).<PreAnalysisResult>andThen((l, result) -> {
            // TODO in follow-PR (for skip_unavailable handling of missing concrete indexes) add some tests for
            // invalid index resolution to updateExecutionInfo
            // If we run out of clusters to search due to unavailability we can stop the analysis right here
            if (result.indices.isValid() && allCCSClustersSkipped(executionInfo, result, logicalPlanListener)) return;
            // whatever tuple we have here (from CCS-special handling or from the original pre-analysis), pass it on to the next step
            l.onResponse(result);
        }).<PreAnalysisResult>andThen((l, result) -> {
            // first attempt (maybe the only one) at analyzing the plan
            analyzeAndMaybeRetry(analyzeAction, requestFilter, result, executionInfo, logicalPlanListener, l);
        }).<PreAnalysisResult>andThen((l, result) -> {
            assert requestFilter != null : "The second pre-analysis shouldn't take place when there is no index filter in the request";

            // here the requestFilter is set to null, performing the pre-analysis after the first step failed
            preAnalyzeMainIndices(preAnalysis, executionInfo, result, null, l);
        }).<LogicalPlan>andThen((l, result) -> {
            assert requestFilter != null : "The second analysis shouldn't take place when there is no index filter in the request";
            LOGGER.debug("Analyzing the plan (second attempt, without filter)");
            LogicalPlan plan;
            try {
                // the order here is tricky - if the cluster has been filtered and later became unavailable,
                // do we want to declare it successful or skipped? For now, unavailability takes precedence.
                EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, result.indices.failures());
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, result.indices, false);
                plan = analyzeAction.apply(result);
            } catch (Exception e) {
                l.onFailure(e);
                return;
            }
            LOGGER.debug("Analyzed plan (second attempt, without filter):\n{}", plan);
            l.onResponse(plan);
        }).addListener(logicalPlanListener);
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
            listener.map(indexResolution -> receiveLookupIndexResolution(result, localPattern, executionInfo, indexResolution))
        );
    }

    private void skipClusterOrError(String clusterAlias, EsqlExecutionInfo executionInfo, String message) {
        VerificationException error = new VerificationException(message);
        // If we can, skip the cluster and mark it as such
        if (executionInfo.isSkipUnavailable(clusterAlias)) {
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
                skipClusterOrError(
                    clusterAlias,
                    executionInfo,
                    "lookup index [" + index + "] is not available " + EsqlCCSUtils.inClusterName(clusterAlias)
                );
            }
        });

        return result.addLookupIndexResolution(
            index,
            checkSingleIndex(index, executionInfo, lookupIndexResolution, clustersWithResolvedIndices.values())
        );
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
        var localIndexNames = indexNames.stream().map(n -> RemoteClusterAware.splitIndexName(n)[1]).collect(Collectors.toSet());
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
                if (connection != null && connection.getTransportVersion().before(TransportVersions.LOOKUP_JOIN_CCS)) {
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

    private void initializeClusterData(List<IndexPattern> indices, EsqlExecutionInfo executionInfo) {
        if (indices.isEmpty()) {
            return;
        }
        assert indices.size() == 1 : "Only single index pattern is supported";
        Map<String, OriginalIndices> clusterIndices = indicesExpressionGrouper.groupIndices(
            configuredClusters,
            IndicesOptions.DEFAULT,
            indices.getFirst().indexPattern()
        );
        for (Map.Entry<String, OriginalIndices> entry : clusterIndices.entrySet()) {
            final String clusterAlias = entry.getKey();
            String indexExpr = Strings.arrayToCommaDelimitedString(entry.getValue().indices());
            executionInfo.swapCluster(clusterAlias, (k, v) -> {
                assert v == null : "No cluster for " + clusterAlias + " should have been added to ExecutionInfo yet";
                return new EsqlExecutionInfo.Cluster(clusterAlias, indexExpr, executionInfo.isSkipUnavailable(clusterAlias));
            });
        }
    }

    private void preAnalyzeMainIndices(
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        List<IndexPattern> indices = preAnalysis.indices;
        if (indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (indices.size() == 1) {
            IndexPattern table = indices.getFirst();

            // if the preceding call to the enrich policy API found unavailable clusters, recreate the index expression to search
            // based only on available clusters (which could now be an empty list)
            String indexExpressionToResolve = EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            if (indexExpressionToResolve.isEmpty()) {
                // if this was a pure remote CCS request (no local indices) and all remotes are offline, return an empty IndexResolution
                listener.onResponse(
                    result.withIndexResolution(IndexResolution.valid(new EsIndex(table.indexPattern(), Map.of(), Map.of())))
                );
            } else {
                // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
                if (preAnalysis.indexMode == IndexMode.TIME_SERIES) {
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
                    listener.delegateFailure((l, indexResolution) -> {
                        l.onResponse(result.withIndexResolution(indexResolution));
                    })
                );
            }
        } else {
            try {
                // occurs when dealing with local relations (row a = 1)
                listener.onResponse(result.withIndexResolution(IndexResolution.invalid("[none specified]")));
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    /**
     * Check if there are any clusters to search.
     *
     * @return true if there are no clusters to search, false otherwise
     */
    private boolean allCCSClustersSkipped(
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        ActionListener<LogicalPlan> logicalPlanListener
    ) {
        IndexResolution indexResolution = result.indices;
        EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.failures());
        if (executionInfo.isCrossClusterSearch()
            && executionInfo.getClusterStates(EsqlExecutionInfo.Cluster.Status.RUNNING).findAny().isEmpty()) {
            // for a CCS, if all clusters have been marked as SKIPPED, nothing to search so send a sentinel Exception
            // to let the LogicalPlanActionListener decide how to proceed
            LOGGER.debug("No more clusters to search, ending analysis stage");
            logicalPlanListener.onFailure(new NoClustersToSearchException());
            return true;
        }

        return false;
    }

    private static void analyzeAndMaybeRetry(
        CheckedFunction<PreAnalysisResult, LogicalPlan, Exception> analyzeAction,
        QueryBuilder requestFilter,
        PreAnalysisResult result,
        EsqlExecutionInfo executionInfo,
        ActionListener<LogicalPlan> logicalPlanListener,
        ActionListener<PreAnalysisResult> l
    ) {
        LogicalPlan plan = null;
        var filterPresentMessage = requestFilter == null ? "without" : "with";
        var attemptMessage = requestFilter == null ? "the only" : "first";
        LOGGER.debug("Analyzing the plan ({} attempt, {} filter)", attemptMessage, filterPresentMessage);

        try {
            if (result.indices.isValid() || requestFilter != null) {
                // We won't run this check with no filter and no valid indices since this may lead to false positive - missing index report
                // when the resolution result is not valid for a different reason.
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, result.indices, requestFilter != null);
            }
            plan = analyzeAction.apply(result);
        } catch (Exception e) {
            if (e instanceof VerificationException ve) {
                LOGGER.debug(
                    "Analyzing the plan ({} attempt, {} filter) failed with {}",
                    attemptMessage,
                    filterPresentMessage,
                    ve.getDetailedMessage()
                );
                if (requestFilter == null) {
                    // if the initial request didn't have a filter, then just pass the exception back to the user
                    logicalPlanListener.onFailure(ve);
                } else {
                    // interested only in a VerificationException, but this time we are taking out the index filter
                    // to try and make the index resolution work without any index filtering. In the next step... to be continued
                    l.onResponse(result);
                }
            } else {
                // if the query failed with any other type of exception, then just pass the exception back to the user
                logicalPlanListener.onFailure(e);
            }
            return;
        }
        LOGGER.debug("Analyzed plan ({} attempt, {} filter):\n{}", attemptMessage, filterPresentMessage, plan);
        // the analysis succeeded from the first attempt, irrespective if it had a filter or not, just continue with the planning
        logicalPlanListener.onResponse(plan);
    }

    private static void resolveFieldNames(LogicalPlan parsed, EnrichResolution enrichResolution, ActionListener<PreAnalysisResult> l) {
        try {
            // we need the match_fields names from enrich policies and THEN, with an updated list of fields, we call field_caps API
            var enrichMatchFields = enrichResolution.resolvedEnrichPolicies()
                .stream()
                .map(ResolvedEnrichPolicy::matchField)
                .collect(Collectors.toSet());
            // get the field names from the parsed plan combined with the ENRICH match fields from the ENRICH policy
            l.onResponse(fieldNames(parsed, enrichMatchFields, new PreAnalysisResult(enrichResolution)));
        } catch (Exception ex) {
            l.onFailure(ex);
        }
    }

    private void resolveInferences(
        List<InferencePlan<?>> inferencePlans,
        PreAnalysisResult preAnalysisResult,
        ActionListener<PreAnalysisResult> l
    ) {
        inferenceRunner.resolveInferenceIds(inferencePlans, l.map(preAnalysisResult::withInferenceResolution));
    }

    static PreAnalysisResult fieldNames(LogicalPlan parsed, Set<String> enrichPolicyMatchFields, PreAnalysisResult result) {
        List<LogicalPlan> inlinestats = parsed.collect(InlineStats.class::isInstance);
        Set<Aggregate> inlinestatsAggs = new HashSet<>();
        for (var i : inlinestats) {
            inlinestatsAggs.add(((InlineStats) i).aggregate());
        }

        if (false == parsed.anyMatch(p -> shouldCollectReferencedFields(p, inlinestatsAggs))) {
            // no explicit columns selection, for example "from employees"
            // also, inlinestats only adds columns to the existent output, its Aggregate shouldn't interfere with potentially using "*"
            return result.withFieldNames(IndexResolver.ALL_FIELDS);
        }

        // TODO: Improve field resolution for FORK - right now we request all fields
        if (parsed.anyMatch(p -> p instanceof Fork)) {
            return result.withFieldNames(IndexResolver.ALL_FIELDS);
        }

        Holder<Boolean> projectAll = new Holder<>(false);
        parsed.forEachExpressionDown(UnresolvedStar.class, us -> {// explicit "*" fields selection
            if (projectAll.get()) {
                return;
            }
            projectAll.set(true);
        });

        if (projectAll.get()) {
            return result.withFieldNames(IndexResolver.ALL_FIELDS);
        }

        var referencesBuilder = AttributeSet.builder();
        // "keep" and "drop" attributes are special whenever a wildcard is used in their name, as the wildcard can cover some
        // attributes ("lookup join" generated columns among others); steps like removal of Aliases should ignore fields matching the
        // wildcards.
        //
        // E.g. "from test | eval lang = languages + 1 | keep *l" should consider both "languages" and "*l" as valid fields to ask for
        // "from test | eval first_name = 1 | drop first_name | drop *name" should also consider "*name" as valid field to ask for
        //
        // NOTE: the grammar allows wildcards to be used in other commands as well, but these are forbidden in the LogicalPlanBuilder
        // Except in KEEP and DROP.
        var keepRefs = AttributeSet.builder();
        var dropWildcardRefs = AttributeSet.builder();
        // fields required to request for lookup joins to work
        var joinRefs = AttributeSet.builder();
        // lookup indices where we request "*" because we may require all their fields
        Set<String> wildcardJoinIndices = new java.util.HashSet<>();

        boolean[] canRemoveAliases = new boolean[] { true };

        parsed.forEachDown(p -> {// go over each plan top-down
            if (p instanceof RegexExtract re) { // for Grok and Dissect
                // keep the inputs needed by Grok/Dissect
                referencesBuilder.addAll(re.input().references());
            } else if (p instanceof Enrich enrich) {
                AttributeSet enrichFieldRefs = Expressions.references(enrich.enrichFields());
                AttributeSet.Builder enrichRefs = enrichFieldRefs.combine(enrich.matchField().references()).asBuilder();
                // Enrich adds an EmptyAttribute if no match field is specified
                // The exact name of the field will be added later as part of enrichPolicyMatchFields Set
                enrichRefs.removeIf(attr -> attr instanceof EmptyAttribute);
                referencesBuilder.addAll(enrichRefs);
            } else if (p instanceof LookupJoin join) {
                if (join.config().type() instanceof JoinTypes.UsingJoinType usingJoinType) {
                    joinRefs.addAll(usingJoinType.columns());
                }
                if (keepRefs.isEmpty()) {
                    // No KEEP commands after the JOIN, so we need to mark this index for "*" field resolution
                    wildcardJoinIndices.add(((UnresolvedRelation) join.right()).indexPattern().indexPattern());
                } else {
                    // Keep commands can reference the join columns with names that shadow aliases, so we block their removal
                    joinRefs.addAll(keepRefs);
                }
            } else {
                referencesBuilder.addAll(p.references());
                if (p instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.TIME_SERIES) {
                    // METRICS aggs generally rely on @timestamp without the user having to mention it.
                    referencesBuilder.add(new UnresolvedAttribute(ur.source(), MetadataAttribute.TIMESTAMP_FIELD));
                }
                // special handling for UnresolvedPattern (which is not an UnresolvedAttribute)
                p.forEachExpression(UnresolvedNamePattern.class, up -> {
                    var ua = new UnresolvedAttribute(up.source(), up.name());
                    referencesBuilder.add(ua);
                    if (p instanceof Keep) {
                        keepRefs.add(ua);
                    } else if (p instanceof Drop) {
                        dropWildcardRefs.add(ua);
                    } else {
                        throw new IllegalStateException("Only KEEP and DROP should allow wildcards");
                    }
                });
                if (p instanceof Keep) {
                    keepRefs.addAll(p.references());
                }
            }

            // If the current node in the tree is of type JOIN (lookup join, inlinestats) or ENRICH or other type of
            // command that we may add in the future which can override already defined Aliases with EVAL
            // (for example
            //
            // from test
            // | eval ip = 123
            // | enrich ips_policy ON hostname
            // | rename ip AS my_ip
            //
            // and ips_policy enriches the results with the same name ip field),
            // these aliases should be kept in the list of fields.
            if (canRemoveAliases[0] && p.anyMatch(EsqlSession::couldOverrideAliases)) {
                canRemoveAliases[0] = false;
            }
            if (canRemoveAliases[0]) {
                // remove any already discovered UnresolvedAttributes that are in fact aliases defined later down in the tree
                // for example "from test | eval x = salary | stats max = max(x) by gender"
                // remove the UnresolvedAttribute "x", since that is an Alias defined in "eval"
                // also remove other down-the-tree references to the extracted fields from "grok" and "dissect"
                AttributeSet planRefs = p.references();
                Set<String> fieldNames = planRefs.names();
                p.forEachExpressionDown(NamedExpression.class, ne -> {
                    if ((ne instanceof Alias || ne instanceof ReferenceAttribute) == false) {
                        return;
                    }
                    // do not remove the UnresolvedAttribute that has the same name as its alias, ie "rename id AS id"
                    // or the UnresolvedAttributes that are used in Functions that have aliases "STATS id = MAX(id)"
                    if (fieldNames.contains(ne.name())) {
                        return;
                    }
                    referencesBuilder.removeIf(
                        attr -> matchByName(attr, ne.name(), keepRefs.contains(attr) || dropWildcardRefs.contains(attr))
                    );
                });
            }
        });

        // Add JOIN ON column references afterward to avoid Alias removal
        referencesBuilder.addAll(joinRefs);
        // If any JOIN commands need wildcard field-caps calls, persist the index names
        if (wildcardJoinIndices.isEmpty() == false) {
            result = result.withWildcardJoinIndices(wildcardJoinIndices);
        }

        // remove valid metadata attributes because they will be filtered out by the IndexResolver anyway
        // otherwise, in some edge cases, we will fail to ask for "*" (all fields) instead
        referencesBuilder.removeIf(a -> a instanceof MetadataAttribute || MetadataAttribute.isSupported(a.name()));
        Set<String> fieldNames = referencesBuilder.build().names();

        if (fieldNames.isEmpty() && enrichPolicyMatchFields.isEmpty()) {
            // there cannot be an empty list of fields, we'll ask the simplest and lightest one instead: _index
            return result.withFieldNames(IndexResolver.INDEX_METADATA_FIELD);
        } else {
            fieldNames.addAll(subfields(fieldNames));
            fieldNames.addAll(enrichPolicyMatchFields);
            fieldNames.addAll(subfields(enrichPolicyMatchFields));
            return result.withFieldNames(fieldNames);
        }
    }

    /**
     * Indicates whether the given plan gives an exact list of fields that we need to collect from field_caps.
     */
    private static boolean shouldCollectReferencedFields(LogicalPlan plan, Set<Aggregate> inlinestatsAggs) {
        return plan instanceof Project || (plan instanceof Aggregate agg && inlinestatsAggs.contains(agg) == false);
    }

    /**
     * Could a plan "accidentally" override aliases?
     * Examples are JOIN and ENRICH, that _could_ produce fields with the same
     * name of an existing alias, based on their index mapping.
     * Here we just have to consider commands where this information is not available before index resolution,
     * eg. EVAL, GROK, DISSECT can override an alias, but we know it in advance, ie. we don't need to resolve indices to know.
     */
    private static boolean couldOverrideAliases(LogicalPlan p) {
        return (p instanceof Aggregate
            || p instanceof Completion
            || p instanceof Drop
            || p instanceof Eval
            || p instanceof Filter
            || p instanceof Fork
            || p instanceof InlineStats
            || p instanceof Insist
            || p instanceof Keep
            || p instanceof Limit
            || p instanceof MvExpand
            || p instanceof OrderBy
            || p instanceof Project
            || p instanceof RegexExtract
            || p instanceof Rename
            || p instanceof TopN
            || p instanceof UnresolvedRelation) == false;
    }

    private static boolean matchByName(Attribute attr, String other, boolean skipIfPattern) {
        boolean isPattern = Regex.isSimpleMatchPattern(attr.name());
        if (skipIfPattern && isPattern) {
            return false;
        }
        var name = attr.name();
        return isPattern ? Regex.simpleMatch(name, other) : name.equals(other);
    }

    private static Set<String> subfields(Set<String> names) {
        return names.stream().filter(name -> name.endsWith(WILDCARD) == false).map(name -> name + ".*").collect(Collectors.toSet());
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

    public LogicalPlan optimizedPlan(LogicalPlan logicalPlan) {
        if (logicalPlan.analyzed() == false) {
            throw new IllegalStateException("Expected analyzed plan");
        }
        var plan = logicalPlanOptimizer.optimize(logicalPlan);
        LOGGER.debug("Optimized logicalPlan plan:\n{}", plan);
        return plan;
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

    record PreAnalysisResult(
        IndexResolution indices,
        Map<String, IndexResolution> lookupIndices,
        EnrichResolution enrichResolution,
        Set<String> fieldNames,
        Set<String> wildcardJoinIndices,
        InferenceResolution inferenceResolution
    ) {
        PreAnalysisResult(EnrichResolution newEnrichResolution) {
            this(null, new HashMap<>(), newEnrichResolution, Set.of(), Set.of(), InferenceResolution.EMPTY);
        }

        PreAnalysisResult withEnrichResolution(EnrichResolution newEnrichResolution) {
            return new PreAnalysisResult(
                indices(),
                lookupIndices(),
                newEnrichResolution,
                fieldNames(),
                wildcardJoinIndices(),
                inferenceResolution()
            );
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

        PreAnalysisResult withFieldNames(Set<String> newFields) {
            return new PreAnalysisResult(
                indices(),
                lookupIndices(),
                enrichResolution(),
                newFields,
                wildcardJoinIndices(),
                inferenceResolution()
            );
        }

        public PreAnalysisResult withWildcardJoinIndices(Set<String> wildcardJoinIndices) {
            return new PreAnalysisResult(
                indices(),
                lookupIndices(),
                enrichResolution(),
                fieldNames(),
                wildcardJoinIndices,
                inferenceResolution()
            );
        }
    }
}
