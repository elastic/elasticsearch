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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.PlanTimeProfile;
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
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.approximation.Approximation;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanPreOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPreOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.planner.premapper.PreMapper;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin.firstSubPlan;
import static org.elasticsearch.xpack.esql.session.SessionUtils.checkPagesBelowSize;

/**
 * Combines all components necessary for the coordinating node to plan and execute an ESQL query,
 * including (pre-)analyzing, optimizing and running the physical plan.
 * <p>
 * In particular, this is where we perform remote calls to pre-analyze the query,
 * that is, to resolve indices, enrich policies and their mappings.
 * <p>
 * Note that this is not a session in the traditional sense of a stateful connection. This will
 * produce a single result set that is either returned to the user directly or stored for
 * later retrieval if the query was async.
 */
public class EsqlSession {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSession.class);

    /**
     * Interface for running the underlying plan.
     * Abstracts away the underlying execution engine.
     */
    public interface PlanRunner {
        void run(
            PhysicalPlan plan,
            Configuration configuration,
            FoldContext foldContext,
            PlanTimeProfile planTimeProfile,
            ActionListener<Result> listener
        );
    }

    private static final TransportVersion LOOKUP_JOIN_CCS = TransportVersion.fromName("lookup_join_ccs");

    private final String sessionId;
    private final TransportVersion localClusterMinimumVersion;
    private final AnalyzerSettings analyzerSettings;
    private final IndexResolver indexResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final ViewResolver viewResolver;

    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final EsqlFunctionRegistry functionRegistry;
    private final PreMapper preMapper;

    private final Mapper mapper;
    private final PlanTelemetry planTelemetry;
    private final IndicesExpressionGrouper indicesExpressionGrouper;
    private final InferenceService inferenceService;
    private final RemoteClusterService remoteClusterService;
    private final BlockFactory blockFactory;
    private final PlannerSettings plannerSettings;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final String clusterName;

    private boolean explainMode;
    private String parsedPlanString;
    private String optimizedLogicalPlanString;
    private final ProjectMetadata projectMetadata;

    public EsqlSession(
        String sessionId,
        TransportVersion localClusterMinimumVersion,
        AnalyzerSettings analyzerSettings,
        IndexResolver indexResolver,
        EnrichPolicyResolver enrichPolicyResolver,
        ViewResolver viewResolver,
        PreAnalyzer preAnalyzer,
        EsqlFunctionRegistry functionRegistry,
        Mapper mapper,
        Verifier verifier,
        PlanTelemetry planTelemetry,
        IndicesExpressionGrouper indicesExpressionGrouper,
        ProjectMetadata projectMetadata,
        PlannerSettings plannerSettings,
        TransportActionServices services
    ) {
        this.sessionId = sessionId;
        this.localClusterMinimumVersion = localClusterMinimumVersion;
        this.analyzerSettings = analyzerSettings;
        this.indexResolver = indexResolver;
        this.enrichPolicyResolver = enrichPolicyResolver;
        this.viewResolver = viewResolver;
        this.preAnalyzer = preAnalyzer;
        this.verifier = verifier;
        this.functionRegistry = functionRegistry;
        this.mapper = mapper;
        this.planTelemetry = planTelemetry;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
        this.inferenceService = services.inferenceService();
        this.preMapper = new PreMapper(services);
        this.remoteClusterService = services.transportService().getRemoteClusterService();
        this.blockFactory = services.blockFactoryProvider().blockFactory();
        this.plannerSettings = plannerSettings;
        this.crossProjectModeDecider = services.crossProjectModeDecider();
        this.clusterName = services.clusterService().getClusterName().value();
        this.projectMetadata = projectMetadata;
    }

    public String sessionId() {
        return sessionId;
    }

    /**
     * Execute an ESQL request.
     */
    public void execute(
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        ActionListener<Versioned<Result>> listener
    ) {
        executionInfo.queryProfile().planning().start();
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);
        assert executionInfo != null : "Null EsqlExecutionInfo";
        LOGGER.debug("ESQL query:\n{}", request.query());
        TimeSpanMarker parsingProfile = executionInfo.queryProfile().parsing();
        parsingProfile.start();
        EsqlStatement statement = parse(request);
        var viewResolution = viewResolver.replaceViews(
            statement.plan(),
            (query, viewName) -> EsqlParser.INSTANCE.parseView(
                query,
                request.params(),
                SettingsValidationContext.from(remoteClusterService),
                planTelemetry,
                inferenceService.inferenceSettings(),
                viewName
            ).plan()
        );
        parsingProfile.stop();
        PlanTimeProfile planTimeProfile = request.profile() ? new PlanTimeProfile() : null;

        ZoneId timeZone = request.timeZone() == null
            ? statement.setting(QuerySettings.TIME_ZONE)
            : statement.settingOrDefault(QuerySettings.TIME_ZONE, request.timeZone());

        Configuration configuration = new Configuration(
            timeZone,
            Instant.now(Clock.tick(Clock.system(timeZone), Duration.ofNanos(1))),
            request.locale() != null ? request.locale() : Locale.US,
            // TODO: plug-in security
            null,
            clusterName,
            request.pragmas(),
            analyzerSettings.resultTruncationMaxSize(),
            analyzerSettings.resultTruncationDefaultSize(),
            request.query(),
            request.profile(),
            request.tables(),
            System.nanoTime(),
            request.allowPartialResults(),
            analyzerSettings.timeseriesResultTruncationMaxSize(),
            analyzerSettings.timeseriesResultTruncationDefaultSize(),
            projectRouting(request, statement),
            viewResolution.viewQueries()
        );
        final FoldContext foldContext = configuration.newFoldContext();

        LogicalPlan plan = viewResolution.plan();
        if (plan instanceof Explain explain) {
            explainMode = true;
            plan = explain.query();
            parsedPlanString = plan.toString();
        }

        final EsqlStatement statementFinal = statement;
        analyzedPlan(
            plan,
            statement.setting(UNMAPPED_FIELDS),
            configuration,
            executionInfo,
            request.filter(),
            new EsqlCCSUtils.CssPartialErrorsActionListener(configuration, executionInfo, listener) {
                @Override
                public void onResponse(Versioned<LogicalPlan> analyzedPlan) {
                    assert ThreadPool.assertCurrentThreadPool(
                        ThreadPool.Names.SEARCH,
                        ThreadPool.Names.SEARCH_COORDINATION,
                        ThreadPool.Names.SYSTEM_READ
                    );

                    LogicalPlan plan = analyzedPlan.inner();
                    TransportVersion minimumVersion = analyzedPlan.minimumVersion();

                    var logicalPlanPreOptimizer = new LogicalPlanPreOptimizer(
                        new LogicalPreOptimizerContext(foldContext, inferenceService, minimumVersion)
                    );
                    var logicalPlanOptimizer = new LogicalPlanOptimizer(
                        new LogicalOptimizerContext(configuration, foldContext, minimumVersion)
                    );

                    SubscribableListener.<LogicalPlan>newForked(l -> preOptimizedPlan(plan, logicalPlanPreOptimizer, planTimeProfile, l))
                        .<LogicalPlan>andThen((l, p) -> {
                            if (statementFinal.setting(QuerySettings.APPROXIMATION) != null) {
                                Approximation.verifyPlan(p);
                            }
                            l.onResponse(p);
                        })
                        .<LogicalPlan>andThen(
                            (l, p) -> preMapper.preMapper(
                                new Versioned<>(optimizedPlan(p, logicalPlanOptimizer, planTimeProfile), minimumVersion),
                                l
                            )
                        )
                        .<Result>andThen(
                            (l, p) -> executeOptimizedPlan(
                                request,
                                statementFinal,
                                executionInfo,
                                planRunner,
                                p,
                                configuration,
                                foldContext,
                                minimumVersion,
                                planTimeProfile,
                                l
                            )
                        )
                        .<Versioned<Result>>andThen((l, r) -> l.onResponse(new Versioned<>(r, minimumVersion)))
                        .addListener(listener);
                }
            }
        );
    }

    private String projectRouting(EsqlQueryRequest request, EsqlStatement statement) {
        String projectRouting = statement.setting(QuerySettings.PROJECT_ROUTING);
        if (projectRouting == null) {
            projectRouting = request.projectRouting();
        }

        if (projectRouting != null && crossProjectModeDecider.crossProjectEnabled() == false) {
            throw new VerificationException("[project_routing] is only allowed when cross-project search is enabled");
        }
        return projectRouting;
    }

    /**
     * Execute an analyzed plan. Most code should prefer calling {@link #execute} but
     * this is public for testing.
     */
    public void executeOptimizedPlan(
        EsqlQueryRequest request,
        EsqlStatement statement,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        LogicalPlan optimizedPlan,
        Configuration configuration,
        FoldContext foldContext,
        TransportVersion minimumVersion,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ
        );
        var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, minimumVersion));

        if (explainMode) {// TODO: INLINE STATS come back to the explain mode branch and reevaluate
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request, physicalPlanOptimizer, planTimeProfile);
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
            physicalPlan = new LocalSourceExec(Source.EMPTY, fields, LocalSupplier.of(new Page(blocks)));
            planRunner.run(physicalPlan, configuration, foldContext, planTimeProfile, listener);
        } else {
            // TODO: this could be snuck into the underlying listener
            EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);
            // execute any potential subplans
            executeSubPlans(
                optimizedPlan,
                configuration,
                foldContext,
                minimumVersion,
                planRunner,
                executionInfo,
                request,
                statement,
                physicalPlanOptimizer,
                planTimeProfile,
                listener
            );
        }
    }

    private void executeSubPlans(
        LogicalPlan optimizedPlan,
        Configuration configuration,
        FoldContext foldContext,
        TransportVersion minimumVersion,
        PlanRunner runner,
        EsqlExecutionInfo executionInfo,
        EsqlQueryRequest request,
        EsqlStatement statement,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlan = firstSubPlan(optimizedPlan, subPlansResults);

        // TODO: merge into one method
        if (subPlan != null) {
            // code-path to execute subplans
            executeSubPlan(
                new DriverCompletionInfo.Accumulator(),
                optimizedPlan,
                subPlan,
                configuration,
                foldContext,
                executionInfo,
                runner,
                request,
                subPlansResults,
                physicalPlanOptimizer,
                planTimeProfile,
                // Ensure we don't have subplan flag stuck in there on failure
                ActionListener.runAfter(listener, executionInfo::finishSubPlans)
            );
        } else if (statement.setting(QuerySettings.APPROXIMATION) != null) {
            // TODO: unify the subplan execution of query approximation and joins.
            new Approximation(
                optimizedPlan,
                statement.setting(QuerySettings.APPROXIMATION),
                executionInfo,
                p -> logicalPlanToPhysicalPlan(p, request, physicalPlanOptimizer, planTimeProfile),
                runner,
                configuration,
                foldContext,
                minimumVersion,
                planTimeProfile
            ).approximate(listener);
        } else {
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request, physicalPlanOptimizer, planTimeProfile);
            // execute main plan
            runner.run(physicalPlan, configuration, foldContext, planTimeProfile, listener);
        }
    }

    private void executeSubPlan(
        DriverCompletionInfo.Accumulator completionInfoAccumulator,
        LogicalPlan optimizedPlan,
        InlineJoin.LogicalPlanTuple subPlans,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo executionInfo,
        PlanRunner runner,
        EsqlQueryRequest request,
        Set<LocalRelation> subPlansResults,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        LOGGER.debug("Executing subplan:\n{}", subPlans.stubReplacedSubPlan());
        // Create a physical plan out of the logical sub-plan
        var physicalSubPlan = logicalPlanToPhysicalPlan(subPlans.stubReplacedSubPlan(), request, physicalPlanOptimizer, planTimeProfile);

        executionInfo.startSubPlans();

        runner.run(physicalSubPlan, configuration, foldContext, planTimeProfile, listener.delegateFailureAndWrap((next, result) -> {
            AtomicReference<Page> localRelationPage = new AtomicReference<>();
            try {
                // Translate the subquery into a separate, coordinator based plan and the results 'broadcasted' as a local relation
                completionInfoAccumulator.accumulate(result.completionInfo());
                LocalRelation resultWrapper = resultToPlan(subPlans.stubReplacedSubPlan().source(), result);
                localRelationPage.set(resultWrapper.supplier().get());
                var releasingNext = ActionListener.runAfter(next, () -> releaseLocalRelationBlocks(localRelationPage));
                subPlansResults.add(resultWrapper);

                // replace the original logical plan with the backing result
                LogicalPlan newMainPlan = newMainPlan(optimizedPlan, subPlans, resultWrapper);

                // look for the next inlinejoin plan
                var newSubPlan = firstSubPlan(newMainPlan, subPlansResults);

                if (newSubPlan == null) {// run the final "main" plan
                    executionInfo.finishSubPlans();
                    LOGGER.debug("Executing final plan:\n{}", newMainPlan);
                    var newPhysicalPlan = logicalPlanToPhysicalPlan(newMainPlan, request, physicalPlanOptimizer, planTimeProfile);
                    runner.run(
                        newPhysicalPlan,
                        configuration,
                        foldContext,
                        planTimeProfile,
                        releasingNext.delegateFailureAndWrap((finalListener, finalResult) -> {
                            completionInfoAccumulator.accumulate(finalResult.completionInfo());
                            finalListener.onResponse(
                                new Result(
                                    finalResult.schema(),
                                    finalResult.pages(),
                                    configuration,
                                    completionInfoAccumulator.finish(),
                                    executionInfo
                                )
                            );
                        })
                    );
                } else {// continue executing the subplans
                    executeSubPlan(
                        completionInfoAccumulator,
                        newMainPlan,
                        newSubPlan,
                        configuration,
                        foldContext,
                        executionInfo,
                        runner,
                        request,
                        subPlansResults,
                        physicalPlanOptimizer,
                        planTimeProfile,
                        releasingNext
                    );
                }
            } catch (Exception e) {
                // safely release the blocks in case an exception occurs either before, but also after the "final" runner.run() forks off
                // the current thread, but with the blocks still referenced
                releaseLocalRelationBlocks(localRelationPage);
                throw e;
            } finally {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(result.pages().iterator(), p -> p::releaseBlocks)));
            }
        }));
    }

    public static LogicalPlan newMainPlan(LogicalPlan optimizedPlan, InlineJoin.LogicalPlanTuple subPlans, LocalRelation resultWrapper) {
        LogicalPlan newLogicalPlan = optimizedPlan.transformUp(
            InlineJoin.class,
            // use object equality since the right-hand side shouldn't have changed in the optimizedPlan at this point
            // and equals would have ignored name IDs anyway
            ij -> ij.right() == subPlans.originalSubPlan() ? InlineJoin.inlineData(ij, resultWrapper) : ij
        );
        // TODO: INLINE STATS can we do better here and further optimize the plan AFTER one of the subplans executed?
        newLogicalPlan.setOptimized();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Main plan change after previous subplan execution:\n{}", NodeUtils.diffString(optimizedPlan, newLogicalPlan));
        }
        return newLogicalPlan;
    }

    private LocalRelation resultToPlan(Source planSource, Result result) {
        List<Page> pages = result.pages();
        checkPagesBelowSize(
            pages,
            plannerSettings.intermediateLocalRelationMaxSize(),
            actual -> "sub-plan execution results too large ["
                + ByteSizeValue.ofBytes(actual)
                + "] > "
                + plannerSettings.intermediateLocalRelationMaxSize()
        );
        List<Attribute> schema = result.schema();

        Block[] blocks = SessionUtils.fromPages(schema, pages, blockFactory);
        return new LocalRelation(planSource, schema, LocalSupplier.of(blocks.length == 0 ? new Page(0) : new Page(blocks)));
    }

    private static void releaseLocalRelationBlocks(AtomicReference<Page> localRelationPage) {
        Page relationPage = localRelationPage.getAndSet(null);
        if (relationPage != null) {
            Releasables.closeExpectNoException(relationPage);
        }
    }

    private EsqlStatement parse(EsqlQueryRequest request) {
        return EsqlParser.INSTANCE.parse(
            request.query(),
            request.params(),
            SettingsValidationContext.from(remoteClusterService),
            planTelemetry,
            inferenceService.inferenceSettings()
        );
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
        Map<IndexPattern, IndexResolution> indexResolutions
    ) throws Exception {
        FailureCollector failureCollector = new FailureCollector();
        for (IndexResolution indexResolution : indexResolutions.values()) {
            handleFieldCapsFailures(allowPartialResults, executionInfo, indexResolution.failures(), failureCollector);
        }
        Exception failure = failureCollector.getFailure();
        if (failure != null) {
            throw failure;
        }
    }

    static void handleFieldCapsFailures(
        boolean allowPartialResults,
        EsqlExecutionInfo executionInfo,
        Map<String, List<FieldCapabilitiesFailure>> failures,
        FailureCollector failureCollector
    ) {
        for (var e : failures.entrySet()) {
            String clusterAlias = e.getKey();
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(clusterAlias);
            if (cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.RUNNING) {
                assert cluster.getStatus() != EsqlExecutionInfo.Cluster.Status.SUCCESSFUL : "can't mark a cluster success with failures";
            } else if (allowPartialResults == false && executionInfo.shouldSkipOnFailure(clusterAlias) == false) {
                for (FieldCapabilitiesFailure failure : e.getValue()) {
                    failureCollector.unwrapAndCollect(failure.getException());
                }
            } else if (cluster.getFailures().isEmpty()) {
                var shardFailures = e.getValue().stream().map(f -> {
                    ShardId shardId = null;
                    if (ExceptionsHelper.unwrapCause(f.getException()) instanceof ElasticsearchException es) {
                        shardId = es.getShardId();
                    }
                    return new ShardSearchFailure(
                        f.getException(),
                        shardId != null ? new SearchShardTarget(null, shardId, clusterAlias) : null
                    );
                }).toList();
                executionInfo.swapCluster(
                    clusterAlias,
                    (k, curr) -> new EsqlExecutionInfo.Cluster.Builder(cluster).addFailures(shardFailures).build()
                );
            }
        }
    }

    public void analyzedPlan(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        QueryBuilder requestFilter,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);

        TimeSpanMarker preAnalysisProfile = executionInfo.queryProfile().preAnalysis();
        preAnalysisProfile.start();
        PreAnalyzer.PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        preAnalysisProfile.stop();
        // Initialize the PreAnalysisResult with the local cluster's minimum transport version, so our planning will be correct also in
        // case of ROW queries. ROW queries can still require inter-node communication (for ENRICH and LOOKUP JOIN execution) with an older
        // node in the same cluster; so assuming that all nodes are on the same version as this node will be wrong and may cause bugs.
        PreAnalysisResult result = FieldNameUtils.resolveFieldNames(parsed, preAnalysis.enriches().isEmpty() == false)
            .withMinimumTransportVersion(localClusterMinimumVersion);
        String description = requestFilter == null ? "the only attempt without filter" : "first attempt with filter";

        resolveIndicesAndAnalyze(
            parsed,
            unmappedResolution,
            configuration,
            executionInfo,
            description,
            requestFilter,
            preAnalysis,
            result,
            logicalPlanListener
        );
    }

    private void resolveIndicesAndAnalyze(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        String description,
        QueryBuilder requestFilter,
        PreAnalyzer.PreAnalysis preAnalysis,
        PreAnalysisResult result,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        TimeSpanMarker dependencyResolutionProfile = executionInfo.queryProfile().dependencyResolution();
        dependencyResolutionProfile.start();
        SubscribableListener.<PreAnalysisResult>newForked(
            l -> preAnalyzeMainIndices(preAnalysis, configuration, executionInfo, result, requestFilter, l)
        ).andThenApply(r -> {
            if (r.indexResolution.isEmpty() == false // Rule out ROW case with no FROM clauses
                && executionInfo.isCrossClusterSearch()
                && executionInfo.getRunningClusterAliases().findAny().isEmpty()) {
                LOGGER.debug("No more clusters to search, ending analysis stage");
                throw new NoClustersToSearchException();
            }
            // Check if a subquery need to be pruned. If some but not all the subqueries has invalid index resolution,
            // try to prune it by setting IndexResolution to EMPTY_SUBQUERY. Analyzer.PruneEmptyUnionAllBranch will
            // take care of removing the subquery during analysis.
            // If all subqueries have invalid index resolution, we should fail in Analyzer's verifier.
            if (r.indexResolution.isEmpty() == false // it is not a row
                && r.indexResolution.size() > 1 // there is a subquery
                && executionInfo.isCrossClusterSearch()) {
                Collection<IndexResolution> indexResolutions = r.indexResolution.values();
                boolean hasInvalid = indexResolutions.stream().anyMatch(ir -> ir.isValid() == false);
                boolean hasValid = indexResolutions.stream().anyMatch(IndexResolution::isValid);
                // Only if there is partial invalid index resolutions in subqueries
                if (hasInvalid && hasValid) {
                    // iterate the index resolution and replace it with EMPTY_SUBQUERY if the index resolution is invalid
                    r.indexResolution.forEach((indexPattern, indexResolution) -> {
                        if (indexResolution.isValid() == false) {
                            LOGGER.debug("Index pattern [{}] does not match valid indices, pruning the subquery", indexPattern);
                            r.withIndices(indexPattern, IndexResolution.EMPTY_SUBQUERY);
                        }
                    });
                    // check if there is a cluster that does not have any valid index resolution, if so mark it as skipped
                    executionInfo.getRunningClusterAliases().forEach(clusterAlias -> {
                        boolean clusterHasValidIndex = r.indexResolution.values()
                            .stream()
                            .anyMatch(ir -> ir.isValid() && ir.get().originalIndices().get(clusterAlias) != null);
                        if (clusterHasValidIndex == false) {
                            String errorMsg = "no valid indices found in any subquery {}";
                            LOGGER.debug(errorMsg, EsqlCCSUtils.inClusterName(clusterAlias));
                            EsqlCCSUtils.markClusterWithFinalStateAndNoShards(
                                executionInfo,
                                clusterAlias,
                                EsqlExecutionInfo.Cluster.Status.SKIPPED,
                                new VerificationException(errorMsg, EsqlCCSUtils.inClusterName(clusterAlias))
                            );
                        }
                    });
                }
            }
            return r;
        })
            .<PreAnalysisResult>andThen((l, r) -> preAnalyzeLookupIndices(preAnalysis.lookupIndices().iterator(), r, executionInfo, l))
            .<PreAnalysisResult>andThen((l, r) -> {
                // Do not update PreAnalysisResult.minimumTransportVersion, that's already been determined during main index resolution.
                enrichPolicyResolver.resolvePolicies(
                    preAnalysis.enriches(),
                    executionInfo,
                    r.minimumTransportVersion(),
                    l.map(r::withEnrichResolution)
                );
            })
            .<PreAnalysisResult>andThen((l, r) -> {
                inferenceService.inferenceResolver(functionRegistry).resolveInferenceIds(parsed, l.map(r::withInferenceResolution));
            })
            .<Versioned<LogicalPlan>>andThen((l, r) -> {
                dependencyResolutionProfile.stop();
                analyzeWithRetry(parsed, unmappedResolution, configuration, executionInfo, description, requestFilter, preAnalysis, r, l);
            })
            .addListener(logicalPlanListener);
    }

    /**
     * Perform a field caps request for each lookup index. Does not update the minimum transport version.
     */
    private void preAnalyzeLookupIndices(
        Iterator<IndexPattern> lookupIndices,
        PreAnalysisResult preAnalysisResult,
        EsqlExecutionInfo executionInfo,
        ActionListener<PreAnalysisResult> listener
    ) {
        forAll(lookupIndices, preAnalysisResult, (lookupIndex, r, l) -> preAnalyzeLookupIndex(lookupIndex, r, executionInfo, l), listener);
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
            result.wildcardJoinIndices().contains(localPattern) ? IndexResolver.ALL_FIELDS : result.fieldNames,
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
        var localIndexNames = indexNames.stream().map(n -> RemoteClusterAware.splitIndexName(n)[1]).collect(toSet());
        if (localIndexNames.size() == 1) {
            String indexName = localIndexNames.iterator().next();
            EsIndex newIndex = new EsIndex(
                index,
                lookupIndexResolution.get().mapping(),
                Map.of(indexName, IndexMode.LOOKUP),
                Map.of(),
                Map.of(),
                Set.of()
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

    /**
     * Perform a field caps request for each index pattern and determine the minimum transport version of all clusters with matching
     * indices.
     */
    private void preAnalyzeMainIndices(
        PreAnalyzer.PreAnalysis preAnalysis,
        Configuration configuration,
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
                (e, r, l) -> preAnalyzeMainIndices(e.getKey(), e.getValue(), preAnalysis, executionInfo, r, requestFilter, l),
                listener
            );
        } else {
            forAll(
                preAnalysis.indexes().entrySet().iterator(),
                result,
                (e, r, l) -> preAnalyzeFlatMainIndices(
                    e.getKey(),
                    e.getValue(),
                    configuration.projectRouting(),
                    preAnalysis,
                    executionInfo,
                    r,
                    requestFilter,
                    l
                ),
                listener
            );
        }
    }

    private void preAnalyzeMainIndices(
        IndexPattern indexPattern,
        IndexMode indexMode,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
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
                result.fieldNames,
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
                indicesExpressionGrouper,
                listener.delegateFailureAndWrap((l, indexResolution) -> {
                    EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.inner().failures());
                    l.onResponse(
                        result.withIndices(indexPattern, indexResolution.inner())
                            .withMinimumTransportVersion(indexResolution.minimumVersion())
                    );
                })
            );
        }
    }

    private void preAnalyzeFlatMainIndices(
        IndexPattern indexPattern,
        IndexMode indexMode,
        String projectRouting,
        PreAnalyzer.PreAnalysis preAnalysis,
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        executionInfo.queryProfile().incFieldCapsCalls();
        indexResolver.resolveMainFlatWorldIndicesVersioned(
            indexPattern.indexPattern(),
            projectRouting,
            result.fieldNames,
            createQueryFilter(indexMode, requestFilter),
            indexMode == IndexMode.TIME_SERIES,
            // TODO: Same problem with subqueries as preAnalyzeMainIndices, see above.
            result.minimumTransportVersion(),
            preAnalysis.useAggregateMetricDoubleWhenNotSupported(),
            preAnalysis.useDenseVectorWhenNotSupported(),
            listener.delegateFailureAndWrap((l, indexResolution) -> {
                EsqlCCSUtils.initCrossClusterState(indexResolution.inner(), executionInfo);
                EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.inner().failures());
                EsqlCCSUtils.validateCcsLicense(verifier.licenseState(), executionInfo);
                planTelemetry.linkedProjectsCount(executionInfo.clusterInfo.size());
                l.onResponse(
                    result.withIndices(indexPattern, indexResolution.inner()).withMinimumTransportVersion(indexResolution.minimumVersion())
                );
            })
        );
    }

    private static QueryBuilder createQueryFilter(IndexMode indexMode, QueryBuilder requestFilter) {
        // Maybe if no indices are returned, retry without index mode and provide a clearer error message.
        return switch (indexMode) {
            case IndexMode.TIME_SERIES -> {
                var indexModeFilter = new TermQueryBuilder(IndexModeFieldMapper.NAME, IndexMode.TIME_SERIES.getName());
                yield requestFilter != null ? new BoolQueryBuilder().filter(requestFilter).filter(indexModeFilter) : indexModeFilter;
            }
            default -> requestFilter;
        };
    }

    private void analyzeWithRetry(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        String description,
        QueryBuilder requestFilter,
        PreAnalyzer.PreAnalysis preAnalysis,
        PreAnalysisResult result,
        ActionListener<Versioned<LogicalPlan>> listener
    ) {
        LOGGER.debug("Analyzing the plan ({})", description);
        try {
            if (result.indexResolution.values().stream().anyMatch(IndexResolution::isValid) || requestFilter != null) {
                // We won't run this check with no filter and no valid indices since this may lead to false positive - missing index report
                // when the resolution result is not valid for a different reason.
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(
                    executionInfo,
                    result.indexResolution.values(),
                    requestFilter != null
                );
            }
            TimeSpanMarker analysisProfile = executionInfo.queryProfile().analysis();
            analysisProfile.start();
            LogicalPlan plan = analyzedPlan(parsed, unmappedResolution, configuration, result, executionInfo);
            analysisProfile.stop();
            LOGGER.debug("Analyzed plan ({}):\n{}", description, plan);
            // the analysis succeeded from the first attempt, irrespective if it had a filter or not, just continue with the planning
            listener.onResponse(new Versioned<>(plan, result.minimumTransportVersion()));
        } catch (VerificationException ve) {
            LOGGER.debug("Analyzing the plan ({}) failed with {}", description, ve.getDetailedMessage());
            if (requestFilter == null) {
                // if the initial request didn't have a filter, then just pass the exception back to the user
                listener.onFailure(ve);
            } else {
                // retrying the index resolution without index filtering.
                executionInfo.clusterInfo.clear();
                resolveIndicesAndAnalyze(
                    parsed,
                    unmappedResolution,
                    configuration,
                    executionInfo,
                    "second attempt, without filter",
                    null,
                    preAnalysis,
                    result,
                    listener
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private PhysicalPlan logicalPlanToPhysicalPlan(
        LogicalPlan optimizedPlan,
        EsqlQueryRequest request,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile
    ) {
        PhysicalPlan physicalPlan = optimizedPhysicalPlan(optimizedPlan, physicalPlanOptimizer, planTimeProfile);
        physicalPlan = PlannerUtils.integrateEsFilterIntoFragment(physicalPlan, request.filter());
        return EstimatesRowSize.estimateRowSize(0, physicalPlan);
    }

    private LogicalPlan analyzedPlan(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        PreAnalysisResult r,
        EsqlExecutionInfo executionInfo
    ) throws Exception {
        handleFieldCapsFailures(configuration.allowPartialResults(), executionInfo, r.indexResolution());
        AnalyzerContext analyzerContext = new AnalyzerContext(configuration, functionRegistry, unmappedResolution, projectMetadata, r);
        Analyzer analyzer = new Analyzer(analyzerContext, verifier);
        LogicalPlan plan = analyzer.analyze(parsed);
        plan.setAnalyzed();
        return plan;
    }

    public LogicalPlan optimizedPlan(LogicalPlan logicalPlan, LogicalPlanOptimizer logicalPlanOptimizer, PlanTimeProfile planTimeProfile) {
        if (logicalPlan.preOptimized() == false) {
            throw new IllegalStateException("Expected pre-optimized plan");
        }
        long start = planTimeProfile == null ? 0L : System.nanoTime();
        var plan = logicalPlanOptimizer.optimize(logicalPlan);
        if (planTimeProfile != null) {
            planTimeProfile.addLogicalOptimizationPlanTime(System.nanoTime() - start);
        }
        LOGGER.debug("Optimized logicalPlan plan:\n{}", plan);
        return plan;
    }

    public void preOptimizedPlan(
        LogicalPlan logicalPlan,
        LogicalPlanPreOptimizer logicalPlanPreOptimizer,
        PlanTimeProfile planTimeProfile,
        ActionListener<LogicalPlan> listener
    ) {
        long start = planTimeProfile == null ? 0L : System.nanoTime();
        logicalPlanPreOptimizer.preOptimize(logicalPlan, listener.delegateResponse((l, e) -> { l.onFailure(e); }).map(plan -> {
            if (planTimeProfile != null) {
                planTimeProfile.addLogicalOptimizationPlanTime(System.nanoTime() - start);
            }
            return plan;
        }));
    }

    private PhysicalPlan physicalPlan(Versioned<LogicalPlan> optimizedPlan) {
        LogicalPlan logicalPlan = optimizedPlan.inner();
        if (logicalPlan.optimized() == false) {
            throw new IllegalStateException("Expected optimized plan");
        }
        optimizedLogicalPlanString = logicalPlan.toString();
        PhysicalPlan plan = mapper.map(optimizedPlan);
        LOGGER.debug("Physical plan:\n{}", plan);
        return plan;
    }

    private PhysicalPlan optimizedPhysicalPlan(
        LogicalPlan optimizedPlan,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile
    ) {
        long start = planTimeProfile == null ? 0L : System.nanoTime();
        var plan = physicalPlanOptimizer.optimize(
            physicalPlan(new Versioned<>(optimizedPlan, physicalPlanOptimizer.context().minimumVersion()))
        );
        if (planTimeProfile != null) {
            planTimeProfile.addPhysicalOptimizationPlanTime(System.nanoTime() - start);
        }
        LOGGER.debug("Optimized physical plan:\n{}", plan);
        return plan;
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

    public record PreAnalysisResult(
        Set<String> fieldNames,
        Set<String> wildcardJoinIndices,
        Map<IndexPattern, IndexResolution> indexResolution,
        Map<String, IndexResolution> lookupIndices,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        TransportVersion minimumTransportVersion
    ) {

        public PreAnalysisResult(Set<String> fieldNames, Set<String> wildcardJoinIndices) {
            this(
                fieldNames,
                wildcardJoinIndices,
                new HashMap<>(),
                new HashMap<>(),
                null,
                InferenceResolution.EMPTY,
                TransportVersion.current()
            );
        }

        PreAnalysisResult withIndices(IndexPattern indexPattern, IndexResolution indices) {
            indexResolution.put(indexPattern, indices);
            return this;
        }

        PreAnalysisResult addLookupIndexResolution(String index, IndexResolution indexResolution) {
            lookupIndices.put(index, indexResolution);
            return this;
        }

        PreAnalysisResult withEnrichResolution(EnrichResolution enrichResolution) {
            return new PreAnalysisResult(
                fieldNames,
                wildcardJoinIndices,
                indexResolution,
                lookupIndices,
                enrichResolution,
                inferenceResolution,
                minimumTransportVersion
            );
        }

        PreAnalysisResult withInferenceResolution(InferenceResolution inferenceResolution) {
            return new PreAnalysisResult(
                fieldNames,
                wildcardJoinIndices,
                indexResolution,
                lookupIndices,
                enrichResolution,
                inferenceResolution,
                minimumTransportVersion
            );
        }

        PreAnalysisResult withMinimumTransportVersion(TransportVersion minimumTransportVersion) {
            if (this.minimumTransportVersion != null) {
                if (this.minimumTransportVersion.equals(minimumTransportVersion)) {
                    return this;
                }
                minimumTransportVersion = TransportVersion.min(this.minimumTransportVersion, minimumTransportVersion);
            }
            return new PreAnalysisResult(
                fieldNames,
                wildcardJoinIndices,
                indexResolution,
                lookupIndices,
                enrichResolution,
                inferenceResolution,
                minimumTransportVersion
            );
        }
    }
}
