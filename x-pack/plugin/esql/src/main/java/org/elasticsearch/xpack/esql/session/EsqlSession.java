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
import org.elasticsearch.transport.TransportService;
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
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
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
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
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
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
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
    private final ExternalSourceResolver externalSourceResolver;

    private final EsqlParser parser;
    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final Metrics metrics;
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
    private final TransportService transportService;

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
        ExternalSourceResolver externalSourceResolver,
        EsqlParser parser,
        PreAnalyzer preAnalyzer,
        EsqlFunctionRegistry functionRegistry,
        Mapper mapper,
        Verifier verifier,
        Metrics metrics,
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
        this.externalSourceResolver = externalSourceResolver;
        this.parser = parser;
        this.preAnalyzer = preAnalyzer;
        this.verifier = verifier;
        this.metrics = metrics;
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
        this.transportService = services.transportService();
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
        LOGGER.debug("ESQL query:\n{}", request.queryDescription());
        TimeSpanMarker parsingProfile = executionInfo.queryProfile().parsing();
        parsingProfile.start();
        EsqlStatement statement = parse(request);
        gatherSettingsMetrics(statement);
        parsingProfile.stop();
        TimeSpanMarker viewResolutionProfile = executionInfo.queryProfile().viewResolution();
        viewResolutionProfile.start();
        viewResolver.replaceViews(
            statement.plan(),
            (query, viewName) -> parser.parseView(
                query,
                request.params(),
                SettingsValidationContext.from(remoteClusterService),
                inferenceService.inferenceSettings(),
                viewName
            ).plan(),
            listener.delegateFailureAndWrap((l, viewResolution) -> {
                viewResolutionProfile.stop();
                analyseAndExecute(request, executionInfo, planRunner, statement, viewResolution, l);
            })
        );
    }

    private void analyseAndExecute(
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        EsqlStatement statement,
        ViewResolver.ViewResolutionResult viewResolution,
        ActionListener<Versioned<Result>> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);

        // this is stack telemetry
        gatherViewMetrics(viewResolution);

        // this is APM
        gatherPlanTelemetry(viewResolution.plan(), statement.settings());

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
            approximationSettings(request, statement),
            viewResolution.viewQueries()
        );

        LogicalPlan plan = viewResolution.plan();
        Configuration configurationToUse = configuration;
        if (plan instanceof Explain explain) {
            explainMode = true;
            plan = explain.query();
            parsedPlanString = plan.toString();
            // For EXPLAIN mode, enable profile to capture plans from all nodes
            configurationToUse = configuration.withExplainOnly();
        }
        final Configuration finalConfiguration = configurationToUse;
        final FoldContext foldContext = finalConfiguration.newFoldContext();

        analyzedPlan(
            plan,
            statement.setting(UNMAPPED_FIELDS),
            finalConfiguration,
            executionInfo,
            request.filter(),
            new EsqlCCSUtils.CssPartialErrorsActionListener(finalConfiguration, executionInfo, listener) {
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
                        new LogicalOptimizerContext(finalConfiguration, foldContext, minimumVersion)
                    );

                    SubscribableListener.<LogicalPlan>newForked(l -> preOptimizedPlan(plan, logicalPlanPreOptimizer, planTimeProfile, l))
                        .<LogicalPlan>andThen(
                            (l, p) -> preMapper.preMapper(
                                new Versioned<>(optimizedPlan(p, logicalPlanOptimizer, planTimeProfile), minimumVersion),
                                l
                            )
                        )
                        .<Result>andThen(
                            (l, p) -> executeOptimizedPlan(
                                request,
                                executionInfo,
                                planRunner,
                                p,
                                finalConfiguration,
                                foldContext,
                                new Holder<Approximation>(),
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

    private ApproximationSettings approximationSettings(EsqlQueryRequest request, EsqlStatement statement) {
        // The precedence for settings is: SET in the statement > request parameter > default (=disabled).
        ApproximationSettings settings = new ApproximationSettings.Builder(false).merge(request.approximation())
            .merge(statement.setting(QuerySettings.APPROXIMATION))
            .build();
        if (settings != null) {
            EsqlLicenseChecker.checkQueryApproximation(verifier.licenseState());
        }
        return settings;
    }

    /**
     * Execute an analyzed plan. Most code should prefer calling {@link #execute} but
     * this is public for testing.
     */
    private void executeOptimizedPlan(
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        LogicalPlan optimizedPlan,
        Configuration configuration,
        FoldContext foldContext,
        Holder<Approximation> approximation,
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

        EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);

        // In explain mode, wrap the listener to transform results into EXPLAIN table format.
        // We use the same execution path as normal queries to ensure accuracy.
        ActionListener<Result> effectiveListener = explainMode
            ? createExplainListener(listener, optimizedPlan, request, physicalPlanOptimizer, planTimeProfile, configuration, planRunner)
            : listener;

        // Always use the same execution path - executeSubPlans handles both simple queries and those with subplans
        executeSubPlans(
            optimizedPlan,
            configuration,
            foldContext,
            approximation,
            planRunner,
            executionInfo,
            request,
            physicalPlanOptimizer,
            planTimeProfile,
            effectiveListener
        );
    }

    /**
     * Creates a listener that intercepts execution results and transforms them into EXPLAIN output.
     * This ensures EXPLAIN shows exactly the same plans that would be executed.
     */
    private ActionListener<Result> createExplainListener(
        ActionListener<Result> delegate,
        LogicalPlan optimizedPlan,
        EsqlQueryRequest request,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile,
        Configuration configuration,
        PlanRunner planRunner
    ) {
        // Capture the coordinator physical plan string before execution
        PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request, physicalPlanOptimizer, planTimeProfile);
        String physicalPlanString = physicalPlan.toString();

        // Capture subplan information before execution (for INLINE STATS, LOOKUP JOIN)
        List<List<Object>> subplanValues = new ArrayList<>();
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlan = InlineJoin.firstSubPlan(optimizedPlan, subPlansResults);
        if (subPlan != null) {
            int subPlanIndex = 0;
            InlineJoin.LogicalPlanTuple currentSubPlan = subPlan;
            while (currentSubPlan != null) {
                String subPlanStr = currentSubPlan.stubReplacedSubPlan().toString();
                subplanValues.add(List.of("", clusterName, "subplan-" + subPlanIndex, "logicalPlan", subPlanStr));
                PhysicalPlan subPhysicalPlan = logicalPlanToPhysicalPlan(
                    currentSubPlan.stubReplacedSubPlan(),
                    request,
                    physicalPlanOptimizer,
                    planTimeProfile
                );
                subplanValues.add(List.of("", clusterName, "subplan-" + subPlanIndex, "physicalPlan", subPhysicalPlan.toString()));
                subPlanIndex++;
                currentSubPlan = InlineJoin.firstSubPlan(currentSubPlan.stubReplacedSubPlan(), subPlansResults);
            }
        }

        return delegate.delegateFailureAndWrap((next, result) -> {
            List<List<Object>> values = Collections.synchronizedList(new ArrayList<>());
            String localCluster = "";
            String coordinatorNode = clusterName;

            // Add coordinator plans (captured before execution)
            values.add(List.of(localCluster, coordinatorNode, "coordinator", "parsedPlan", parsedPlanString));
            values.add(List.of(localCluster, coordinatorNode, "coordinator", "optimizedLogicalPlan", optimizedLogicalPlanString));
            values.add(List.of(localCluster, coordinatorNode, "coordinator", "optimizedPhysicalPlan", physicalPlanString));

            // Add subplan information (captured before execution)
            values.addAll(subplanValues);

            // Extract plans from profile data (captured during execution)
            // This includes: data node plans, node_reduce plans, and final coordinator plans
            if (result.completionInfo() != null && result.completionInfo().planProfiles() != null) {
                for (var planProfile : result.completionInfo().planProfiles()) {
                    String cluster = planProfile.clusterName() != null ? planProfile.clusterName() : "";
                    String node = planProfile.nodeName() != null ? planProfile.nodeName() : "";
                    String planTree = planProfile.planTree() != null ? planProfile.planTree() : "";
                    String logicalPlanTree = planProfile.logicalPlanTree() != null ? planProfile.logicalPlanTree() : "";
                    String description = planProfile.description();

                    if (ComputeService.DATA_DESCRIPTION.equals(description)) {
                        if (logicalPlanTree.isEmpty() == false) {
                            values.add(List.of(cluster, node, "data", "optimizedLocalLogicalPlan", logicalPlanTree));
                        }
                        values.add(List.of(cluster, node, "data", "localPhysicalPlan", planTree));
                    } else if (ComputeService.REDUCE_DESCRIPTION.equals(description)) {
                        values.add(List.of(cluster, node, "node_reduce", "physicalPlan", planTree));
                    } else if (description != null && description.endsWith("final")) {
                        values.add(List.of(cluster, node, "final", "physicalPlan", planTree));
                    }
                }
            }

            // Release pages from the intermediate result
            for (var page : result.pages()) {
                page.releaseBlocks();
            }

            // Build and return the EXPLAIN table
            finishExplain(values, configuration, configuration.newFoldContext(), planTimeProfile, planRunner, next);
        });
    }

    private void finishExplain(
        List<List<Object>> values,
        Configuration configuration,
        FoldContext foldContext,
        PlanTimeProfile planTimeProfile,
        PlanRunner planRunner,
        ActionListener<Result> listener
    ) {
        var blocks = BlockUtils.fromList(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values);
        PhysicalPlan resultPlan = new LocalSourceExec(Source.EMPTY, Explain.OUTPUT_ATTRIBUTES, LocalSupplier.of(new Page(blocks)));
        planRunner.run(resultPlan, configuration, foldContext, planTimeProfile, listener);
    }

    private void executeSubPlans(
        LogicalPlan optimizedPlan,
        Configuration configuration,
        FoldContext foldContext,
        Holder<Approximation> approximation,
        PlanRunner runner,
        EsqlExecutionInfo executionInfo,
        EsqlQueryRequest request,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        var subPlansResults = new HashSet<LocalRelation>();
        var subPlan = firstSubPlan(optimizedPlan, configuration, approximation, subPlansResults);

        // TODO: merge into one method
        if (subPlan != null) {
            // code-path to execute subplans
            executeSubPlan(
                new DriverCompletionInfo.Accumulator(),
                optimizedPlan,
                subPlan,
                configuration,
                foldContext,
                approximation,
                executionInfo,
                runner,
                request,
                subPlansResults,
                physicalPlanOptimizer,
                planTimeProfile,
                // Ensure we don't have subplan flag stuck in there on failure
                ActionListener.runAfter(listener, executionInfo::finishSubPlans)
            );
        } else {
            PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request, physicalPlanOptimizer, planTimeProfile);
            // execute main plan
            runner.run(physicalPlan, configuration, foldContext, planTimeProfile, listener);
        }
    }

    /**
     * Returns a subplan if one has to be executed, or null otherwise.
     * @param subPlan     first subplan that needs to be executed
     * @param newMainPlan callback to build the new main plan based on the subplan results
     * @param cleanup     callback to release any resources hold by the subplan results
     */
    private record SubPlanAndCallback(
        LogicalPlan subPlan,
        java.util.function.Function<Result, LogicalPlan> newMainPlan,
        Runnable cleanup
    ) {};

    private SubPlanAndCallback firstSubPlan(
        LogicalPlan mainPlan,
        Configuration configuration,
        Holder<Approximation> approximation,
        Set<LocalRelation> subPlansResults
    ) {
        SubPlanAndCallback subPlanAndCallback = null;

        // InlineJoin must be first, because approximation may need to approximate a subplan of it.
        InlineJoin.LogicalPlanTuple subPlans = InlineJoin.firstSubPlan(mainPlan, subPlansResults);
        if (subPlans != null) {
            AtomicReference<Page> localRelationPage = new AtomicReference<>();
            subPlanAndCallback = new SubPlanAndCallback(subPlans.stubReplacedSubPlan(), result -> {
                // Translate the subquery into a separate, coordinator based plan and the results 'broadcasted' as a local relation
                LocalRelation resultWrapper = resultToPlan(subPlans.stubReplacedSubPlan().source(), result);
                localRelationPage.set(resultWrapper.supplier().get());
                subPlansResults.add(resultWrapper);
                return InlineJoin.newMainPlan(mainPlan, subPlans, resultWrapper);
            }, () -> releaseLocalRelationBlocks(localRelationPage));
        }

        LogicalPlan plan = subPlanAndCallback != null ? subPlanAndCallback.subPlan : mainPlan;
        if (ApproximationPlan.is(plan)) {
            if (approximation.get() == null) {
                approximation.set(new Approximation(plan, configuration.approximationSettings()));
            }
            LogicalPlan subPlan = approximation.get().firstSubPlan();
            if (subPlan != null) {
                subPlanAndCallback = new SubPlanAndCallback(subPlan, result -> {
                    Double sampleProbability = approximation.get().processResult(result);
                    if (sampleProbability != null) {
                        return ApproximationPlan.substituteSampleProbability(mainPlan, sampleProbability);
                    } else {
                        return mainPlan;
                    }
                }, () -> {});
            }
        }

        return subPlanAndCallback;
    }

    private void executeSubPlan(
        DriverCompletionInfo.Accumulator completionInfoAccumulator,
        LogicalPlan optimizedPlan,
        SubPlanAndCallback subPlan,
        Configuration configuration,
        FoldContext foldContext,
        Holder<Approximation> approximation,
        EsqlExecutionInfo executionInfo,
        PlanRunner runner,
        EsqlQueryRequest request,
        Set<LocalRelation> subPlansResults,
        PhysicalPlanOptimizer physicalPlanOptimizer,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        LOGGER.debug("Executing subplan:\n{}", subPlan.subPlan);
        // Create a physical plan out of the logical sub-plan
        var physicalSubPlan = logicalPlanToPhysicalPlan(subPlan.subPlan, request, physicalPlanOptimizer, planTimeProfile);

        executionInfo.startSubPlans();

        runner.run(physicalSubPlan, configuration, foldContext, planTimeProfile, listener.delegateFailureAndWrap((next, result) -> {
            completionInfoAccumulator.accumulate(result.completionInfo());
            try {
                var releasingNext = ActionListener.runAfter(next, subPlan.cleanup);
                LogicalPlan newMainPlan = subPlan.newMainPlan.apply(result);

                // look for the next inlinejoin plan
                var newSubPlan = firstSubPlan(newMainPlan, configuration, approximation, subPlansResults);

                if (newSubPlan == null) {// run the final "main" plan
                    executionInfo.finishSubPlans();
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
                        approximation,
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
                subPlan.cleanup.run();
                throw e;
            } finally {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(result.pages().iterator(), p -> p::releaseBlocks)));
            }
        }));
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
        return request.parse(parser, SettingsValidationContext.from(remoteClusterService), inferenceService.inferenceSettings());
    }

    /**
     * Populates {@code planTelemetry} from the view-resolved plan, capturing commands, functions,
     * and settings from the original statement plus any nodes introduced by view expansion.
     */
    private void gatherPlanTelemetry(LogicalPlan plan, List<QuerySetting> settings) {
        EsqlFunctionRegistry registry = planTelemetry.functionRegistry().snapshotRegistry();
        // Collect Aggregate nodes that are the inner aggregate of an INLINE STATS command; those
        // should not be counted as standalone STATS commands.
        Set<LogicalPlan> inlineStatsAggregates = new HashSet<>();
        plan.forEachDown(InlineStats.class, inlineStats -> inlineStatsAggregates.add(inlineStats.aggregate()));
        plan.forEachDown(node -> {
            if (node instanceof TelemetryAware ta && ta.telemetryLabel() != null && inlineStatsAggregates.contains(node) == false) {
                // Not all TelemetryAware nodes have a label — e.g. lookup index UnresolvedRelation
                // nodes introduced by LOOKUP JOIN have a null command name; those are skipped.
                // Aggregate nodes that are the inner aggregate of INLINE STATS are also skipped —
                // the INLINE STATS node itself is counted instead.
                planTelemetry.command(ta);
            }
            node.forEachExpression(Function.class, f -> {
                if (f instanceof UnresolvedFunction uf) {
                    planTelemetry.function(uf.name());
                } else if (registry.functionExists(f.getClass())) {
                    // Concrete Function instances arise from inline cast expressions (e.g. x::long)
                    // or programmatically-built plans (e.g. Prometheus plan builders).
                    // Not all Function subclasses are user-callable (e.g. Add, GreaterThan are
                    // Function subclasses but are never in the registry); those are skipped.
                    planTelemetry.function(f.getClass());
                }
            });
        });
        if (settings != null) {
            settings.forEach(s -> planTelemetry.setting(s.name()));
        }
    }

    private void gatherSettingsMetrics(EsqlStatement statement) {
        if (metrics == null || statement.settings() == null) {
            return;
        }
        // Deduplicate settings by name - if the same setting is SET multiple times in a query,
        // we only count it once for telemetry purposes.
        // The Metrics class only registers counters for settings applicable to the current environment
        // (e.g., snapshot-only settings are not registered in non-snapshot builds).
        // incSetting() silently ignores settings that don't have a registered counter.
        statement.settings().stream().map(QuerySetting::name).distinct().forEach(metrics::incSetting);
    }

    private void gatherViewMetrics(ViewResolver.ViewResolutionResult viewResolution) {
        if (metrics == null || viewResolution.viewQueries().isEmpty()) {
            return;
        }
        metrics.inc(FeatureMetric.VIEW);
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

    private void analyzedPlan(
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
        PreAnalysisResult result = FieldNameUtils.resolveFieldNames(
            parsed,
            preAnalysis.enriches().isEmpty() == false,
            unmappedResolution == UnmappedResolution.LOAD
        ).withMinimumTransportVersion(localClusterMinimumVersion);
        String description = requestFilter == null ? "the only attempt without filter" : "first attempt with filter";
        // Extract timestamp bounds eagerly from the request filter so they can be threaded through to the analyzer,
        // even when index resolution is retried without the filter (e.g. because the filter covers an empty time range).
        // Extraction uses configuration::absoluteStartedTimeInMillis (fixed at request start), so it is safe to do here.
        TimestampBounds timestampBounds = QueryDslTimestampBoundsExtractor.extractTimestampBounds(
            requestFilter,
            configuration::absoluteStartedTimeInMillis
        );

        resolveIndicesAndAnalyze(
            parsed,
            unmappedResolution,
            configuration,
            executionInfo,
            description,
            requestFilter,
            timestampBounds,
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
        TimestampBounds timestampBounds,
        PreAnalyzer.PreAnalysis preAnalysis,
        PreAnalysisResult result,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        executionInfo.queryProfile().indicesResolutionMarker().start();
        // TODO this is a quick hack to alleviate the pressure off of https://github.com/elastic/elasticsearch/issues/145920. A btter
        // solution would be to just not track the unmapped indices at all, but that requires a more structural change.
        boolean trackedUnmappedFieldIndices = unmappedResolution == UnmappedResolution.LOAD || parsed.anyMatch(p -> p instanceof Insist);
        SubscribableListener.<PreAnalysisResult>newForked(
            l -> preAnalyzeMainIndices(preAnalysis, configuration, executionInfo, trackedUnmappedFieldIndices, result, requestFilter, l)
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
            .andThenApply(r -> {
                executionInfo.queryProfile().indicesResolutionMarker().stop();
                return r;
            })
            .<PreAnalysisResult>andThen((l, r) -> preAnalyzeExternalSources(parsed, preAnalysis, r, l))
            .<PreAnalysisResult>andThen((l, r) -> {
                // Do not update PreAnalysisResult.minimumTransportVersion, that's already been determined during main index resolution.
                executionInfo.queryProfile().enrichResolutionMarker().start();
                enrichPolicyResolver.resolvePolicies(
                    preAnalysis.enriches(),
                    executionInfo,
                    r.minimumTransportVersion(),
                    l.delegateFailureAndWrap((ll, enrichResolution) -> {
                        executionInfo.queryProfile().enrichResolutionMarker().stop();
                        ll.onResponse(r.withEnrichResolution(enrichResolution));
                    })
                );
            })
            .<PreAnalysisResult>andThen((l, r) -> {
                executionInfo.queryProfile().inferenceResolutionMarker().start();
                inferenceService.inferenceResolver(functionRegistry)
                    .resolveInferenceIds(parsed, l.delegateFailureAndWrap((ll, inferenceResolution) -> {
                        executionInfo.queryProfile().inferenceResolutionMarker().stop();
                        ll.onResponse(r.withInferenceResolution(inferenceResolution));
                    }));
            })
            .<Versioned<LogicalPlan>>andThen((l, r) -> {
                analyzeWithRetry(
                    parsed,
                    unmappedResolution,
                    configuration,
                    executionInfo,
                    description,
                    requestFilter,
                    timestampBounds,
                    preAnalysis,
                    r,
                    l
                );
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

    /**
     * Resolve external sources (Iceberg tables/Parquet files) if present in the query.
     * This runs in parallel with other resolution steps to avoid blocking.
     * Extracts partition filter hints from the WHERE clause for partition-aware glob rewriting.
     */
    private void preAnalyzeExternalSources(
        LogicalPlan plan,
        PreAnalyzer.PreAnalysis preAnalysis,
        PreAnalysisResult result,
        ActionListener<PreAnalysisResult> listener
    ) {
        if (preAnalysis.icebergPaths().isEmpty()) {
            listener.onResponse(result);
            return;
        }

        Map<String, Map<String, Expression>> pathParams = extractIcebergParams(plan);

        var filterHints = PartitionFilterHintExtractor.extract(plan);

        externalSourceResolver.resolve(
            preAnalysis.icebergPaths(),
            pathParams,
            filterHints.isEmpty() ? null : filterHints,
            listener.map(result::withExternalSourceResolution)
        );
    }

    /**
     * Extract external source parameters from UnresolvedExternalRelation nodes in the plan.
     * Returns a map from table path to parameter map.
     */
    private Map<String, Map<String, Expression>> extractIcebergParams(LogicalPlan plan) {
        Map<String, Map<String, Expression>> pathParams = new HashMap<>();
        plan.forEachUp(org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation.class, p -> {
            if (p.tablePath() instanceof org.elasticsearch.xpack.esql.core.expression.Literal literal && literal.value() != null) {
                // Use BytesRefs.toString() which handles both BytesRef and String
                String path = org.elasticsearch.common.lucene.BytesRefs.toString(literal.value());
                pathParams.put(path, p.params());
            }
        });
        return pathParams;
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

    /**
     * Perform a field caps request for each index pattern and determine the minimum transport version of all clusters with matching
     * indices.
     */
    private void preAnalyzeMainIndices(
        PreAnalyzer.PreAnalysis preAnalysis,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        boolean trackUnmappedFieldIndices,
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
            forAll(
                preAnalysis.indexes().entrySet().iterator(),
                result,
                (e, r, l) -> preAnalyzeFlatMainIndices(
                    e.getKey(),
                    e.getValue(),
                    configuration.projectRouting(),
                    preAnalysis,
                    executionInfo,
                    trackUnmappedFieldIndices,
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
                            result.fieldNames,
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
                    indexResolver.resolveMainFlatWorldIndicesVersioned(
                        indexPattern.indexPattern(),
                        projectRouting,
                        result.fieldNames,
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

    private void analyzeWithRetry(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        String description,
        QueryBuilder requestFilter,
        TimestampBounds timestampBounds,
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
            LogicalPlan plan = analyzedPlan(parsed, unmappedResolution, configuration, result, executionInfo, timestampBounds);
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
                    timestampBounds,
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
        EsqlExecutionInfo executionInfo,
        TimestampBounds timestampBounds
    ) throws Exception {
        handleFieldCapsFailures(configuration.allowPartialResults(), executionInfo, r.indexResolution());
        AnalyzerContext analyzerContext = new AnalyzerContext(
            configuration,
            functionRegistry,
            unmappedResolution,
            projectMetadata,
            r,
            timestampBounds
        );
        Analyzer analyzer = new Analyzer(analyzerContext, verifier);
        LogicalPlan plan = analyzer.analyze(parsed);
        plan.setAnalyzed();
        return plan;
    }

    private LogicalPlan optimizedPlan(LogicalPlan logicalPlan, LogicalPlanOptimizer logicalPlanOptimizer, PlanTimeProfile planTimeProfile) {
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

    private void preOptimizedPlan(
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
        ExternalSourceResolution externalSourceResolution,
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
                ExternalSourceResolution.EMPTY,
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
                externalSourceResolution,
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
                externalSourceResolution,
                minimumTransportVersion
            );
        }

        PreAnalysisResult withExternalSourceResolution(ExternalSourceResolution externalSourceResolution) {
            return new PreAnalysisResult(
                fieldNames,
                wildcardJoinIndices,
                indexResolution,
                lookupIndices,
                enrichResolution,
                inferenceResolution,
                externalSourceResolution,
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
                externalSourceResolution,
                minimumTransportVersion
            );
        }
    }
}
