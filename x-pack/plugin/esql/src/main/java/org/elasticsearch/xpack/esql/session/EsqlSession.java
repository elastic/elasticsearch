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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.IpLocationResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.anonymizer.PlanAnonymizer;
import org.elasticsearch.xpack.esql.approximation.ApproximationDriver;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalStatsRequirementExtractor;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.BucketColumnMetadata;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
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
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedIpLocation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.planner.premapper.PreMapper;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.telemetry.FeatureMetric;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.view.ViewCompaction;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;
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
    private final DatasetResolver datasetResolver;
    private final ExternalSourceResolver externalSourceResolver;

    private final EsqlParser parser;
    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final Metrics metrics;
    private final EsqlFunctionRegistry functionRegistry;
    private final PromqlFunctionRegistry promqlFunctionRegistry;
    private final AnalysisRegistry analysisRegistry;
    private final PreMapper preMapper;

    private final Mapper mapper;
    private final PlanTelemetry planTelemetry;
    private final IndicesExpressionGrouper indicesExpressionGrouper;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final InferenceService inferenceService;
    private final RemoteClusterService remoteClusterService;
    private final BlockFactory blockFactory;
    private final PlannerSettings plannerSettings;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final String clusterName;
    private final String clusterUuid;
    private final IpLocationService ipLocationService;

    private boolean explainMode;
    private String parsedPlanString;
    private String optimizedLogicalPlanString;
    private final ProjectMetadata projectMetadata;

    /**
     * Snapshot of the planning stages this session has completed so far. Read once by the failure-
     * path anonymized log to surface whichever stages succeeded before the failure. The pipeline is
     * sequential per session — each stage's listener callback fires after the previous completes, so
     * writes never overlap. A single volatile reference (vs four separate volatile fields) gives the
     * failure listener an atomic, all-or-nothing view of the snapshot.
     */
    private record PlanSnapshot(LogicalPlan parsed, LogicalPlan analyzed, LogicalPlan optimized, PhysicalPlan physical) {

        static final PlanSnapshot EMPTY = new PlanSnapshot(null, null, null, null);

        PlanSnapshot withParsed(LogicalPlan p) {
            return new PlanSnapshot(p, analyzed, optimized, physical);
        }

        PlanSnapshot withAnalyzed(LogicalPlan a) {
            return new PlanSnapshot(parsed, a, optimized, physical);
        }

        PlanSnapshot withOptimized(LogicalPlan o) {
            return new PlanSnapshot(parsed, analyzed, o, physical);
        }

        PlanSnapshot withPhysical(PhysicalPlan p) {
            return new PlanSnapshot(parsed, analyzed, optimized, p);
        }
    }

    private volatile PlanSnapshot planSnapshot = PlanSnapshot.EMPTY;

    public EsqlSession(
        String sessionId,
        TransportVersion localClusterMinimumVersion,
        AnalyzerSettings analyzerSettings,
        IndexResolver indexResolver,
        EnrichPolicyResolver enrichPolicyResolver,
        ViewResolver viewResolver,
        DatasetResolver datasetResolver,
        ExternalSourceResolver externalSourceResolver,
        EsqlParser parser,
        PreAnalyzer preAnalyzer,
        EsqlFunctionRegistry functionRegistry,
        PromqlFunctionRegistry promqlFunctionRegistry,
        AnalysisRegistry analysisRegistry,
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
        this.datasetResolver = datasetResolver;
        this.externalSourceResolver = externalSourceResolver;
        this.parser = parser;
        this.preAnalyzer = preAnalyzer;
        this.verifier = verifier;
        this.metrics = metrics;
        this.functionRegistry = functionRegistry;
        this.promqlFunctionRegistry = promqlFunctionRegistry;
        this.analysisRegistry = analysisRegistry;
        this.mapper = mapper;
        this.planTelemetry = planTelemetry;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
        this.indexNameExpressionResolver = services.indexNameExpressionResolver();
        this.inferenceService = services.inferenceService();
        this.preMapper = new PreMapper(services);
        this.remoteClusterService = services.transportService().getRemoteClusterService();
        this.blockFactory = services.blockFactoryProvider().blockFactory();
        this.plannerSettings = plannerSettings;
        this.crossProjectModeDecider = services.crossProjectModeDecider();
        this.clusterName = services.clusterService().getClusterName().value();
        this.clusterUuid = resolveClusterUuid(services.clusterService());
        this.projectMetadata = projectMetadata;
        this.ipLocationService = services.ipLocationService();
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
        // Wrap the outer listener so any failure — parse, view-resolution, analyze, optimize, map,
        // execute — funnels through one place that emits the anonymized log on INTERNAL_SERVER_ERROR.
        listener = wrapForAnonymizedFailureLog(listener);
        TimeSpanMarker parsingProfile = executionInfo.queryProfile().parsing();
        parsingProfile.start();
        EsqlStatement statement = parse(request);
        // Capture the true parsed plan — before view resolution, before any analyzer rule runs.
        // PROMQL syntax still visible, views still as UnresolvedRelation, surrogate rewrites not
        // applied. This is the form closest to user intent for failure-path triage.
        planSnapshot = planSnapshot.withParsed(statement.plan());
        parsingProfile.stop();

        // Resolve all query settings up front, immediately after parse, so every downstream phase only reads
        // resolved values (default < request body < in-query SET) and never re-derives precedence. This also runs
        // each setting's validator (e.g. the project_routing cross-project gate) before any view-resolution work.
        ResolvedSettings resolved = QuerySettings.resolve(
            request.requestSettings(),
            statement,
            SettingsValidationContext.from(remoteClusterService)
        );
        gatherSettingsMetrics(request, statement);
        if (QuerySettings.APPROXIMATION.get(resolved) != null) {
            EsqlLicenseChecker.checkQueryApproximation(verifier.licenseState());
        }

        TimeSpanMarker viewResolutionProfile = executionInfo.queryProfile().viewResolution();
        viewResolutionProfile.start();
        // View and IN subquery resolution. IN_SUBQUERY telemetry is gathered from the result (ViewResolutionResult.hasInSubquery)
        // once resolution succeeds, because IN subqueries can be hidden inside view definitions and only become visible — and are
        // rewritten away into SemiJoin/AntiJoin/MarkJoin — during resolution. The WHERE counter is set by the analyzer/verifier plan
        // walk via FeatureMetric.WHERE matching SemiJoin/AntiJoin/MarkJoin too.
        viewResolver.replaceViews(
            statement.plan(),
            QuerySettings.PROJECT_ROUTING.get(resolved),
            (query, viewName) -> parser.parseView(
                query,
                request.params(),
                SettingsValidationContext.from(remoteClusterService),
                inferenceService.inferenceSettings(),
                viewName
            ).plan(),
            listener.delegateFailureAndWrap((l, viewResolution) -> {
                // Validate: no InSubquery expressions should survive view and subquery resolution.
                InSubqueryResolver.verify(viewResolution.plan());
                viewResolutionProfile.stop();
                analyseAndExecute(request, executionInfo, planRunner, statement, resolved, viewResolution, l);
            })
        );
    }

    private void analyseAndExecute(
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PlanRunner planRunner,
        EsqlStatement statement,
        ResolvedSettings resolved,
        ViewResolver.ViewResolutionResult viewResolution,
        ActionListener<Versioned<Result>> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SEARCH);

        // this is stack telemetry
        gatherViewMetrics(viewResolution);
        gatherInSubqueryMetrics(viewResolution);

        // this is APM
        gatherPlanTelemetry(viewResolution.plan(), statement.settings());

        // Trigger IP location database downloads for any IP_LOCATION command
        requestIpLocationDownloads(viewResolution.plan());

        PlanTimeProfile planTimeProfile = request.profile() ? new PlanTimeProfile() : null;

        ZoneId timeZone = QuerySettings.TIME_ZONE.get(resolved);
        Configuration configuration = new Configuration(
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
            resolved,
            viewResolution.viewQueries()
        );

        // Pre-analysis pass over the uncompacted plan from ViewResolver: reshape user-written
        // Subquery/UnionAll structures into ViewUnionAll. ViewShadowRelation siblings and nested
        // ViewUnionAlls are intentionally preserved here — they are stripped/flattened in
        // ViewCompaction.postIndexResolution(), which runs as an analyzer rule after ResolveTable so
        // lenient field-caps can pair each shadow with its strict sibling.
        LogicalPlan plan = ViewCompaction.preIndexResolution(viewResolution.plan());
        // Run structural checks that don't need analysis or index resolution. Doing this here
        // (after view resolution, before pre-analysis) lets a malformed query fail-fast without
        // paying for field-caps round trips.
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
            QuerySettings.UNMAPPED_FIELDS.get(finalConfiguration.resolvedSettings()),
            finalConfiguration,
            executionInfo,
            request.filter(),
            new EsqlCCSUtils.CssPartialErrorsActionListener(finalConfiguration, executionInfo, listener) {
                @Override
                public void onResponse(Versioned<LogicalPlan> analyzedPlan) {
                    assert ThreadPool.assertCurrentThreadPool(
                        ThreadPool.Names.SEARCH,
                        ThreadPool.Names.SEARCH_COORDINATION,
                        ThreadPool.Names.SYSTEM_READ,
                        // External source resolution ({@link ExternalSourceResolver}) dispatches through esql_worker
                        // and this callback is reached on that thread.
                        ESQL_WORKER_THREAD_POOL_NAME
                    );

                    LogicalPlan plan = analyzedPlan.inner();
                    // Capture the analyzed plan for failure-path logging: schema-resolved,
                    // PROMQL→TS conversion done, but surrogate rewrites haven't fired yet.
                    planSnapshot = planSnapshot.withAnalyzed(plan);
                    // Flag external-source usage on the shared telemetry so the coordinator emits
                    // external-source-scoped operational metrics only for queries that scanned one.
                    if (plan.anyMatch(ExternalRelation.class::isInstance)) {
                        planTelemetry.externalSource(true);
                    }
                    TransportVersion minimumVersion = analyzedPlan.minimumVersion();

                    var logicalPlanPreOptimizer = new LogicalPlanPreOptimizer(
                        new LogicalPreOptimizerContext(foldContext, inferenceService, minimumVersion)
                    );
                    var logicalPlanOptimizer = new LogicalPlanOptimizer(
                        new LogicalOptimizerContext(finalConfiguration, foldContext, minimumVersion)
                    );

                    var columnMetadata = new Holder<Map<NameId, Map<String, Object>>>();
                    SubscribableListener.<LogicalPlan>newForked(l -> preOptimizedPlan(plan, logicalPlanPreOptimizer, planTimeProfile, l))
                        .<LogicalPlan>andThen(
                            (l, p) -> preMapper.preMapper(
                                new Versioned<>(optimizedPlan(p, logicalPlanOptimizer, planTimeProfile), minimumVersion),
                                l
                            )
                        )
                        .<Result>andThen((l, p) -> {
                            columnMetadata.set(createColumnMetadata(p, foldContext));
                            executeOptimizedPlan(
                                request,
                                executionInfo,
                                planRunner,
                                p,
                                finalConfiguration,
                                foldContext,
                                new Holder<ApproximationDriver>(),
                                minimumVersion,
                                planTimeProfile,
                                l
                            );
                        })
                        .<Versioned<Result>>andThen(
                            (l, r) -> l.onResponse(attachMetadataAndVersion(r, columnMetadata.get(), minimumVersion))
                        )
                        .addListener(listener);
                }
            }
        );
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
        Holder<ApproximationDriver> approximation,
        TransportVersion minimumVersion,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_COORDINATION,
            ThreadPool.Names.SYSTEM_READ,
            // Downstream of the analyzed-plan callback, which may complete on esql_worker after external source resolution.
            ESQL_WORKER_THREAD_POOL_NAME
        );
        var physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration, minimumVersion));

        EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);

        // In explain mode, wrap the listener to transform results into EXPLAIN table format.
        // We use the same execution path as normal queries to ensure accuracy.
        listener = explainMode
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
            listener
        );
    }

    /**
     * Resolves the cluster UUID once at session construction. Tolerates the test-fixture case where
     * a mocked {@code ClusterService} returns a null {@code ClusterState} — falls back to empty
     * string. The UUID feeds the HMAC key used by the anonymizer; an empty key still produces a
     * deterministic HMAC, only the token namespace is shared across affected sessions (test only).
     */
    private static String resolveClusterUuid(org.elasticsearch.cluster.service.ClusterService clusterService) {
        try {
            var state = clusterService.state();
            return state != null ? state.metadata().clusterUUID() : "";
        } catch (Exception e) {
            return "";
        }
    }

    private ActionListener<Versioned<Result>> wrapForAnonymizedFailureLog(ActionListener<Versioned<Result>> delegate) {
        return delegate.delegateResponse((next, err) -> {
            // Only log on internal server errors. User-facing failures (verification errors, parse
            // errors, type mismatches) return a 4xx with a useful message and don't need the plan.
            if (ExceptionsHelper.status(err) == RestStatus.INTERNAL_SERVER_ERROR) {
                logAnonymizedPlans(planSnapshot, err);
            }
            next.onFailure(err);
        });
    }

    private Map<NameId, Map<String, Object>> createColumnMetadata(LogicalPlan optimizedPlan, FoldContext foldContext) {
        // TODO we need to enforce NameId do not change during optimization.
        // Otherwise metadata might not be found when redering result.
        // Bucket metadata is gated on the COLUMN_METADATA_BUCKET capability — snapshot-only until the feature is finalized.
        Map<NameId, Map<String, Object>> bucketMetadata = EsqlCapabilities.Cap.COLUMN_METADATA_BUCKET.isEnabled()
            ? BucketColumnMetadata.createColumnMetadata(optimizedPlan, foldContext)
            : Map.of();
        return Maps.merge(
            bucketMetadata,
            ApproximationPlan.createColumnMetadata(optimizedPlan.output()),
            (a, b) -> Maps.merge(a, b, (m1, m2) -> {
                throw new IllegalStateException("Should not produce metadata with the same key");
            })
        );
    }

    private static Versioned<Result> attachMetadataAndVersion(
        Result result,
        Map<NameId, Map<String, Object>> columnMetadata,
        TransportVersion minimumVersion
    ) {
        return new Versioned<>(
            new Result(
                result.schema(),
                result.pages(),
                columnMetadata,
                result.configuration(),
                result.completionInfo(),
                result.executionInfo()
            ),
            minimumVersion
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
        Holder<ApproximationDriver> approximation,
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
            // execute main plan. Wrap the listener so the coordinator reconciles any data-node-captured
            // source stats into ExternalSourceCacheService before delivering Result.
            runner.run(physicalPlan, configuration, foldContext, planTimeProfile, listener.delegateFailureAndWrap((next, result) -> {
                reconcileCapturedSourceStats(result.completionInfo());
                next.onResponse(result);
            }));
        }
    }

    /**
     * Pushes data-node-captured per-file source stats (carried by {@link DriverCompletionInfo#capturedSourceMetadata})
     * into the coordinator's {@code ExternalSourceCacheService}, so the next query's planning-time
     * lookup finds the {@code _stats.*} keys embedded in the matching {@code SchemaCacheEntry}'s
     * {@code safeMetadata} — same shape Parquet's footer-derived stats already use.
     */
    private void reconcileCapturedSourceStats(DriverCompletionInfo info) {
        if (info == null) {
            return;
        }
        Map<String, List<Map<String, Object>>> captured = info.capturedSourceMetadata();
        if (captured == null || captured.isEmpty()) {
            return;
        }
        ExternalSourceCacheService cache = externalSourceResolver.cacheService();
        if (cache != null) {
            cache.reconcileSourceStatsFromContributions(captured);
        }
    }

    private void logAnonymizedPlans(PlanSnapshot snap, Exception err) {
        if (LOGGER.isErrorEnabled() == false) {
            return;
        }
        // If parse never completed, there's nothing useful to anonymize. The exception message
        // already carries the parse-error position.
        if (snap.parsed() == null) {
            return;
        }
        try {
            var anonymized = PlanAnonymizer.forSubmission(clusterUuid)
                .anonymize(snap.parsed(), snap.analyzed(), snap.optimized(), snap.physical());
            LOGGER.error(
                """
                    ES|QL query failed in session [{}]
                    failure:
                    {}
                    parsed:
                    {}
                    analyzed:
                    {}
                    optimized:
                    {}
                    physical:
                    {}
                    schema:
                    {}""".stripIndent(),
                sessionId,
                err.getMessage(),
                anonymized.parsed(),
                anonymized.analyzed(),
                anonymized.optimized(),
                anonymized.physical(),
                anonymized.schema(),
                err
            );
        } catch (Exception e) {
            LOGGER.warn("Plan anonymization failed for session [{}]", sessionId, e);
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
        Runnable cleanup,
        boolean isSubqueryJoinSubPlan
    ) {};

    private SubPlanAndCallback firstSubPlan(
        LogicalPlan mainPlan,
        Configuration configuration,
        Holder<ApproximationDriver> approximation,
        Set<LocalRelation> subPlansResults
    ) {
        SubPlanAndCallback subPlanAndCallback = null;

        // Find the first (bottom-up) SemiJoin or InlineJoin that needs subplan execution.
        // Processing bottom-up ensures inner subplans (e.g. INLINE STATS inside IN subquery)
        // are resolved before outer ones that depend on them.
        LogicalPlan firstJoin = findFirstSubPlanJoin(mainPlan, subPlansResults);

        if (firstJoin instanceof AbstractSubqueryJoin) {
            AbstractSubqueryJoin.LogicalPlanTuple semiJoinTuple = AbstractSubqueryJoin.firstSubPlan(mainPlan, subPlansResults);
            if (semiJoinTuple != null) {
                AtomicReference<Page> localRelationPage = new AtomicReference<>();
                subPlanAndCallback = new SubPlanAndCallback(semiJoinTuple.subPlan(), result -> {
                    LocalRelation resultWrapper = resultToPlan(semiJoinTuple.subPlan().source(), result);
                    // AbstractSubqueryJoin.inlineData may release this page eagerly (filter / empty paths) or swap
                    // it for a smaller, breaker-tracked dedup page (hash-join path) so the cleanup
                    // below releases the right one at end of main plan execution.
                    localRelationPage.set(resultWrapper.supplier().get());
                    subPlansResults.add(resultWrapper);
                    return AbstractSubqueryJoin.newMainPlan(
                        mainPlan,
                        semiJoinTuple,
                        resultWrapper,
                        resolveInSubqueryHashJoinThreshold(configuration),
                        blockFactory,
                        localRelationPage
                    );
                }, () -> releaseLocalRelationBlocks(localRelationPage), true);
            }
        } else if (firstJoin instanceof InlineJoin) {
            InlineJoin.LogicalPlanTuple subPlans = InlineJoin.firstSubPlan(mainPlan, subPlansResults);
            if (subPlans != null) {
                AtomicReference<Page> localRelationPage = new AtomicReference<>();
                subPlanAndCallback = new SubPlanAndCallback(subPlans.stubReplacedSubPlan(), result -> {
                    LocalRelation resultWrapper = resultToPlan(subPlans.stubReplacedSubPlan().source(), result);
                    localRelationPage.set(resultWrapper.supplier().get());
                    subPlansResults.add(resultWrapper);
                    return InlineJoin.newMainPlan(mainPlan, subPlans, resultWrapper);
                }, () -> releaseLocalRelationBlocks(localRelationPage), false);
            }
        }

        LogicalPlan plan = subPlanAndCallback != null ? subPlanAndCallback.subPlan() : mainPlan;
        if (ApproximationPlan.is(plan)) {
            if (approximation.get() == null) {
                approximation.set(ApproximationDriver.create(plan, QuerySettings.APPROXIMATION.get(configuration.resolvedSettings())));
            }
            LogicalPlan subPlan = approximation.get().firstSubPlan();
            if (subPlan != null) {
                subPlanAndCallback = new SubPlanAndCallback(
                    subPlan,
                    result -> approximation.get().newMainPlan(mainPlan, result),
                    () -> {},
                    false
                );
            }
        }

        return subPlanAndCallback;
    }

    /**
     * Finds the first (bottom-up) SemiJoin or InlineJoin in the plan that has an unresolved subplan.
     * Returns the join node itself, or null if none found.
     */
    private static LogicalPlan findFirstSubPlanJoin(LogicalPlan plan, Set<LocalRelation> subPlansResults) {
        Holder<LogicalPlan> result = new Holder<>();
        // Evaluate the right hand side of a SemiJoin or InlineJoin, unless it is a LocalRelation and registered in subPlansResults already
        plan.forEachUp(p -> {
            if (result.get() != null) {
                return;
            }
            // Whether checking the subquery join or InlineJoin first does not matter, the plan is processed bottom up, looking for
            // joins whose right child haven't been evaluated yet
            if (p instanceof AbstractSubqueryJoin sj) {
                if (sj.right() instanceof LocalRelation lr && subPlansResults.contains(lr)) {
                    return; // already processed
                }
                result.set(sj);
            } else if (p instanceof InlineJoin ij) {
                if (ij.right().anyMatch(r -> r instanceof StubRelation)) {
                    result.set(ij);
                } else if (ij.right() instanceof LocalRelation lr && (subPlansResults.isEmpty() || subPlansResults.contains(lr) == false)) {
                    result.set(ij);
                } else if (ij.right() instanceof LocalRelation == false && ij.right().anyMatch(r -> r instanceof LocalRelation)) {
                    result.set(ij);
                }
            }
        });
        return result.get();
    }

    private void executeSubPlan(
        DriverCompletionInfo.Accumulator completionInfoAccumulator,
        SubPlanAndCallback subPlan,
        Configuration configuration,
        FoldContext foldContext,
        Holder<ApproximationDriver> approximation,
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
        // An IN subquery may not have a pipeline breaker inside it, and mapper does not receive the SemiJoin node because only the right
        // hand side plans are sent to mapper. Ensure there is an ExchangeExec on top of it, so that the intermediate results can be sent
        // back to the coordinator
        if (subPlan.isSubqueryJoinSubPlan()) {
            physicalSubPlan = Mapper.ensureExchangeForSubPlan(physicalSubPlan);
        }

        executionInfo.startSubPlans(subPlan.isSubqueryJoinSubPlan());

        runner.run(physicalSubPlan, configuration, foldContext, planTimeProfile, listener.delegateFailureAndWrap((next, result) -> {
            completionInfoAccumulator.accumulate(result.completionInfo());
            try {
                var releasingNext = ActionListener.runAfter(next, subPlan.cleanup);
                LogicalPlan newMainPlan = subPlan.newMainPlan.apply(result);
                LOGGER.debug("New main plan after subplan execution:\n{}", newMainPlan);

                // look for the next inlinejoin plan
                var newSubPlan = firstSubPlan(newMainPlan, configuration, approximation, subPlansResults);
                LOGGER.debug("Next subplan: {}", newSubPlan != null ? newSubPlan.subPlan() : "null");

                if (newSubPlan == null) {
                    executionInfo.finishSubPlans();
                    var newPhysicalPlan = logicalPlanToPhysicalPlan(newMainPlan, request, physicalPlanOptimizer, planTimeProfile);
                    runner.run(
                        newPhysicalPlan,
                        configuration,
                        foldContext,
                        planTimeProfile,
                        releasingNext.delegateFailureAndWrap((finalListener, finalResult) -> {
                            completionInfoAccumulator.accumulate(finalResult.completionInfo());
                            DriverCompletionInfo merged = completionInfoAccumulator.finish();
                            reconcileCapturedSourceStats(merged);
                            EsqlCCSUtils.finalizeSubPlanOnlyRemoteClusters(executionInfo);
                            finalListener.onResponse(
                                new Result(finalResult.schema(), finalResult.pages(), null, configuration, merged, executionInfo)
                            );
                        })
                    );
                } else {
                    executeSubPlan(
                        completionInfoAccumulator,
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

    /**
     * Resolves the IN subquery hash join threshold: query pragma overrides the cluster setting.
     */
    private int resolveInSubqueryHashJoinThreshold(Configuration configuration) {
        int pragmaValue = QueryPragmas.IN_SUBQUERY_HASH_JOIN_THRESHOLD.get(configuration.pragmas().getSettings());
        return pragmaValue >= 0 ? pragmaValue : plannerSettings.inSubqueryHashJoinThreshold();
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

    /**
     * Requests the download of the IP location databases referenced by any {@code IP_LOCATION} command in the plan.
     * This is a fire-and-forget side-effect (the actual download is asynchronous) and is intentionally performed here, on the
     * coordinator before analysis, rather than inside the analyzer rule that resolves the command's output columns.
     */
    private void requestIpLocationDownloads(LogicalPlan plan) {
        if (ipLocationService == null || projectMetadata == null) {
            return;
        }
        if (plan.anyMatch(UnresolvedIpLocation.class::isInstance)) {
            ipLocationService.requestDownloads(projectMetadata.id().id(), IpLocationConsumer.ESQL);
        }
    }

    /**
     * Pre-fetches the IP database metadata for every {@code IP_LOCATION} command in the plan and bundles it into an
     * {@link IpLocationResolution} for the analyzer.
     * The lookup is a synchronous, side-effect-free metadata read keyed by database file name; an unrecognized database
     * yields no entry, which the analyzer rule reports as an unresolved command.
     */
    private IpLocationResolution resolveIpLocations(LogicalPlan plan) {
        if (ipLocationService == null) {
            return IpLocationResolution.SERVICE_UNAVAILABLE;
        }
        Map<String, IpDataLookupInfo> databaseInfo = new HashMap<>();
        plan.forEachDown(UnresolvedIpLocation.class, ip -> {
            IpDataLookupInfo info = ipLocationService.getIpDataLookupInfo(ip.databaseFile());
            if (info != null) {
                databaseInfo.put(ip.databaseFile(), info);
            }
        });
        return IpLocationResolution.fromPrefetched(databaseInfo);
    }

    private void gatherSettingsMetrics(EsqlQueryRequest request, EsqlStatement statement) {
        if (metrics == null) {
            return;
        }
        // The Metrics class only registers counters for settings applicable to the current environment
        // (e.g., snapshot-only settings are not registered in non-snapshot builds); incSetting() silently
        // ignores settings that don't have a registered counter.
        suppliedSettingNames(request, statement).forEach(metrics::incSetting);
    }

    /**
     * Names of every setting the user supplied, from either surface — the request body ({@code settings.{}} or a
     * legacy top-level field) and in-query {@code SET} — deduped by name so a setting given via both is counted once.
     */
    static Set<String> suppliedSettingNames(EsqlQueryRequest request, EsqlStatement statement) {
        Set<String> supplied = new HashSet<>();
        for (var def : request.requestSettings().keySet()) {
            supplied.add(def.name());
        }
        if (statement.settings() != null) {
            for (QuerySetting setting : statement.settings()) {
                supplied.add(setting.name());
            }
        }
        return supplied;
    }

    private void gatherViewMetrics(ViewResolver.ViewResolutionResult viewResolution) {
        if (metrics == null || viewResolution.viewQueries().isEmpty()) {
            return;
        }
        metrics.inc(FeatureMetric.VIEW);
    }

    /**
     * Increments the {@code IN_SUBQUERY} counter once per query when view + subquery resolution rewrote any
     * {@link org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery} into a
     * {@code SemiJoin}/{@code AntiJoin}/{@code MarkJoin}.
     * <p>
     * The decision comes from {@link ViewResolver.ViewResolutionResult#hasInSubquery()} rather than the pre-resolution plan,
     * because an IN subquery may live only inside a view definition: it is not visible in the original plan and is rewritten away
     * during resolution. Mirrors {@link #gatherViewMetrics}: direct increment, once per query, on successful resolution. The
     * {@code WHERE} counter is handled by the analyzer/verifier plan walk via {@code FeatureMetric#WHERE} matching
     * SemiJoin/AntiJoin/MarkJoin.
     */
    private void gatherInSubqueryMetrics(ViewResolver.ViewResolutionResult viewResolution) {
        if (metrics == null || viewResolution.hasInSubquery() == false) {
            return;
        }
        metrics.inc(FeatureMetric.IN_SUBQUERY);
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

        TimeSpanMarker datasetResolutionProfile = executionInfo.queryProfile().datasetResolution();
        datasetResolutionProfile.start();
        // Rewrite FROM <dataset> targets into UnresolvedExternalRelation so analysis treats them like the inline
        // EXTERNAL command. The resolver first read-authorizes the names through the security filter — they are
        // stripped from the plan here and would otherwise never reach authorization. Completes synchronously when
        // no FROM pattern can match a registered dataset.
        datasetResolver.replaceDatasets(parsed, projectMetadata, logicalPlanListener.delegateFailureAndWrap((delegate, rewritten) -> {
            datasetResolutionProfile.stop();
            analyzedPlanAfterDatasetResolution(rewritten, unmappedResolution, configuration, executionInfo, requestFilter, delegate);
        }));
    }

    private void analyzedPlanAfterDatasetResolution(
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
        // Initialize the PreAnalysisResult with the local cluster's minimum transport version, so our planning will be correct also
        // in case of ROW queries. ROW queries can still require inter-node communication (for ENRICH and LOOKUP JOIN execution) with
        // an older node in the same cluster; so assuming that all nodes are on the same version as this node will be wrong and may
        // cause bugs.
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
        boolean nullify = parsed.collectFirstChildren(p -> p instanceof PromqlCommand).isEmpty() == false;
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
            .<PreAnalysisResult>andThen(
                (l, r) -> preAnalyzeLookupIndices(preAnalysis.lookupIndices().iterator(), parsed, r, executionInfo, l)
            )
            .andThenApply(r -> {
                executionInfo.queryProfile().indicesResolutionMarker().stop();
                return r;
            })
            .<PreAnalysisResult>andThen((l, r) -> preAnalyzeExternalSources(externalSourceResolver, parsed, preAnalysis, r, l))
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
                inferenceService.resolveInferenceIds(preAnalysis.inferenceIds(), l.delegateFailureAndWrap((ll, inferenceResolution) -> {
                    executionInfo.queryProfile().inferenceResolutionMarker().stop();
                    ll.onResponse(r.withInferenceResolution(inferenceResolution));
                }));
            })
            .<Versioned<LogicalPlan>>andThen((l, r) -> {
                analyzeWithRetry(
                    parsed,
                    nullify ? UnmappedResolution.NULLIFY : unmappedResolution,
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
        LogicalPlan plan,
        PreAnalysisResult preAnalysisResult,
        EsqlExecutionInfo executionInfo,
        ActionListener<PreAnalysisResult> listener
    ) {
        forAll(
            lookupIndices,
            preAnalysisResult,
            (lookupIndex, r, l) -> preAnalyzeLookupIndex(lookupIndex, plan, r, executionInfo, l),
            listener
        );
    }

    private void preAnalyzeLookupIndex(
        IndexPattern lookupIndexPattern,
        LogicalPlan plan,
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
            ThreadPool.Names.SYSTEM_READ,
            // Typically entered from a field-caps completion on SEARCH_COORDINATION, but on analyzer retry
            // (analyzeWithRetry -> resolveIndicesAndAnalyze) with a synchronous main-index forAll (e.g. no FROM
            // indices) the resolver's continuation reaches this on esql_worker.
            ESQL_WORKER_THREAD_POOL_NAME
        );
        // No need to update the minimum transport version in the PreAnalysisResult,
        // it should already have been determined during the main index resolution.
        executionInfo.queryProfile().incFieldCapsCalls();
        var lookupIndexScope = EsqlCCSUtils.onlyRunning(
            executionInfo,
            computeLookupJoinIndexScope(plan, localPattern, result.indexResolution())
        );
        indexResolver.resolveLookupIndices(
            EsqlCCSUtils.createQualifiedLookupIndexExpressionFromAvailableClusters(lookupIndexScope, localPattern),
            result.wildcardJoinIndices().contains(localPattern) ? IndexResolver.ALL_FIELDS : result.fieldNames,
            // We use the minimum version determined in the main index resolution, because for remote LOOKUP JOIN, we're only considering
            // remote lookup indices in the field caps request - but the coordinating cluster must be considered, too!
            // The main index resolution should already have taken the version of the coordinating cluster into account and this should
            // be reflected in result.minimumTransportVersion().
            result.minimumTransportVersion(),
            listener.map(
                indexResolution -> receiveLookupIndexResolution(result, lookupIndexScope, localPattern, executionInfo, indexResolution)
            )
        );
    }

    /**
     * Derives the scope (set of clusters) where the lookup join index needs to be found.
     * For example for a query like `FROM (FROM cluster-1:index-1 | LOOKUP JOIN dictionary-1),(FROM cluster-2:index-2)`
     * `dictionary-1` must be found only on `cluster-1` as joining is not performed on `cluster-2`.
     * <p>
     * Only the data-bearing left subtree of each matching LOOKUP JOIN is considered, see {@link #collectLookupJoinLeftScope}.
     */
    static Set<String> computeLookupJoinIndexScope(
        LogicalPlan plan,
        String lookupPattern,
        Map<IndexPattern, IndexResolution> indexResolution
    ) {
        Set<String> scope = new LinkedHashSet<>();
        plan.forEachUp(LookupJoin.class, lj -> {
            if (lj.right() instanceof UnresolvedRelation ur && ur.indexPattern().indexPattern().equals(lookupPattern)) {
                collectLookupJoinLeftScope(lj.left(), scope, indexResolution);
            }
        });
        return scope;
    }

    /**
     * Collects the clusters that feed rows into a LOOKUP JOIN by walking only the data-bearing spine of its left subtree.
     * <p>
     * For any {@link AbstractSubqueryJoin} (SEMI/ANTI/MARK) that {@code InSubqueryResolver} produces for {@code field IN (subquery)}, only
     * the left child carries rows into the subsequent plan; the right child does not contribute source clusters to this join.
     * So {@code ... | WHERE x IN (FROM remote:idx) | LOOKUP JOIN ...} scopes the lookup to the outer source only, not to {@code remote}.
     * A {@code FROM a, b} union ({@code Fork}/{@code UnionAll}) instead reads from every branch, so all of its children are followed.
     * The behavior is validated by {@code CrossClusterInSubqueryIT.testMissingLookupIndexAfterWhereInSubquery}
     * <p>
     * Each index source contributes the clusters its pattern resolved to; a {@link Row} - a coordinator-only source carrying no index
     * relation - contributes the local cluster. The behavior is validated by
     * {@code CrossClusterInSubqueryIT.testMissingLookupIndexInsideWhereInSubquery} and
     * {@code CrossClusterSubqueryIT.testSubqueryWithRowAndLookupIndicesMissingOnClustersReferencedBySubquery}.
     */
    private static void collectLookupJoinLeftScope(
        LogicalPlan plan,
        Set<String> scope,
        Map<IndexPattern, IndexResolution> indexResolution
    ) {
        switch (plan) {
            case UnresolvedRelation source -> {
                IndexResolution resolution = indexResolution.get(source.indexPattern());
                if (resolution != null && resolution.isValid()) {
                    scope.addAll(resolution.get().originalIndices().keySet());
                }
            }
            case Row row -> scope.add(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            case AbstractSubqueryJoin subqueryJoin -> collectLookupJoinLeftScope(subqueryJoin.left(), scope, indexResolution);
            default -> {
                for (LogicalPlan child : plan.children()) {
                    collectLookupJoinLeftScope(child, scope, indexResolution);
                }
            }
        }
    }

    /**
     * Resolve external sources (Iceberg tables/Parquet files) if present in the query.
     * This runs in parallel with other resolution steps to avoid blocking.
     * Extracts partition filter hints from the WHERE clause for partition-aware glob rewriting.
     */
    // package-private static so EsqlSessionTests can drive the wiring with a capturing
    // ExternalSourceResolver and assert that the computed pathsRequiringStats set is forwarded.
    static void preAnalyzeExternalSources(
        ExternalSourceResolver externalSourceResolver,
        LogicalPlan plan,
        PreAnalyzer.PreAnalysis preAnalysis,
        PreAnalysisResult result,
        ActionListener<PreAnalysisResult> listener
    ) {
        if (preAnalysis.icebergPaths().isEmpty()) {
            listener.onResponse(result);
            return;
        }

        Map<String, Map<String, Object>> pathConfigs = extractExternalConfigs(plan);

        var filterHints = PartitionFilterHintExtractor.extract(plan);

        // Always non-null (empty when no ungrouped aggregate is present). A non-null set switches the
        // resolver to selective eager stats: only the listed paths read every file's footer at
        // planning time; the rest defer (see ExternalStatsRequirementExtractor).
        Set<String> pathsRequiringStats = ExternalStatsRequirementExtractor.pathsRequiringEagerStats(plan);

        externalSourceResolver.resolve(
            preAnalysis.icebergPaths(),
            pathConfigs,
            filterHints.isEmpty() ? null : filterHints,
            pathsRequiringStats,
            listener.map(result::withExternalSourceResolution)
        );
    }

    /**
     * Map from table path to config across all {@code UnresolvedExternalRelation} nodes. Every
     * {@code tablePath} is a non-null {@code Literal} post-parsing; non-Literal here is a
     * precondition violation and throws.
     */
    // package-private for testing
    static Map<String, Map<String, Object>> extractExternalConfigs(LogicalPlan plan) {
        Map<String, Map<String, Object>> pathConfigs = new HashMap<>();
        plan.forEachUp(org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation.class, p -> {
            org.elasticsearch.xpack.esql.core.expression.Expression tablePath = p.tablePath();
            if (tablePath instanceof org.elasticsearch.xpack.esql.core.expression.Literal literal && literal.value() != null) {
                String path = org.elasticsearch.common.lucene.BytesRefs.toString(literal.value());
                // If two UnresolvedExternalRelation nodes share the same literal path the later config
                // overwrites the earlier one. Today the dataset-rewrite pipeline produces at most one
                // UER per concrete path, so this is benign; if plan shapes evolve to allow per-call
                // config divergence at the same path this needs to become a merge or a verifier check.
                pathConfigs.put(path, p.config());
            } else {
                throw new IllegalStateException(
                    "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                        + (tablePath == null ? "null" : tablePath.sourceText())
                        + "]"
                );
            }
        });
        return pathConfigs;
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
        Set<String> lookupIndexScope,
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
        lookupIndexScope.forEach(clusterAlias -> {
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
            ThreadPool.Names.SYSTEM_READ,
            // On analyzer retry (analyzeWithRetry -> resolveIndicesAndAnalyze) this may be re-entered on esql_worker,
            // the thread the external source resolver continued on.
            ESQL_WORKER_THREAD_POOL_NAME
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
                    QuerySettings.PROJECT_ROUTING.get(configuration.resolvedSettings()),
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
                            QuerySettings.PROJECT_ROUTING.get(configuration.resolvedSettings()),
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
                    EsqlCCSUtils.checkForRemoteResourceErrors(indexResolution.inner().failures());
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
            result.fieldNames,
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
                EsqlCCSUtils.checkForRemoteResourceErrors(indexResolution.inner().failures());
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
                EsqlCCSUtils.checkForRemoteResourceErrors(indexResolution.inner().failures());
                EsqlCCSUtils.validateCcsLicense(verifier.licenseState(), executionInfo);
                planTelemetry.linkedProjectsCount(executionInfo.clusterInfo.size());
                maybeRetryConcreteTimeSeriesResolution(indexPattern, indexMode, result, indexResolution, l, retryListener -> {
                    executionInfo.queryProfile().incFieldCapsCalls();
                    indexResolver.resolveFlatIndicesVersioned(
                        false /* lenient */,
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
                    result.linkedResolution.values(),
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
        // Capture the optimized plan before mapping so a failure in physical planning still
        // surfaces it in the failure log.
        planSnapshot = planSnapshot.withOptimized(optimizedPlan);
        PhysicalPlan physicalPlan = optimizedPhysicalPlan(optimizedPlan, physicalPlanOptimizer, planTimeProfile);
        physicalPlan = PlannerUtils.integrateEsFilterIntoFragment(physicalPlan, request.filter());
        physicalPlan = EstimatesRowSize.estimateRowSize(0, physicalPlan);
        // Overwrite on each call so a failure during subplan execution surfaces the most recent
        // physical plan we built.
        planSnapshot = planSnapshot.withPhysical(physicalPlan);
        return physicalPlan;
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
            promqlFunctionRegistry,
            analysisRegistry,
            unmappedResolution,
            projectMetadata,
            r,
            timestampBounds,
            resolveIpLocations(parsed)
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
        // CPS specific linkedIndexPatterns. Such patterns references indices (if present) shadowing views resolved on origin
        Map<LinkedIndexPattern, IndexResolution> linkedResolution,
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

        PreAnalysisResult withWithLinkedIndices(LinkedIndexPattern indexPattern, IndexResolution indices) {
            linkedResolution.put(indexPattern, indices);
            return this;
        }

        PreAnalysisResult withEnrichResolution(EnrichResolution enrichResolution) {
            return new PreAnalysisResult(
                fieldNames,
                wildcardJoinIndices,
                indexResolution,
                lookupIndices,
                linkedResolution,
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
                linkedResolution,
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
                linkedResolution,
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
                linkedResolution,
                enrichResolution,
                inferenceResolution,
                externalSourceResolution,
                minimumTransportVersion
            );
        }
    }
}
