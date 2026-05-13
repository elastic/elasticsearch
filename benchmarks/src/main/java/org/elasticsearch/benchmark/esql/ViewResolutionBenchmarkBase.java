/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.view.ViewResolutionService;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;

/**
 * Shared base for ES|QL view resolution benchmarks.
 * <p>
 * Provides two benchmark methods:
 * <ul>
 *   <li>{@link #resolveViews} &ndash; measures only {@link ViewResolver#replaceViews}</li>
 *   <li>{@link #queryPlanning} &ndash; measures the full pipeline: parse &rarr; resolve views
 *       &rarr; analyze &rarr; optimize</li>
 * </ul>
 * Scenarios cover the impact on every query, even those not targeting views:
 * <ul>
 *   <li>{@code disabled} &ndash; views feature flag off (baseline)</li>
 *   <li>{@code enabled_no_views} &ndash; feature on but no views defined</li>
 *   <li>{@code enabled_index} &ndash; feature on, views exist, query targets a plain index</li>
 *   <li>{@code depth_N} / {@code depth_N_filter} &ndash; query targets a view chain of depth 1&ndash;5,
 *       optionally with a {@code WHERE} filter at each level</li>
 * </ul>
 * Each scenario is also crossed with a {@code queryFilter} flag that appends
 * {@code | WHERE x > 5} to the outer query, testing resolution when the parsed
 * plan tree contains additional nodes beyond the {@code FROM}.
 * <p>
 * Subclasses declare {@code @Param}-annotated fields and wire them via
 * {@link #getScenario()} and {@link #getQueryFilter()}, allowing different
 * parameter sets for nightly regression runs vs full local exploration.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public abstract class ViewResolutionBenchmarkBase {

    static {
        Utils.configureBenchmarkLogging();
    }

    protected abstract String getScenario();

    protected abstract String getQueryFilter();

    private ViewResolver viewResolver;
    private LogicalPlan preParsedPlan;
    private String queryString;
    private BiFunction<String, String, LogicalPlan> viewParser;
    private Analyzer analyzer;
    private LogicalPlanOptimizer optimizer;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private EsqlParser parser;

    @Setup
    public void setup() {
        String scenario = getScenario();
        String queryFilter = getQueryFilter();

        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        InferenceSettings inferenceSettings = new InferenceSettings(Settings.EMPTY);
        SettingsValidationContext validationCtx = new SettingsValidationContext(false, false);
        TransportVersion minimumVersion = TransportVersion.current();

        parser = new EsqlParser(new EsqlConfig(functionRegistry));
        viewParser = (query, viewName) -> parser.parseView(query, new QueryParams(), validationCtx, inferenceSettings, viewName).plan();

        LinkedHashMap<String, EsField> mapping = new LinkedHashMap<>();
        for (int i = 0; i < 5; i++) {
            String name = "field" + i;
            mapping.put(name, new EsField(name, LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }
        mapping.put("x", new EsField("x", LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        for (int i = 0; i < 10; i++) {
            String name = "col" + i;
            mapping.put(name, new EsField(name, KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }
        EsIndex esIndex = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Map.of(), Map.of());

        Configuration config = new Configuration(
            DateUtils.UTC,
            Instant.now(),
            Locale.US,
            null,
            null,
            QueryPragmas.EMPTY,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            "",
            false,
            Map.of(),
            System.nanoTime(),
            false,
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY),
            null,
            null,
            Map.of()
        );

        analyzer = new Analyzer(
            new AnalyzerContext(
                config,
                functionRegistry,
                Map.of(new IndexPattern(Source.EMPTY, esIndex.name()), IndexResolution.valid(esIndex)),
                Map.of(),
                new EnrichResolution(),
                InferenceResolution.EMPTY,
                minimumVersion,
                UNMAPPED_FIELDS.defaultValue()
            ),
            new Verifier(new Metrics(functionRegistry, true, true), new XPackLicenseState(() -> 0L))
        );

        optimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), minimumVersion));

        boolean viewsEnabled;
        ViewMetadata viewMetadata;
        String outerFilterSuffix = "true".equals(queryFilter) ? " | WHERE x > 5" : "";

        switch (scenario) {
            case "disabled" -> {
                viewsEnabled = false;
                viewMetadata = ViewMetadata.EMPTY;
                queryString = "FROM test" + outerFilterSuffix;
            }
            case "enabled_no_views" -> {
                viewsEnabled = true;
                viewMetadata = ViewMetadata.EMPTY;
                queryString = "FROM test" + outerFilterSuffix;
            }
            case "enabled_index" -> {
                viewsEnabled = true;
                viewMetadata = new ViewMetadata(Map.of("some_view", new View("some_view", "FROM other")));
                queryString = "FROM test" + outerFilterSuffix;
            }
            default -> {
                viewsEnabled = true;
                boolean hasFilter = scenario.endsWith("_filter");
                int depth = Integer.parseInt(scenario.replace("depth_", "").replace("_filter", ""));
                viewMetadata = buildViewChain(depth, hasFilter);
                queryString = "FROM v0" + outerFilterSuffix;
            }
        }

        preParsedPlan = parsePlan(queryString);
        viewResolver = createResolver(viewsEnabled, viewMetadata);
    }

    @TearDown
    public void tearDown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        if (clusterService != null) {
            clusterService.close();
        }
    }

    /**
     * Builds a chain of views: v0 &rarr; v1 &rarr; &hellip; &rarr; v(depth-1) &rarr; test.
     * When {@code withFilter} is true each view appends {@code | WHERE fieldN > 10}.
     */
    static ViewMetadata buildViewChain(int depth, boolean withFilter) {
        Map<String, View> views = new HashMap<>();
        for (int i = 0; i < depth; i++) {
            String target = (i == depth - 1) ? "test" : "v" + (i + 1);
            String query = withFilter ? "FROM " + target + " | WHERE field" + i + " > 10" : "FROM " + target;
            views.put("v" + i, new View("v" + i, query));
        }
        return new ViewMetadata(views);
    }

    private LogicalPlan parsePlan(String query) {
        return parser.parseQuery(query, new QueryParams(), new InferenceSettings(Settings.EMPTY));
    }

    private ViewResolver createResolver(boolean viewsEnabled, ViewMetadata viewMetadata) {
        Settings settings = Settings.builder().put("node.name", "benchmark").put(ViewResolver.MAX_VIEW_DEPTH_SETTING.getKey(), 10).build();

        Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(ViewResolver.MAX_VIEW_DEPTH_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(settings, allSettings);

        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        clusterService = new ClusterService(settings, clusterSettings, threadPool, null);

        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        projectBuilder.putCustom(ViewMetadata.TYPE, viewMetadata);
        projectBuilder.put(
            IndexMetadata.builder("test")
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
        );

        ClusterState state = ClusterState.builder(new ClusterName("benchmark")).putProjectMetadata(projectBuilder).build();
        clusterService.getClusterApplierService().setInitialState(state);

        ProjectResolver projectResolver = DefaultProjectResolver.INSTANCE;
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            new SystemIndices(List.of()),
            projectResolver
        );
        ViewResolutionService viewResolutionService = new ViewResolutionService(indexNameExpressionResolver);

        return new BenchmarkViewResolver(clusterService, projectResolver, viewsEnabled, viewResolutionService);
    }

    /** Measures only the view resolution step (pre-parsed plan). */
    @Benchmark
    public void resolveViews(Blackhole blackhole) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        viewResolver.replaceViews(preParsedPlan, viewParser, future);
        blackhole.consume(future.actionGet());
    }

    /** Measures the full pipeline: parse &rarr; resolve views &rarr; analyze &rarr; optimize. */
    @Benchmark
    public void queryPlanning(Blackhole blackhole) {
        LogicalPlan parsed = parsePlan(queryString);

        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        viewResolver.replaceViews(parsed, viewParser, future);
        LogicalPlan resolved = future.actionGet().plan();

        LogicalPlan analyzed = analyzer.analyze(resolved);
        LogicalPlan optimized = optimizer.optimize(analyzed);
        blackhole.consume(optimized);
    }

    /**
     * {@link ViewResolver} subclass that uses the real {@link ViewResolutionService} algorithm
     * to resolve views from the cluster state's {@code indicesLookup}, bypassing only the
     * transport layer.
     */
    static class BenchmarkViewResolver extends ViewResolver {
        private final boolean enabled;
        private final ViewResolutionService viewResolutionService;
        private final ClusterService benchmarkClusterService;
        private final ProjectResolver benchmarkProjectResolver;

        BenchmarkViewResolver(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            boolean enabled,
            ViewResolutionService viewResolutionService
        ) {
            super(clusterService, projectResolver, null, CrossProjectModeDecider.NOOP);
            this.enabled = enabled;
            this.viewResolutionService = viewResolutionService;
            this.benchmarkClusterService = clusterService;
            this.benchmarkProjectResolver = projectResolver;
        }

        @Override
        protected boolean viewsFeatureEnabled() {
            return enabled;
        }

        @Override
        protected void doEsqlResolveViewsRequest(
            EsqlResolveViewAction.Request request,
            ActionListener<EsqlResolveViewAction.Response> listener
        ) {
            try {
                ProjectState projectState = benchmarkClusterService.state().projectState(benchmarkProjectResolver.getProjectId());
                ViewResolutionService.ViewResolutionResult result = viewResolutionService.resolveViews(
                    projectState,
                    request.indices(),
                    request.indicesOptions(),
                    request.getResolvedIndexExpressions()
                );
                listener.onResponse(new EsqlResolveViewAction.Response(result.views(), result.resolvedIndexExpressions()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
