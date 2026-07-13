/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
import org.elasticsearch.xpack.esql.action.suggestions.CursorLocation;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionBuilder;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionContext;
import org.elasticsearch.xpack.esql.action.suggestions.valuesampling.HotTierValueSampler;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transport handler for {@code POST /_esql/suggestions}.
 *
 * <p>Runs the full local-cluster analysis pipeline (parse, view resolution, dataset resolution, index
 * resolution, analyze, optimize) via {@link PlanExecutor#analyzeAndOptimize}
 * — the same machinery every real ES|QL query resolves through — then walks the analyzed+optimized plan to
 * detect the completion context and, for {@code FIELD_NAME}/{@code PIPE_POSITION}, emit field-name suggestions
 * from the real resolved schema.
 *
 * <p><b>No cross-cluster resolution:</b> before analysis, every {@code FROM} target is checked with
 * {@link RemoteClusterAware#isRemoteIndexName(String)}. If any target is cluster-qualified (e.g. {@code remote:logs}),
 * this action skips analysis for the whole request and falls back to the coordinator-only, parse-only behavior
 * (field names statically resolvable from the plan, e.g. an explicit {@code KEEP}/{@code EVAL} list) rather than
 * partially resolving local targets and silently dropping remote ones.
 *
 * <p><b>Hot-tier value sampling (Steps 18–20):</b> when {@code includeSampleValues} is set and the cursor
 * sits in a {@code STRING_LITERAL_EQUALITY} context on a plain {@code keyword} field, this action fans out a
 * direct, node-grouped {@code TermsEnum} read to the hot-tier shard copies of the resolved index/indices (see
 * {@link org.elasticsearch.xpack.esql.action.suggestions.valuesampling.HotTierValueSampler}) and populates that
 * field's {@code values}. Every other context/type combination, and every {@code range} statistic, remains
 * deferred (see the suggestions API spec) — this action returns the coordinator-only field skeleton for those.
 *
 * <p>{@code includeSampleValues} therefore has two modes today:
 * <ul>
 *     <li>{@code false} (default): coordinator-only, field-name/type completion, no data-node visit.</li>
 *     <li>{@code true}: additionally samples {@code values} for a {@code STRING_LITERAL_EQUALITY}/{@code keyword}
 *     target from hot-tier data nodes (see above); {@code range} sampling remains deferred. This action still
 *     returns the coordinator-only field skeleton for every other combination so callers get a stable shape
 *     rather than an error.</li>
 * </ul>
 */
public class TransportEsqlSuggestionsAction extends HandledTransportAction<EsqlSuggestionsRequest, EsqlSuggestionsResponse> {

    private final ThreadPool threadPool;
    private final PlanExecutor planExecutor;
    private final ClusterService clusterService;
    private final ViewResolver viewResolver;
    private final DatasetResolver datasetResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;
    private final RemoteClusterService remoteClusterService;
    private final TransportActionServices services;
    private final EsqlParser parser;
    private final HotTierValueSampler valueSampler;

    @Inject
    public TransportEsqlSuggestionsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SearchService searchService,
        ExchangeService exchangeService,
        ClusterService clusterService,
        ViewResolver viewResolver,
        ProjectResolver projectResolver,
        ThreadPool threadPool,
        BlockFactoryProvider blockFactoryProvider,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        UsageService usageService,
        UserAgentParserRegistry userAgentParserRegistry,
        IpLocationService ipLocationService,
        CrossProjectModeDecider crossProjectModeDecider,
        IndicesService indicesService,
        ScriptService scriptService,
        // Reuses the node-shared EnrichPolicyResolver rather than constructing a second one: its constructor
        // registers a transport handler, and a duplicate registration fails node startup.
        TransportEsqlQueryAction queryAction
    ) {
        super(
            EsqlSuggestionsAction.NAME,
            transportService,
            actionFilters,
            EsqlSuggestionsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
        this.planExecutor = planExecutor;
        this.clusterService = clusterService;
        this.viewResolver = viewResolver;
        this.remoteClusterService = transportService.getRemoteClusterService();
        this.datasetResolver = new DatasetResolver(client, threadPool.executor(ThreadPool.Names.SEARCH), crossProjectModeDecider);
        this.enrichPolicyResolver = queryAction.enrichPolicyResolver();
        this.services = new TransportActionServices(
            transportService,
            searchService,
            exchangeService,
            clusterService,
            projectResolver,
            indexNameExpressionResolver,
            usageService,
            new InferenceService(client, clusterService),
            userAgentParserRegistry,
            ipLocationService,
            blockFactoryProvider,
            new PlannerSettings.Holder(clusterService),
            crossProjectModeDecider
        );
        // A stateless parser is sufficient for the up-front remote-index check; it does not consult cluster state.
        this.parser = new EsqlParser(new EsqlConfig(planExecutor.functionRegistry()));
        this.valueSampler = new HotTierValueSampler(
            clusterService,
            transportService,
            indicesService,
            scriptService,
            clusterService.getSettings(),
            threadPool
        );
    }

    @Override
    protected void doExecute(Task task, EsqlSuggestionsRequest request, ActionListener<EsqlSuggestionsResponse> listener) {
        // Real analysis performs async index resolution (field-caps round trips), so this can no longer complete
        // synchronously: dispatch onto SEARCH, matching EsqlSession's internal thread-pool assertions.
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> doExecuteOnSearchPool(request, listener));
    }

    private void doExecuteOnSearchPool(EsqlSuggestionsRequest request, ActionListener<EsqlSuggestionsResponse> listener) {
        LogicalPlan parsed;
        try {
            parsed = parser.parseQuery(request.query());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        if (hasRemoteTarget(parsed)) {
            // Cross-cluster resolution is out of scope entirely (see class javadoc): fall back to the
            // coordinator-only, parse-only behavior rather than partially resolving local targets.
            ActionListener.completeWith(listener, () -> suggest(parser, request));
            return;
        }

        String sessionId = UUIDs.randomBase64UUID();
        EsqlQueryRequest analysisRequest = EsqlQueryRequest.syncEsqlQueryRequest(request.query());
        analysisRequest.allowPartialResults(false);
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(
            clusterAlias -> remoteClusterService.shouldSkipOnFailure(clusterAlias, false),
            EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
        );
        planExecutor.analyzeAndOptimize(
            analysisRequest,
            sessionId,
            clusterService.state().getMinTransportVersion(),
            new AnalyzerSettings(
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(clusterService.getSettings()),
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.get(clusterService.getSettings()),
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.get(clusterService.getSettings()),
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.get(clusterService.getSettings())
            ),
            enrichPolicyResolver,
            viewResolver,
            datasetResolver,
            executionInfo,
            remoteClusterService,
            services,
            threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            () -> false,
            listener.delegateFailureAndWrap(
                (l, optimizedPlan) -> completeWithValueSampling(request, optimizedPlan, suggestFromAnalyzedPlan(request, optimizedPlan), l)
            )
        );
    }

    /**
     * Extends a coordinator-only response with hot-tier sampled {@code values} for the narrow slice
     * described in the suggestions API spec, Steps 18–20: {@code includeSampleValues=true}, a
     * {@code STRING_LITERAL_EQUALITY} context, and a plain {@code keyword} mapping (not {@code text},
     * and not the {@code constant_keyword}/{@code wildcard} cousins {@link DataType#KEYWORD} also
     * collapses onto — see the concrete-mapping-type gate below). Every other combination is
     * unchanged from the coordinator-only baseline.
     */
    private void completeWithValueSampling(
        EsqlSuggestionsRequest request,
        LogicalPlan optimizedPlan,
        EsqlSuggestionsResponse base,
        ActionListener<EsqlSuggestionsResponse> listener
    ) {
        if (request.includeSampleValues() == false) {
            listener.onResponse(base);
            return;
        }
        CursorLocation locations = new CursorLocation(request.query());
        SuggestionContext context = SuggestionContext.detect(optimizedPlan, locations, request.cursor());
        if (context.kind() != SuggestionContext.Kind.STRING_LITERAL_EQUALITY || context.targetField() == null) {
            listener.onResponse(base);
            return;
        }
        String field = context.targetField();
        DataType fieldType = resolveFieldType(context.schemaSource(optimizedPlan), field);
        // DataType.KEYWORD collapses keyword/constant_keyword/wildcard; only the plain keyword mapping is
        // supported here (see the concrete-mapping-type gate below) — everything else is unchanged.
        if (fieldType != DataType.KEYWORD) {
            listener.onResponse(base);
            return;
        }

        Set<String> concreteIndices = new LinkedHashSet<>();
        optimizedPlan.forEachDown(EsRelation.class, relation -> concreteIndices.addAll(relation.concreteQualifiedIndices()));
        if (concreteIndices.isEmpty()) {
            listener.onResponse(base);
            return;
        }

        ProjectMetadata metadata = services.projectResolver().getProjectState(clusterService.state()).metadata();
        List<String> plainKeywordIndices = new ArrayList<>();
        for (String index : concreteIndices) {
            if (HotTierValueSampler.isPlainKeywordMapping(metadata, index, field)) {
                plainKeywordIndices.add(index);
            }
            // TODO: constant_keyword/wildcard (and anything else DataType.KEYWORD collapses onto) are not
            // yet supported here — skip rather than guess at a workaround (see the suggestions API spec,
            // Step 18).
        }
        if (plainKeywordIndices.isEmpty()) {
            listener.onResponse(base);
            return;
        }

        Map<String, Set<ShardId>> hotNodeBundles = valueSampler.hotTierNodeBundles(metadata, Set.copyOf(plainKeywordIndices));
        if (hotNodeBundles.isEmpty()) {
            // No hot-tier nodes at all for this index: skip the fan-out entirely (Step 18), but the
            // result only ever reflects hot-tier data by construction, so hot_only still applies (Step 19).
            listener.onResponse(withWarnings(base, List.of(EsqlSuggestionsResponse.Warning.HOT_ONLY)));
            return;
        }

        valueSampler.sampleValues(
            field,
            hotNodeBundles,
            request.size(),
            TimeValue.timeValueSeconds(5).millis(),
            listener.delegateFailureAndWrap((l, result) -> {
                Map<String, FieldSuggestion> fields = new LinkedHashMap<>(base.fields());
                // An empty sample (e.g. Step 20's DLS gate refusing every contributing shard) is reported
                // the same way as "statistics not populated": no `values` key at all, not an empty list.
                List<FieldSuggestion.ValueSuggestion> values = result.values().isEmpty() ? null : result.values();
                fields.put(field, new FieldSuggestion(SuggestionBuilder.wireType(fieldType), values, null));
                l.onResponse(new EsqlSuggestionsResponse(fields, warningsForSampleResult(result)));
            })
        );
    }

    private static EsqlSuggestionsResponse withWarnings(EsqlSuggestionsResponse base, List<EsqlSuggestionsResponse.Warning> warnings) {
        return new EsqlSuggestionsResponse(base.fields(), warnings);
    }

    /**
     * The warnings that attach to a hot-tier value-sampling result (Step 19): {@code hot_only} always,
     * since this path only ever reflects hot-tier data; {@code shards_skipped} when any contributing
     * node returned a partial result or an error; {@code dls_active} when the requesting user's DLS
     * role query is not effectively {@code match_all} for the resolved index (Step 20).
     */
    static List<EsqlSuggestionsResponse.Warning> warningsForSampleResult(HotTierValueSampler.SampleResult result) {
        List<EsqlSuggestionsResponse.Warning> warnings = new ArrayList<>();
        warnings.add(EsqlSuggestionsResponse.Warning.HOT_ONLY);
        if (result.shardsSkipped()) {
            warnings.add(EsqlSuggestionsResponse.Warning.SHARDS_SKIPPED);
        }
        if (result.dlsActive()) {
            warnings.add(EsqlSuggestionsResponse.Warning.DLS_ACTIVE);
        }
        return warnings;
    }

    /** The resolved {@link DataType} of {@code field} in {@code schemaSource}'s output, or {@code null} if unresolved/absent. */
    private static DataType resolveFieldType(LogicalPlan schemaSource, String field) {
        for (Attribute attribute : schemaSource.output()) {
            if (attribute.resolved() && attribute.name().equals(field)) {
                return attribute.dataType();
            }
        }
        return null;
    }

    /** {@code true} if any {@code FROM} target in the parsed plan is cluster-qualified (e.g. {@code remote:logs}). */
    static boolean hasRemoteTarget(LogicalPlan plan) {
        AtomicBoolean remote = new AtomicBoolean(false);
        plan.forEachDown(UnresolvedRelation.class, relation -> {
            for (String index : relation.indexPattern().indexPattern().split(",")) {
                String trimmed = index.trim();
                if (trimmed.isEmpty() == false && RemoteClusterAware.isRemoteIndexName(trimmed)) {
                    remote.set(true);
                }
            }
        });
        return remote.get();
    }

    /**
     * Coordinator-side completion built from a fully analyzed+optimized plan. Extracted as a static method so it
     * can be unit-tested without transport plumbing.
     */
    static EsqlSuggestionsResponse suggestFromAnalyzedPlan(EsqlSuggestionsRequest request, LogicalPlan optimizedPlan) {
        CursorLocation locations = new CursorLocation(request.query());
        SuggestionContext context = SuggestionContext.detect(optimizedPlan, locations, request.cursor());
        Map<String, FieldSuggestion> fields = switch (context.kind()) {
            // The values/range come from a data-node visit that is deferred; emit an empty skeleton.
            case STRING_LITERAL_EQUALITY, NUMERIC_LITERAL_RANGE -> Map.of();
            case FIELD_NAME, PIPE_POSITION -> SuggestionBuilder.fieldsFromSchema(context.schemaSource(optimizedPlan));
        };
        return new EsqlSuggestionsResponse(fields, List.of());
    }

    /**
     * Pure coordinator-side completion against a parsed-but-unanalyzed plan: the fallback used when the query
     * targets a remote-qualified index (see class javadoc). Extracted as a static method so it can be
     * unit-tested with a plain {@link EsqlParser} and no transport plumbing.
     */
    static EsqlSuggestionsResponse suggest(EsqlParser parser, EsqlSuggestionsRequest request) {
        LogicalPlan parsed = parser.parseQuery(request.query());
        CursorLocation locations = new CursorLocation(request.query());
        SuggestionContext context = SuggestionContext.detect(parsed, locations, request.cursor());

        Map<String, FieldSuggestion> fields = switch (context.kind()) {
            case STRING_LITERAL_EQUALITY, NUMERIC_LITERAL_RANGE -> Map.of();
            case FIELD_NAME, PIPE_POSITION -> SuggestionBuilder.fieldsFromSchema(context.schemaSource(parsed));
        };

        return new EsqlSuggestionsResponse(fields, List.of());
    }
}
