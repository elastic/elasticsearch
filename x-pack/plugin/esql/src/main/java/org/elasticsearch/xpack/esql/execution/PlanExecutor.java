/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.elasticsearch.xpack.esql.telemetry.QueryMetric;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {

    private final IndexResolver indexResolver;
    private final EsqlParser parser;
    private final PreAnalyzer preAnalyzer;
    private final EsqlFunctionRegistry functionRegistry;
    private final PromqlFunctionRegistry promqlFunctionRegistry;
    private final Mapper mapper;
    private final Metrics metrics;
    private final Verifier verifier;
    private final PlanTelemetryManager planTelemetryManager;
    private final EsqlQueryLog queryLog;
    private final DataSourceModule dataSourceModule;
    private final ExternalSourceCacheService cacheService;
    private final AnalysisRegistry analysisRegistry;

    public PlanExecutor(
        IndexResolver indexResolver,
        MeterRegistry meterRegistry,
        XPackLicenseState licenseState,
        EsqlQueryLog queryLog,
        List<BiConsumer<LogicalPlan, Failures>> extraCheckers,
        CrossProjectModeDecider crossProjectModeDecider,
        DataSourceModule dataSourceModule,
        EsqlFunctionRegistry functionRegistry,
        PromqlFunctionRegistry promqlFunctionRegistry,
        EsqlParser parser,
        ExternalSourceCacheService cacheService,
        AnalysisRegistry analysisRegistry
    ) {
        this.indexResolver = indexResolver;
        this.parser = parser;
        this.preAnalyzer = new PreAnalyzer();
        this.functionRegistry = functionRegistry;
        this.promqlFunctionRegistry = promqlFunctionRegistry;
        this.mapper = new Mapper();
        this.metrics = new Metrics(functionRegistry, crossProjectModeDecider.crossProjectEnabled());
        this.verifier = new Verifier(metrics, licenseState, extraCheckers);
        this.planTelemetryManager = new PlanTelemetryManager(meterRegistry);
        this.queryLog = queryLog;
        this.dataSourceModule = dataSourceModule;
        this.cacheService = cacheService;
        this.analysisRegistry = analysisRegistry;
    }

    /**
     * Builds the {@link ExternalSourceResolver} used for external (Iceberg/Parquet) discovery. The executor,
     * cancellation signal, and in-flight fan-out bound are supplied by the caller ({@link #esql}, ultimately
     * from {@code TransportEsqlQueryAction}) rather than hardcoded here, so the resolver keeps the
     * caller-owned executor/cancellation seam while the wiring — executor isolation (off {@code SEARCH}) and the
     * caller-supplied {@code metadataReadConcurrency} (production: the {@code snapshot_meta}-shaped discovery
     * default, capped at 100) — stays testable without standing up the full query path. See {@link #esql} for why
     * an in-flight bound that may exceed the pool size is safe.
     */
    static ExternalSourceResolver createExternalSourceResolver(
        Executor externalSourceExecutor,
        DataSourceModule dataSourceModule,
        Settings settings,
        ExternalSourceCacheService cacheService,
        BooleanSupplier cancellation,
        int externalSourceConcurrency
    ) {
        return new ExternalSourceResolver(
            externalSourceExecutor,
            dataSourceModule,
            settings,
            cacheService,
            cancellation,
            externalSourceConcurrency
        );
    }

    /**
     * @param externalSourceExecutor Executor for {@link ExternalSourceResolver} work — glob expansion, footer reads,
     *                               schema reconciliation. Must not be the SEARCH pool: a wildcard external query
     *                               would otherwise starve regular ES searches and other ES|QL queries. Production
     *                               wiring passes {@code esql_worker}.
     * @param externalSourceConcurrency maximum number of in-flight per-file metadata reads during a multi-file
     *                               resolve. Production wiring passes
     *                               {@link ExternalSourceSettings#blobStoreConcurrency(Settings)} (the
     *                               {@code snapshot_meta} shape, capped at 100): footer reads are async (the pool
     *                               thread is released across the network round-trip), so the bound caps concurrent
     *                               in-flight reads rather than pinning that many threads.
     * @param cancellation consulted before each per-file footer read so a wide-glob discovery aborts promptly when
     *                               the originating query is cancelled.
     */
    public void esql(
        EsqlQueryRequest request,
        String sessionId,
        TransportVersion localClusterMinimumVersion,
        AnalyzerSettings analyzerSettings,
        EnrichPolicyResolver enrichPolicyResolver,
        ViewResolver viewResolver,
        DatasetResolver datasetResolver,
        EsqlExecutionInfo executionInfo,
        IndicesExpressionGrouper indicesExpressionGrouper,
        EsqlSession.PlanRunner planRunner,
        TransportActionServices services,
        Executor externalSourceExecutor,
        int externalSourceConcurrency,
        BooleanSupplier cancellation,
        ActionListener<Versioned<Result>> listener
    ) {
        final PlanTelemetry planTelemetry = new PlanTelemetry(functionRegistry);
        // Resolution (glob expansion, footer reads, schema reconciliation) runs on the caller-supplied
        // executor rather than the SEARCH pool, so a wildcard external query cannot starve regular ES
        // searches or other ES|QL queries. The per-query multi-file metadata fan-out is bounded by
        // externalSourceConcurrency (production: the snapshot_meta-shaped discovery default, capped at 100):
        // because footer reads are async (the pool thread is released across the network round-trip) the bound
        // caps in-flight reads rather than pinning that many threads, so a wide discovery cannot starve execution.
        // NOTE: this release-across-the-read guarantee holds for storage backends with native async
        // reads (e.g. S3). Backends whose readBytesAsync is an executor-backed sync read (local, GCS)
        // still occupy a worker thread for the duration of each footer read; the bound limits how
        // many do so at once, and re-homing those blocking reads off esql_worker is handled by the
        // follow-up concurrency-fairness work rather than here.
        final ExternalSourceResolver externalSourceResolver = createExternalSourceResolver(
            externalSourceExecutor,
            dataSourceModule,
            services.clusterService().getSettings(),
            cacheService,
            cancellation,
            externalSourceConcurrency
        );
        final var session = new EsqlSession(
            sessionId,
            localClusterMinimumVersion,
            analyzerSettings,
            indexResolver,
            enrichPolicyResolver,
            viewResolver,
            datasetResolver,
            externalSourceResolver,
            parser,
            preAnalyzer,
            functionRegistry,
            promqlFunctionRegistry,
            analysisRegistry,
            mapper,
            verifier,
            metrics,
            planTelemetry,
            indicesExpressionGrouper,
            services.projectResolver().getProjectMetadata(services.clusterService().state()),
            services.plannerSettings().get(),
            services
        );
        QueryMetric clientId = QueryMetric.fromString("rest");
        metrics.total(clientId);

        var begin = System.nanoTime();
        ActionListener<Versioned<Result>> executeListener = wrap(
            x -> onQuerySuccess(request, listener, x, planTelemetry, begin),
            ex -> onQueryFailure(request, listener, ex, clientId, planTelemetry, begin)
        );
        // Wrap it in a listener so that if we have any exceptions during execution, the listener picks it up
        // and all the metrics are properly updated
        ActionListener.run(executeListener, l -> session.execute(request, executionInfo, planRunner, l));
    }

    private void onQuerySuccess(
        EsqlQueryRequest request,
        ActionListener<Versioned<Result>> listener,
        Versioned<Result> x,
        PlanTelemetry planTelemetry,
        long begin
    ) {
        planTelemetryManager.publish(planTelemetry, true);
        boolean partial = x != null && x.inner().completionInfo().partial();
        recordExternalSourceQuery(
            dataSourceModule.externalSourceMetrics(),
            planTelemetry.externalSource(),
            (System.nanoTime() - begin) / 1_000_000,
            partial,
            null
        );
        queryLog.onQueryPhase(x, request.queryDescription());
        listener.onResponse(x);
    }

    private void onQueryFailure(
        EsqlQueryRequest request,
        ActionListener<Versioned<Result>> listener,
        Exception ex,
        QueryMetric clientId,
        PlanTelemetry planTelemetry,
        long begin
    ) {
        // TODO when we decide if we will differentiate Kibana from REST, this String value will likely come from the request
        metrics.failed(clientId);
        planTelemetryManager.publish(planTelemetry, false);
        recordExternalSourceQuery(
            dataSourceModule.externalSourceMetrics(),
            planTelemetry.externalSource(),
            (System.nanoTime() - begin) / 1_000_000,
            false,
            ex
        );
        queryLog.onQueryFailure(request.queryDescription(), ex, System.nanoTime() - begin);
        listener.onFailure(ex);
    }

    /**
     * Publishes the per-query external-source coordinator metrics — but only when the query actually scanned an
     * external source ({@code externalSource}: an {@code ExternalRelation} was seen in the analyzed plan, flagged on
     * {@code PlanTelemetry}). The outcome is classified from {@code failure}: {@code null} → success; a
     * {@link TaskCancelledException} anywhere in the cause chain → cancelled; anything else → failure. A hard failure
     * that unwraps to a {@link CircuitBreakingException} additionally bumps {@code breaker.tripped}. Best-effort: the
     * {@code recordX} methods self-guard, so an instrumentation failure never affects the query outcome.
     * <p>
     * <b>Known gap (only hard-failure breaker trips are counted):</b> a query that returns {@code is_partial=true}
     * BECAUSE a breaker tripped mid-scan reaches the success path with {@code failure == null}, so it is NOT counted
     * in {@code breaker.tripped}. The tripping {@link CircuitBreakingException} is not cleanly reachable here: a pure
     * external-source partial is flagged via {@code EsqlExecutionInfo#markPartial()}, which sets {@code is_partial}
     * directly with NO {@code clusterInfo} / {@code ShardSearchFailure} carrying the cause — so scanning the
     * execution-info cluster failures would systematically miss exactly this case while adding a fragile traversal.
     * The partial itself is still counted in {@code queries.partial.total}; only the breaker attribution is dropped.
     * <p>
     * Package-private and static (no coordinator state, only the sink) so the classification can be unit-tested
     * directly against a real registry-backed {@link ExternalSourceMetrics}.
     */
    static void recordExternalSourceQuery(
        ExternalSourceMetrics externalSourceMetrics,
        boolean externalSource,
        long durationMillis,
        boolean partial,
        Throwable failure
    ) {
        if (externalSource == false) {
            return;
        }
        String outcome;
        if (failure == null) {
            outcome = ExternalSourceMetrics.OUTCOME_SUCCESS;
        } else if (ExceptionsHelper.unwrap(failure, TaskCancelledException.class) != null) {
            outcome = ExternalSourceMetrics.OUTCOME_CANCELLED;
        } else {
            outcome = ExternalSourceMetrics.OUTCOME_FAILURE;
        }
        externalSourceMetrics.recordQuery(outcome, durationMillis, partial);
        // Only hard-failure breaker trips are attributed here: a CB that instead produced is_partial=true reaches the
        // success path with failure==null and is NOT counted (its CircuitBreakingException is not cleanly reachable at
        // this seam — see the javadoc "Known gap"). The partial is still counted via queries.partial.total above.
        if (failure != null && ExceptionsHelper.unwrap(failure, CircuitBreakingException.class) != null) {
            externalSourceMetrics.recordBreakerTripped();
        }
    }

    public IndexResolver indexResolver() {
        return indexResolver;
    }

    public Metrics metrics() {
        return this.metrics;
    }

    public DataSourceModule dataSourceModule() {
        return dataSourceModule;
    }

    public ExternalSourceCacheService cacheService() {
        return cacheService;
    }
}
