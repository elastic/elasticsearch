/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.TimeSpanMarker;
import org.elasticsearch.xpack.esql.action.TransportResolveSchemaAction;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.EsqlSession.PreAnalysisResult;
import org.elasticsearch.xpack.esql.session.FieldNameUtils;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.NoClustersToSearchException;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * The single seam the session asks for schemas, instead of reaching for a different resolver per kind of index
 * abstraction. It composes one provider per kind — indices (the field-caps fetch), views (plan expansion),
 * datasets and external sources (the dataset rewrite plus external-source resolve) — and forwards each call to
 * the resolver that owns that kind's fetch, behaviour-identical to the direct calls it replaces. The provider
 * boundary is where the shared enumerate and authorize stages will later be lifted so every kind goes through one
 * front; for now the providers are thin and each kind keeps its native output shape.
 */
public final class SchemaService {

    private static final Logger LOGGER = LogManager.getLogger(SchemaService.class);

    private final IndexSchemaProvider indexProvider;
    private final ViewSchemaProvider viewProvider;
    private final DatasetSchemaProvider datasetProvider;
    private final List<AbstractionSchemaProvider> providers;
    private final RemoteClusterService remoteClusterService;
    private final Executor executor;
    private final PreAnalyzer preAnalyzer;
    private final TransportVersion localClusterMinimumVersion;

    public SchemaService(
        IndexResolver indexResolver,
        ViewResolver viewResolver,
        ExternalSourceResolver externalSourceResolver,
        RemoteClusterService remoteClusterService,
        CrossProjectModeDecider crossProjectModeDecider,
        IndicesExpressionGrouper indicesExpressionGrouper,
        PlanTelemetry planTelemetry,
        Verifier verifier,
        Client client,
        Executor executor,
        PreAnalyzer preAnalyzer,
        TransportVersion localClusterMinimumVersion
    ) {
        this.remoteClusterService = remoteClusterService;
        this.executor = executor;
        this.preAnalyzer = preAnalyzer;
        this.localClusterMinimumVersion = localClusterMinimumVersion;
        this.indexProvider = new IndexSchemaProvider(
            indexResolver,
            remoteClusterService,
            crossProjectModeDecider,
            indicesExpressionGrouper,
            planTelemetry,
            verifier
        );
        this.viewProvider = new ViewSchemaProvider(viewResolver);
        this.datasetProvider = new DatasetSchemaProvider(externalSourceResolver, client, executor, crossProjectModeDecider);
        this.providers = List.of(indexProvider, viewProvider, datasetProvider);
    }

    /**
     * Test-only constructor: inject the dispatch providers directly so the routing and grouping logic can be exercised
     * with lightweight fakes, without standing up every real resolver. The kind-specific accessors are unset.
     */
    SchemaService(List<AbstractionSchemaProvider> providers) {
        this.indexProvider = null;
        this.viewProvider = null;
        this.datasetProvider = null;
        this.providers = providers;
        this.remoteClusterService = null;
        this.executor = null;
        this.preAnalyzer = null;
        this.localClusterMinimumVersion = null;
    }

    /**
     * The single kind-blind entry the session calls to turn a parsed plan into a fully schema-resolved, analyzed plan.
     * The session hands over the parsed plan and the per-query inputs and never names an index-abstraction kind: the
     * per-kind sequencing (views, datasets, main indices, lookup indices, external sources) lives here.
     *
     * <p>The sequence is split into the same two phases as the legacy session flow, so it is behaviour-identical:
     * <ul>
     *   <li><b>one-shot prologue</b> — view expansion (+ in-subquery verification), the dataset rewrite, pre-analysis,
     *       and the field-name seed. These run exactly once; the dataset authorization round-trips and the seed are
     *       never re-issued on a retry.</li>
     *   <li><b>retried index body</b> — main-index resolution, the CCS prune/no-clusters checks, lookup-index
     *       resolution, external-source resolution, enrich, inference and analyze. A filter-driven
     *       {@link VerificationException} re-enters {@link #resolveIndicesAndAnalyze} without the request filter,
     *       re-running only this body.</li>
     * </ul>
     *
     * <p>The genuinely session-owned, non-schema interludes are supplied by {@code delegate}: building the
     * {@link Configuration} (with its telemetry / EXPLAIN-unwrap / IP-download side effects) right after view
     * resolution, enrich- and inference-id resolution (so their resolvers stay in the session), and the analyzer leaf.
     */
    public void resolvePlan(
        LogicalPlan parsedPlan,
        ProjectMetadata projectMetadata,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> viewParser,
        UnmappedResolution unmappedResolution,
        EsqlExecutionInfo executionInfo,
        QueryBuilder requestFilter,
        PlanResolutionDelegate delegate,
        ActionListener<Versioned<LogicalPlan>> listener
    ) {
        var viewResolutionProfile = executionInfo.queryProfile().viewResolution();
        viewResolutionProfile.start();
        replaceViews(parsedPlan, projectRouting, viewParser, listener.delegateFailureAndWrap((l, viewResolution) -> {
            // Validate: no InSubquery expressions should survive view and subquery resolution.
            InSubqueryResolver.verify(viewResolution.plan());
            viewResolutionProfile.stop();
            // The session builds the Configuration (and runs its telemetry / EXPLAIN-unwrap / IP-download side effects)
            // from the view-resolved result, handing back the plan to pre-analyze and the Configuration to drive the rest.
            PlanResolutionDelegate.Prepared prepared = delegate.afterViewResolution(viewResolution);
            resolveDatasetsAndAnalyze(
                prepared.plan(),
                unmappedResolution,
                prepared.configuration(),
                executionInfo,
                requestFilter,
                delegate,
                projectMetadata,
                l
            );
        }));
    }

    private void resolveDatasetsAndAnalyze(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        QueryBuilder requestFilter,
        PlanResolutionDelegate delegate,
        ProjectMetadata projectMetadata,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        var datasetResolutionProfile = executionInfo.queryProfile().datasetResolution();
        datasetResolutionProfile.start();
        // Authorize and rewrite FROM targets that resolve to datasets into UnresolvedExternalRelation so the rest of
        // pre-analysis + analysis treats them identically to the inline EXTERNAL command. Only datasets the caller can
        // read are rewritten (the resolve is a security-filtered action); the rest of analysis runs in the callback once
        // it completes. The resolve bails synchronously when no FROM pattern could match a registered dataset.
        resolveDatasets(parsed, projectMetadata, logicalPlanListener.delegateFailureAndWrap((l, rewrittenPlan) -> {
            datasetResolutionProfile.stop();
            analyzeRewrittenPlan(
                rewrittenPlan,
                unmappedResolution,
                configuration,
                executionInfo,
                requestFilter,
                delegate,
                projectMetadata,
                l
            );
        }));
    }

    private void analyzeRewrittenPlan(
        LogicalPlan parsed,
        UnmappedResolution unmappedResolution,
        Configuration configuration,
        EsqlExecutionInfo executionInfo,
        QueryBuilder requestFilter,
        PlanResolutionDelegate delegate,
        ProjectMetadata projectMetadata,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        var preAnalysisProfile = executionInfo.queryProfile().preAnalysis();
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
            delegate,
            projectMetadata,
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
        PlanResolutionDelegate delegate,
        ProjectMetadata projectMetadata,
        ActionListener<Versioned<LogicalPlan>> logicalPlanListener
    ) {
        executionInfo.queryProfile().indicesResolutionMarker().start();
        // TODO this is a quick hack to alleviate the pressure off of https://github.com/elastic/elasticsearch/issues/145920. A btter
        // solution would be to just not track the unmapped indices at all, but that requires a more structural change.
        boolean trackedUnmappedFieldIndices = unmappedResolution == UnmappedResolution.LOAD || parsed.anyMatch(p -> p instanceof Insist);
        boolean nullify = parsed.collectFirstChildren(p -> p instanceof PromqlCommand).isEmpty() == false;
        SchemaContext schemaCtx = new SchemaContext(
            executionInfo,
            configuration,
            preAnalysis,
            requestFilter,
            trackedUnmappedFieldIndices,
            result.fieldNames(),
            result.minimumTransportVersion()
        );
        SubscribableListener.<PreAnalysisResult>newForked(l -> resolveMainIndices(schemaCtx, projectMetadata, result, l))
            .andThenApply(r -> {
                if (r.indexResolution().isEmpty() == false // Rule out ROW case with no FROM clauses
                    && executionInfo.isCrossClusterSearch()
                    && executionInfo.getRunningClusterAliases().findAny().isEmpty()) {
                    LOGGER.debug("No more clusters to search, ending analysis stage");
                    throw new NoClustersToSearchException();
                }
                // Check if a subquery need to be pruned. If some but not all the subqueries has invalid index resolution,
                // try to prune it by setting IndexResolution to EMPTY_SUBQUERY. Analyzer.PruneEmptyUnionAllBranch will
                // take care of removing the subquery during analysis.
                // If all subqueries have invalid index resolution, we should fail in Analyzer's verifier.
                if (r.indexResolution().isEmpty() == false // it is not a row
                    && r.indexResolution().size() > 1 // there is a subquery
                    && executionInfo.isCrossClusterSearch()) {
                    Collection<IndexResolution> indexResolutions = r.indexResolution().values();
                    boolean hasInvalid = indexResolutions.stream().anyMatch(ir -> ir.isValid() == false);
                    boolean hasValid = indexResolutions.stream().anyMatch(IndexResolution::isValid);
                    // Only if there is partial invalid index resolutions in subqueries
                    if (hasInvalid && hasValid) {
                        // iterate the index resolution and replace it with EMPTY_SUBQUERY if the index resolution is invalid
                        r.indexResolution().forEach((indexPattern, indexResolution) -> {
                            if (indexResolution.isValid() == false) {
                                LOGGER.debug("Index pattern [{}] does not match valid indices, pruning the subquery", indexPattern);
                                r.withIndices(indexPattern, IndexResolution.EMPTY_SUBQUERY);
                            }
                        });
                        // check if there is a cluster that does not have any valid index resolution, if so mark it as skipped
                        executionInfo.getRunningClusterAliases().forEach(clusterAlias -> {
                            boolean clusterHasValidIndex = r.indexResolution()
                                .values()
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
            .<PreAnalysisResult>andThen((l, r) -> resolveLookupIndices(schemaCtx, projectMetadata, r, l))
            .andThenApply(r -> {
                executionInfo.queryProfile().indicesResolutionMarker().stop();
                return r;
            })
            .<PreAnalysisResult>andThen((l, r) -> {
                if (preAnalysis.icebergPaths().isEmpty()) {
                    l.onResponse(r);
                } else {
                    resolveExternalSources(parsed, preAnalysis.icebergPaths(), l.map(r::withExternalSourceResolution));
                }
            })
            .<PreAnalysisResult>andThen((l, r) -> {
                // Do not update PreAnalysisResult.minimumTransportVersion, that's already been determined during main index resolution.
                delegate.resolveEnrich(preAnalysis, r.minimumTransportVersion(), l.map(r::withEnrichResolution));
            })
            .<PreAnalysisResult>andThen((l, r) -> delegate.resolveInference(preAnalysis, l.map(r::withInferenceResolution)))
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
                    delegate,
                    projectMetadata,
                    l
                );
            })
            .addListener(logicalPlanListener);
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
        PlanResolutionDelegate delegate,
        ProjectMetadata projectMetadata,
        ActionListener<Versioned<LogicalPlan>> listener
    ) {
        LOGGER.debug("Analyzing the plan ({})", description);
        try {
            if (result.indexResolution().values().stream().anyMatch(IndexResolution::isValid) || requestFilter != null) {
                // We won't run this check with no filter and no valid indices since this may lead to false positive - missing index report
                // when the resolution result is not valid for a different reason.
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(
                    executionInfo,
                    result.indexResolution().values(),
                    result.linkedResolution().values(),
                    requestFilter != null
                );
            }
            TimeSpanMarker analysisProfile = executionInfo.queryProfile().analysis();
            analysisProfile.start();
            LogicalPlan plan = delegate.analyze(parsed, unmappedResolution, configuration, result, timestampBounds);
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
                    delegate,
                    projectMetadata,
                    listener
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * The session-owned, non-schema interludes the kind-blind {@link #resolvePlan} sequence calls back into: building
     * the {@link Configuration} from the view-resolved result (with telemetry / EXPLAIN-unwrap / IP-download side
     * effects), enrich- and inference-id resolution (their resolvers stay in the session), and the analyzer leaf
     * (re-run inside the filter retry). These are the steps that are not index-abstraction schema discovery.
     */
    public interface PlanResolutionDelegate {
        /**
         * Right after view resolution and in-subquery verification (the view-resolution profile marker already
         * stopped), build the {@link Configuration} and run the session's view/plan telemetry, EXPLAIN unwrap and IP
         * download side effects. Returns the plan to pre-analyze (EXPLAIN-unwrapped) and the Configuration that drives
         * resolution and execution.
         */
        Prepared afterViewResolution(ViewResolver.ViewResolutionResult viewResolution);

        /** Resolve the enrich policies referenced by the query. */
        void resolveEnrich(PreAnalyzer.PreAnalysis preAnalysis, TransportVersion minimumVersion, ActionListener<EnrichResolution> listener);

        /** Resolve the inference ids referenced by the query. */
        void resolveInference(PreAnalyzer.PreAnalysis preAnalysis, ActionListener<InferenceResolution> listener);

        /** Run the analyzer leaf on the schema-resolved plan. */
        LogicalPlan analyze(
            LogicalPlan parsed,
            UnmappedResolution unmappedResolution,
            Configuration configuration,
            PreAnalysisResult result,
            TimestampBounds timestampBounds
        ) throws Exception;

        /** The plan to pre-analyze and the Configuration to drive resolution and execution, produced after view resolution. */
        record Prepared(LogicalPlan plan, Configuration configuration) {}
    }

    /**
     * The singular schema-resolution entry: resolve {@code names} of any index-abstraction kind. Each name is routed
     * to the provider that {@link AbstractionSchemaProvider#handles its kind} and resolved into a {@link ResolvedSchema};
     * the caller never branches on what kind a name is.
     */
    public void resolveSchema(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<List<ResolvedSchema>> listener
    ) {
        var lookup = projectMetadata.getIndicesLookup();
        Map<AbstractionSchemaProvider, List<String>> byProvider = new LinkedHashMap<>();
        for (String name : names) {
            IndexAbstraction abstraction = lookup.get(name);
            // A name absent from the lookup is a not-yet-existing concrete index (e.g. a write-on-create target) or a
            // remote-cluster pattern the local lookup can't see — route it to the index provider, whose resolution
            // (and the security layer ahead of it) handles the missing/remote case, exactly as the legacy path did.
            IndexAbstraction.Type type = abstraction == null ? IndexAbstraction.Type.CONCRETE_INDEX : abstraction.getType();
            byProvider.computeIfAbsent(providerFor(type), p -> new ArrayList<>()).add(name);
        }
        if (byProvider.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }
        var grouped = new GroupedActionListener<List<ResolvedSchema>>(
            byProvider.size(),
            listener.map(perProvider -> perProvider.stream().flatMap(List::stream).toList())
        );
        byProvider.forEach((provider, providerNames) -> provider.resolveSchema(ctx, projectMetadata, providerNames, grouped));
    }

    /**
     * The coordinator's federating split (the design's {@code FederatingSchemaService}). Given a mix of local and
     * {@code alias:foo} names, resolve the local part in-process through the singular {@link #resolveSchema} dispatch,
     * resolve each remote alias's part by invoking {@code resolve_schema} on that remote via
     * {@code getRemoteClusterClient(alias).execute(...)} — the recursion: a remote node running its own
     * {@link SchemaService} umbrella against its own cluster state — then merge every kind's {@code Attribute}s into one
     * list. {@code Attribute} is the merge currency; the remote returns only schema (and, for datasets, config), never
     * its internal {@code IndexResolution} or expanded-view body.
     *
     * <p>This is additive: existing local resolution (datasets / indices / views via the in-process providers and the
     * field-caps CCS path) is untouched. The federation kicks in only when a name carries a remote-cluster prefix.
     *
     * <p>POC scope. Cross-version protocol-compat (gating wire fields against the negotiated {@code TransportVersion}
     * for old/new cluster pairs — the {@code #cps-project-team} "many protocol versions" problem) is a production
     * follow-up and is not handled here; the wire form assumes both clusters speak the current {@code resolve_schema}.
     */
    public void resolveSchemaFederated(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<List<ResolvedSchema>> listener
    ) {
        List<String> localNames = new ArrayList<>();
        Map<String, List<String>> byRemote = new LinkedHashMap<>();
        for (String name : names) {
            if (RemoteClusterAware.isRemoteIndexName(name)) {
                var split = RemoteClusterAware.splitIndexName(name);
                byRemote.computeIfAbsent(split.clusterAlias(), k -> new ArrayList<>()).add(split.indexExpression());
            } else {
                localNames.add(name);
            }
        }
        // No remote names → behaviour-identical to the in-process dispatch (the additive federation no-ops).
        if (byRemote.isEmpty()) {
            resolveSchema(ctx, projectMetadata, localNames, listener);
            return;
        }
        int legs = byRemote.size() + (localNames.isEmpty() ? 0 : 1);
        var grouped = new GroupedActionListener<List<ResolvedSchema>>(
            legs,
            listener.map(perLeg -> perLeg.stream().flatMap(List::stream).toList())
        );
        if (localNames.isEmpty() == false) {
            resolveSchema(ctx, projectMetadata, localNames, grouped);
        }
        byRemote.forEach((alias, remoteNames) -> resolveRemoteSchema(alias, remoteNames, grouped));
    }

    /**
     * Invoke {@code resolve_schema} on a single remote cluster. The remote runs its own {@link SchemaService} against
     * its own cluster state (the recursion to the remote umbrella) and returns the resolved schemas as wire-serialized
     * {@link ResolvedSchema.Remote}s; we re-qualify each name with the {@code alias:} prefix so merged attributes carry
     * their origin, exactly as the analyzer's existing currency expects.
     */
    private void resolveRemoteSchema(String alias, List<String> remoteNames, ActionListener<List<ResolvedSchema>> listener) {
        var remoteClient = remoteClusterService.getRemoteClusterClient(
            alias,
            executor,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
        );
        var request = new TransportResolveSchemaAction.Request(remoteNames.toArray(String[]::new));
        remoteClient.execute(
            TransportResolveSchemaAction.REMOTE_TYPE,
            request,
            new ThreadedActionListener<>(executor, listener.map(response -> {
                List<ResolvedSchema> qualified = new ArrayList<>(response.schemas().size());
                for (ResolvedSchema schema : response.schemas()) {
                    ResolvedSchema.Remote remote = (ResolvedSchema.Remote) schema;
                    qualified.add(
                        new ResolvedSchema.Remote(alias + ":" + remote.name(), remote.kind(), remote.attributes(), remote.config())
                    );
                }
                return qualified;
            }))
        );
    }

    // package-private for SchemaServiceTests
    AbstractionSchemaProvider providerFor(IndexAbstraction.Type type) {
        for (AbstractionSchemaProvider provider : providers) {
            if (provider.handles().contains(type)) {
                return provider;
            }
        }
        throw new IllegalStateException("no schema provider handles index abstraction type [" + type + "]");
    }

    /** Expand any view in {@code plan} into its underlying query. A plan rewrite, not a flat schema fetch. */
    public void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        viewProvider.replaceViews(plan, projectRouting, parser, listener);
    }

    /**
     * Authorize and rewrite {@code FROM <dataset>} targets into external relations so analysis treats them like the
     * inline {@code EXTERNAL} command. Only datasets the caller can {@code read} are rewritten.
     *
     * <p>The authorized datasets' configs are produced through the singular {@link #resolveSchema} dispatch — this is
     * the first production call site of the unified schema-discovery front. Each authorized dataset name routes to the
     * dataset provider's {@code resolveSchema} arm, which returns a {@link ResolvedSchema.Dataset} carrying the merged
     * external-source config; the dataset rewrite consumes that config instead of recomputing it inline. The merged
     * config is byte-identical to the inline path, so routing it through the umbrella is behaviour-identical.
     */
    public void resolveDatasets(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        datasetProvider.resolveDatasets(parsed, projectMetadata, this::resolveDatasetConfigs, listener);
    }

    /**
     * Resolve each authorized dataset name to its merged external-source config through the singular
     * {@link #resolveSchema} dispatch. The dispatch routes each name to the dataset provider's schema arm; we pull the
     * {@link ResolvedSchema.Dataset} config out of each result and key it by name for the rewrite.
     */
    private void resolveDatasetConfigs(
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<Map<String, Map<String, Object>>> listener
    ) {
        SchemaContext ctx = null; // the dataset schema arm reads no SchemaContext fields
        // Route through the federating split: it resolves local names in-process and any remote `alias:ds` names via the
        // remote umbrella, merging the configs. This is the live in-session caller of the federation entry — closing the
        // long-standing gap where the remotable resolve_schema action had no caller. For today's local-only dataset names
        // it is behaviour-identical (the split no-ops to the in-process dispatch when there are no remote expressions);
        // remote dataset *execution* (turning a remote config into an external read) is a production follow-up.
        resolveSchemaFederated(ctx, projectMetadata, names, listener.map(resolved -> {
            Map<String, Map<String, Object>> configs = new LinkedHashMap<>();
            for (ResolvedSchema schema : resolved) {
                Map<String, Object> config = switch (schema) {
                    case ResolvedSchema.Dataset dataset -> dataset.config();
                    case ResolvedSchema.Remote remote when remote.kind() == ResolvedSchema.Remote.Kind.DATASET -> remote.config();
                    default -> throw new IllegalStateException(
                        "expected a dataset schema for [" + schema.name() + "] but got [" + schema.getClass().getSimpleName() + "]"
                    );
                };
                configs.put(schema.name(), config);
            }
            return configs;
        }));
    }

    /** Resolve the schema of external sources (Iceberg tables / Parquet files) referenced by the query. */
    public void resolveExternalSources(LogicalPlan plan, List<String> icebergPaths, ActionListener<ExternalSourceResolution> listener) {
        datasetProvider.resolveExternalSources(plan, icebergPaths, listener);
    }

    /**
     * Resolve the main index patterns of the query: run the field-caps fetch for each pattern, init cross-cluster
     * state, and accumulate the resolution (plus the minimum transport version) into {@code result}.
     *
     * <p>The per-pattern field-caps fetch is sourced through the singular {@link #resolveSchema} dispatch — the index
     * provider's orchestration (CCS-state init, the time-series retry, version accumulation, telemetry) wraps the
     * umbrella fetch rather than calling {@code IndexResolver} inline. The fetch is byte-identical either way, so
     * routing it through the umbrella is behaviour-identical.
     */
    public void resolveMainIndices(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        PreAnalysisResult result,
        ActionListener<PreAnalysisResult> listener
    ) {
        indexProvider.useFetcher(indexFetcher(projectMetadata));
        indexProvider.resolveMainIndices(ctx, result, listener);
    }

    /**
     * Resolve every lookup index referenced by the query, validating each resolves to a single lookup-mode index. The
     * lookup field-caps fetch is sourced through the umbrella too; the validation (which reads the cross-cluster state
     * the main resolution populated) stays in the index provider.
     */
    public void resolveLookupIndices(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        PreAnalysisResult result,
        ActionListener<PreAnalysisResult> listener
    ) {
        indexProvider.useFetcher(indexFetcher(projectMetadata));
        indexProvider.resolveLookupIndices(ctx, result, listener);
    }

    /**
     * The umbrella-backed fetch the index orchestration sources each field-caps resolution from. {@code fetch} routes a
     * single, already-known-index pattern through the umbrella's index-schema arm — pinned to that arm rather than
     * re-running the type-based {@link #resolveSchema} routing, because the orchestration's patterns are genuine indices
     * (views and datasets are rewritten out of the plan before main-index resolution, and a CPS linked pattern is an
     * index name that may shadow a local view — re-routing by local type would misroute it). It wraps the request in a
     * one-shot {@link SchemaContext} and pulls the {@link ResolvedSchema.Index} resolution out. {@code fetchLookup}
     * routes the lookup-index fetch through the provider's lookup arm (its lookup-specific field-caps shape has no
     * per-name umbrella variant). The captured {@code projectMetadata} satisfies the arm's signature.
     */
    private IndexSchemaProvider.IndexResolutionFetcher indexFetcher(ProjectMetadata projectMetadata) {
        return new IndexSchemaProvider.IndexResolutionFetcher() {
            @Override
            public void fetch(IndexSchemaProvider.IndexSchemaRequest request, ActionListener<Versioned<IndexResolution>> listener) {
                indexProvider.resolveSchema(
                    SchemaContext.forIndexRequest(request),
                    projectMetadata,
                    List.of(request.pattern()),
                    listener.map(resolved -> {
                        if (resolved.size() != 1 || resolved.get(0) instanceof ResolvedSchema.Index == false) {
                            throw new IllegalStateException("expected a single index schema for [" + request.pattern() + "]");
                        }
                        return ((ResolvedSchema.Index) resolved.get(0)).resolution();
                    })
                );
            }

            @Override
            public void fetchLookup(
                String indexExpression,
                Set<String> fieldNames,
                TransportVersion minimumVersion,
                ActionListener<IndexResolution> listener
            ) {
                indexProvider.fetchLookupResolution(indexExpression, fieldNames, minimumVersion, listener);
            }
        };
    }
}
