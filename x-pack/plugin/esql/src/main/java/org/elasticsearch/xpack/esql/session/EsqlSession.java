/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.TableInfo;
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
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
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
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.planner.premapper.PreMapper;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;

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
        analyzedPlan(
            parse(request.query(), request.params()),
            executionInfo,
            request.filter(),
            new EsqlCCSUtils.CssPartialErrorsActionListener(executionInfo, listener) {
                @Override
                public void onResponse(LogicalPlan analyzedPlan) {
                    preMapper.preMapper(
                        analyzedPlan,
                        listener.delegateFailureAndWrap(
                            (l, p) -> executeOptimizedPlan(request, executionInfo, planRunner, optimizedPlan(p), l)
                        )
                    );
                }
            }
        );
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
        PhysicalPlan physicalPlan = logicalPlanToPhysicalPlan(optimizedPlan, request);
        // TODO: this could be snuck into the underlying listener
        EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);
        // execute any potential subplans
        executeSubPlans(physicalPlan, planRunner, executionInfo, request, listener);
    }

    private record PlanTuple(PhysicalPlan physical, LogicalPlan logical) {}

    private void executeSubPlans(
        PhysicalPlan physicalPlan,
        PlanRunner runner,
        EsqlExecutionInfo executionInfo,
        EsqlQueryRequest request,
        ActionListener<Result> listener
    ) {
        List<PlanTuple> subplans = new ArrayList<>();

        // Currently the inlinestats are limited and supported as streaming operators, thus present inside the fragment as logical plans
        // Below they get collected, translated into a separate, coordinator based plan and the results 'broadcasted' as a local relation
        physicalPlan.forEachUp(FragmentExec.class, f -> {
            f.fragment().forEachUp(InlineJoin.class, ij -> {
                // extract the right side of the plan and replace its source
                LogicalPlan subplan = InlineJoin.replaceStub(ij.left(), ij.right());
                // mark the new root node as optimized
                subplan.setOptimized();
                PhysicalPlan subqueryPlan = logicalPlanToPhysicalPlan(subplan, request);
                subplans.add(new PlanTuple(subqueryPlan, ij.right()));
            });
        });

        Iterator<PlanTuple> iterator = subplans.iterator();

        // TODO: merge into one method
        if (subplans.size() > 0) {
            // code-path to execute subplans
            executeSubPlan(new ArrayList<>(), physicalPlan, iterator, executionInfo, runner, listener);
        } else {
            // execute main plan
            runner.run(physicalPlan, listener);
        }
    }

    private void executeSubPlan(
        List<DriverProfile> profileAccumulator,
        PhysicalPlan plan,
        Iterator<PlanTuple> subPlanIterator,
        EsqlExecutionInfo executionInfo,
        PlanRunner runner,
        ActionListener<Result> listener
    ) {
        PlanTuple tuple = subPlanIterator.next();

        runner.run(tuple.physical, listener.delegateFailureAndWrap((next, result) -> {
            try {
                profileAccumulator.addAll(result.profiles());
                LocalRelation resultWrapper = resultToPlan(tuple.logical, result);

                // replace the original logical plan with the backing result
                final PhysicalPlan newPlan = plan.transformUp(FragmentExec.class, f -> {
                    LogicalPlan frag = f.fragment();
                    return f.withFragment(
                        frag.transformUp(
                            InlineJoin.class,
                            ij -> ij.right() == tuple.logical ? InlineJoin.inlineData(ij, resultWrapper) : ij
                        )
                    );
                });
                if (subPlanIterator.hasNext() == false) {
                    runner.run(newPlan, next.delegateFailureAndWrap((finalListener, finalResult) -> {
                        profileAccumulator.addAll(finalResult.profiles());
                        finalListener.onResponse(new Result(finalResult.schema(), finalResult.pages(), profileAccumulator, executionInfo));
                    }));
                } else {
                    // continue executing the subplans
                    executeSubPlan(profileAccumulator, newPlan, subPlanIterator, executionInfo, runner, next);
                }
            } finally {
                Releasables.closeExpectNoException(Releasables.wrap(Iterators.map(result.pages().iterator(), p -> p::releaseBlocks)));
            }
        }));
    }

    private LocalRelation resultToPlan(LogicalPlan plan, Result result) {
        List<Page> pages = result.pages();
        List<Attribute> schema = result.schema();
        // if (pages.size() > 1) {
        Block[] blocks = SessionUtils.fromPages(schema, pages);
        return new LocalRelation(plan.source(), schema, LocalSupplier.of(blocks));
    }

    private LogicalPlan parse(String query, QueryParams params) {
        var parsed = new EsqlParser().createStatement(query, params, planTelemetry);
        LOGGER.debug("Parsed logical plan:\n{}", parsed);
        return parsed;
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

        Function<PreAnalysisResult, LogicalPlan> analyzeAction = (l) -> {
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
                    (String) e.policyName().fold(FoldContext.small() /* TODO remove me*/),
                    e.mode()
                )
            )
            .collect(Collectors.toSet());
        final List<TableInfo> indices = preAnalysis.indices;

        EsqlCCSUtils.checkForCcsLicense(executionInfo, indices, indicesExpressionGrouper, configuredClusters, verifier.licenseState());
        initializeClusterData(indices, executionInfo);

        var listener = SubscribableListener.<EnrichResolution>newForked(
            l -> enrichPolicyResolver.resolvePolicies(unresolvedPolicies, executionInfo, l)
        )
            .<PreAnalysisResult>andThen((l, enrichResolution) -> resolveFieldNames(parsed, enrichResolution, l))
            .<PreAnalysisResult>andThen((l, preAnalysisResult) -> resolveInferences(preAnalysis.inferencePlans, preAnalysisResult, l));
        // first resolve the lookup indices, then the main indices
        for (TableInfo lookupIndex : preAnalysis.lookupIndices) {
            listener = listener.andThen((l, preAnalysisResult) -> { preAnalyzeLookupIndex(lookupIndex, preAnalysisResult, l); });
        }
        listener.<PreAnalysisResult>andThen((l, result) -> {
            // resolve the main indices
            preAnalyzeIndices(preAnalysis.indices, executionInfo, result, requestFilter, l);
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
            preAnalyzeIndices(preAnalysis.indices, executionInfo, result, null, l);
        }).<LogicalPlan>andThen((l, result) -> {
            assert requestFilter != null : "The second analysis shouldn't take place when there is no index filter in the request";
            LOGGER.debug("Analyzing the plan (second attempt, without filter)");
            LogicalPlan plan;
            try {
                // the order here is tricky - if the cluster has been filtered and later became unavailable,
                // do we want to declare it successful or skipped? For now, unavailability takes precedence.
                EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, result.indices.unavailableClusters());
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, result.indices, null);
                plan = analyzeAction.apply(result);
            } catch (Exception e) {
                l.onFailure(e);
                return;
            }
            LOGGER.debug("Analyzed plan (second attempt, without filter):\n{}", plan);
            l.onResponse(plan);
        }).addListener(logicalPlanListener);
    }

    private void preAnalyzeLookupIndex(TableInfo tableInfo, PreAnalysisResult result, ActionListener<PreAnalysisResult> listener) {
        IndexPattern table = tableInfo.id();
        Set<String> fieldNames = result.wildcardJoinIndices().contains(table.indexPattern()) ? IndexResolver.ALL_FIELDS : result.fieldNames;
        // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
        indexResolver.resolveAsMergedMapping(
            table.indexPattern(),
            fieldNames,
            null,
            listener.map(indexResolution -> result.addLookupIndexResolution(table.indexPattern(), indexResolution))
        );
        // TODO: Verify that the resolved index actually has indexMode: "lookup"
    }

    private void initializeClusterData(List<TableInfo> indices, EsqlExecutionInfo executionInfo) {
        if (indices.isEmpty()) {
            return;
        }
        assert indices.size() == 1 : "Only single index pattern is supported";
        Map<String, OriginalIndices> clusterIndices = indicesExpressionGrouper.groupIndices(
            configuredClusters,
            IndicesOptions.DEFAULT,
            indices.get(0).id().indexPattern()
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

    private void preAnalyzeIndices(
        List<TableInfo> indices,
        EsqlExecutionInfo executionInfo,
        PreAnalysisResult result,
        QueryBuilder requestFilter,
        ActionListener<PreAnalysisResult> listener
    ) {
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (indices.size() == 1) {
            TableInfo tableInfo = indices.get(0);
            IndexPattern table = tableInfo.id();

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
                indexResolver.resolveAsMergedMapping(
                    indexExpressionToResolve,
                    result.fieldNames,
                    requestFilter,
                    listener.delegateFailure((l, indexResolution) -> {
                        if (configuration.allowPartialResults() == false && indexResolution.getUnavailableShards().isEmpty() == false) {
                            l.onFailure(indexResolution.getUnavailableShards().iterator().next());
                        } else {
                            l.onResponse(result.withIndexResolution(indexResolution));
                        }
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
        EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.unavailableClusters());
        if (executionInfo.isCrossClusterSearch() && executionInfo.getClusterStateCount(EsqlExecutionInfo.Cluster.Status.RUNNING) == 0) {
            // for a CCS, if all clusters have been marked as SKIPPED, nothing to search so send a sentinel Exception
            // to let the LogicalPlanActionListener decide how to proceed
            LOGGER.debug("No more clusters to search, ending analysis stage");
            logicalPlanListener.onFailure(new NoClustersToSearchException());
            return true;
        }

        return false;
    }

    private static void analyzeAndMaybeRetry(
        Function<PreAnalysisResult, LogicalPlan> analyzeAction,
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
                EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, result.indices, requestFilter);
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
        List<InferencePlan> inferencePlans,
        PreAnalysisResult preAnalysisResult,
        ActionListener<PreAnalysisResult> l
    ) {
        inferenceRunner.resolveInferenceIds(inferencePlans, l.map(preAnalysisResult::withInferenceResolution));
    }

    static PreAnalysisResult fieldNames(LogicalPlan parsed, Set<String> enrichPolicyMatchFields, PreAnalysisResult result) {
        if (false == parsed.anyMatch(plan -> plan instanceof Aggregate || plan instanceof Project)) {
            // no explicit columns selection, for example "from employees"
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
        // "keep" and "drop" attributes are special whenever a wildcard is used in their name, as the wildcard can shadow some
        // attributes ("lookup join" generated columns among others) and steps like removal of Aliases should ignore the fields
        // to remove if their name matches one of these wildcards.
        //
        // ie "from test | eval lang = languages + 1 | keep *l" should consider both "languages" and "*l" as valid fields to ask for
        // "from test | eval first_name = 1 | drop first_name | drop *name should also consider "*name" as valid field to ask for
        //
        // NOTE: the grammar allows wildcards to be used in other commands as well, but these are forbidden in the LogicalPlanBuilder
        var shadowingRefsBuilder = AttributeSet.builder();
        var keepJoinRefsBuilder = AttributeSet.builder();
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
                    keepJoinRefsBuilder.addAll(usingJoinType.columns());
                }
                if (shadowingRefsBuilder.isEmpty()) {
                    // No KEEP commands after the JOIN, so we need to mark this index for "*" field resolution
                    wildcardJoinIndices.add(((UnresolvedRelation) join.right()).indexPattern().indexPattern());
                } else {
                    // Keep commands can reference the join columns with names that shadow aliases, so we block their removal
                    keepJoinRefsBuilder.addAll(shadowingRefsBuilder);
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
                    shadowingRefsBuilder.add(ua);
                });
                if (p instanceof Keep) {
                    shadowingRefsBuilder.addAll(p.references());
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
                    referencesBuilder.removeIf(attr -> matchByName(attr, ne.name(), shadowingRefsBuilder.contains(attr)));
                });
            }
        });

        // Add JOIN ON column references afterward to avoid Alias removal
        referencesBuilder.addAll(keepJoinRefsBuilder);
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
     * Could a plan "accidentally" override aliases?
     * Examples are JOIN and ENRICH, that _could_ produce fields with the same
     * name of an existing alias, based on their index mapping.
     * Here we just have to consider commands where this information is not available before index resolution,
     * eg. EVAL, GROK, DISSECT can override an alias, but we know it in advance, ie. we don't need to resolve indices to know.
     */
    private static boolean couldOverrideAliases(LogicalPlan p) {
        return (p instanceof Aggregate
            || p instanceof Drop
            || p instanceof Eval
            || p instanceof Filter
            || p instanceof InlineStats
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
            return new PreAnalysisResult(newIndexResolution, lookupIndices(), enrichResolution(), fieldNames(), wildcardJoinIndices(), inferenceResolution());
        }

        PreAnalysisResult addLookupIndexResolution(String index, IndexResolution newIndexResolution) {
            lookupIndices.put(index, newIndexResolution);
            return this;
        }

        PreAnalysisResult withFieldNames(Set<String> newFields) {
            return new PreAnalysisResult(indices(), lookupIndices(), enrichResolution(), newFields, wildcardJoinIndices(), inferenceResolution());
        }

        public PreAnalysisResult withWildcardJoinIndices(Set<String> wildcardJoinIndices) {
            return new PreAnalysisResult(indices(), lookupIndices(), enrichResolution(), fieldNames(), wildcardJoinIndices, inferenceResolution());
        }
    }
}
