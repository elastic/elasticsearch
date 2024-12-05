/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
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
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.stats.PlanningMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final Mapper mapper;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final PlanningMetrics planningMetrics;
    private final IndicesExpressionGrouper indicesExpressionGrouper;

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
        PlanningMetrics planningMetrics,
        IndicesExpressionGrouper indicesExpressionGrouper
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
        this.planningMetrics = planningMetrics;
        this.indicesExpressionGrouper = indicesExpressionGrouper;
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
            new EsqlSessionCCSUtils.CssPartialErrorsActionListener(executionInfo, listener) {
                @Override
                public void onResponse(LogicalPlan analyzedPlan) {
                    executeOptimizedPlan(request, executionInfo, planRunner, optimizedPlan(analyzedPlan), listener);
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
        EsqlSessionCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);
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
        var parsed = new EsqlParser().createStatement(query, params);
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

        TriFunction<IndexResolution, IndexResolution, EnrichResolution, LogicalPlan> analyzeAction = (indices, lookupIndices, policies) -> {
            planningMetrics.gatherPreAnalysisMetrics(parsed);
            Analyzer analyzer = new Analyzer(
                new AnalyzerContext(configuration, functionRegistry, indices, lookupIndices, policies),
                verifier
            );
            LogicalPlan plan = analyzer.analyze(parsed);
            plan.setAnalyzed();
            return plan;
        };

        PreAnalyzer.PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        var unresolvedPolicies = preAnalysis.enriches.stream()
            .map(e -> new EnrichPolicyResolver.UnresolvedPolicy((String) e.policyName().fold(), e.mode()))
            .collect(Collectors.toSet());
        final List<TableInfo> indices = preAnalysis.indices;
        // TODO: make a separate call for lookup indices
        final Set<String> targetClusters = enrichPolicyResolver.groupIndicesPerCluster(
            indices.stream().flatMap(t -> Arrays.stream(Strings.commaDelimitedListToStringArray(t.id().index()))).toArray(String[]::new)
        ).keySet();

        SubscribableListener.<EnrichResolution>newForked(l -> enrichPolicyResolver.resolvePolicies(targetClusters, unresolvedPolicies, l))
            .<ListenerResult>andThen((l, enrichResolution) -> {
                // we need the match_fields names from enrich policies and THEN, with an updated list of fields, we call field_caps API
                var enrichMatchFields = enrichResolution.resolvedEnrichPolicies()
                    .stream()
                    .map(ResolvedEnrichPolicy::matchField)
                    .collect(Collectors.toSet());
                // get the field names from the parsed plan combined with the ENRICH match fields from the ENRICH policy
                var fieldNames = fieldNames(parsed, enrichMatchFields);
                ListenerResult listenerResult = new ListenerResult(null, null, enrichResolution, fieldNames);

                // first resolve the lookup indices, then the main indices
                preAnalyzeLookupIndices(preAnalysis.lookupIndices, listenerResult, l);
            })
            .<ListenerResult>andThen((l, listenerResult) -> {
                // resolve the main indices
                preAnalyzeIndices(preAnalysis.indices, executionInfo, listenerResult, requestFilter, l);
            })
            .<ListenerResult>andThen((l, listenerResult) -> {
                // TODO in follow-PR (for skip_unavailable handling of missing concrete indexes) add some tests for
                // invalid index resolution to updateExecutionInfo
                if (listenerResult.indices.isValid()) {
                    // CCS indices and skip_unavailable cluster values can stop the analysis right here
                    if (analyzeCCSIndices(executionInfo, targetClusters, unresolvedPolicies, listenerResult, logicalPlanListener, l))
                        return;
                }
                // whatever tuple we have here (from CCS-special handling or from the original pre-analysis), pass it on to the next step
                l.onResponse(listenerResult);
            })
            .<ListenerResult>andThen((l, listenerResult) -> {
                // first attempt (maybe the only one) at analyzing the plan
                analyzeAndMaybeRetry(analyzeAction, requestFilter, listenerResult, logicalPlanListener, l);
            })
            .<ListenerResult>andThen((l, listenerResult) -> {
                assert requestFilter != null : "The second pre-analysis shouldn't take place when there is no index filter in the request";

                // "reset" execution information for all ccs or non-ccs (local) clusters, since we are performing the indices
                // resolving one more time (the first attempt failed and the query had a filter)
                for (String clusterAlias : executionInfo.clusterAliases()) {
                    executionInfo.swapCluster(clusterAlias, (k, v) -> null);
                }

                // here the requestFilter is set to null, performing the pre-analysis after the first step failed
                preAnalyzeIndices(preAnalysis.indices, executionInfo, listenerResult, null, l);
            })
            .<LogicalPlan>andThen((l, listenerResult) -> {
                assert requestFilter != null : "The second analysis shouldn't take place when there is no index filter in the request";
                LOGGER.debug("Analyzing the plan (second attempt, without filter)");
                LogicalPlan plan;
                try {
                    plan = analyzeAction.apply(listenerResult.indices, listenerResult.lookupIndices, listenerResult.enrichResolution);
                } catch (Exception e) {
                    l.onFailure(e);
                    return;
                }
                LOGGER.debug("Analyzed plan (second attempt, without filter):\n{}", plan);
                l.onResponse(plan);
            })
            .addListener(logicalPlanListener);
    }

    private void preAnalyzeLookupIndices(List<TableInfo> indices, ListenerResult listenerResult, ActionListener<ListenerResult> listener) {
        if (indices.size() > 1) {
            // Note: JOINs on more than one index are not yet supported
            listener.onFailure(new MappingException("More than one LOOKUP JOIN is not supported"));
        } else if (indices.size() == 1) {
            TableInfo tableInfo = indices.get(0);
            TableIdentifier table = tableInfo.id();
            // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
            indexResolver.resolveAsMergedMapping(
                table.index(),
                Set.of("*"), // TODO: for LOOKUP JOIN, this currently declares all lookup index fields relevant and might fetch too many.
                null,
                listener.map(indexResolution -> listenerResult.withLookupIndexResolution(indexResolution))
            );
            // TODO: Verify that the resolved index actually has indexMode: "lookup"
        } else {
            try {
                // No lookup indices specified
                listener.onResponse(
                    new ListenerResult(
                        listenerResult.indices,
                        IndexResolution.invalid("[none specified]"),
                        listenerResult.enrichResolution,
                        listenerResult.fieldNames
                    )
                );
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    private void preAnalyzeIndices(
        List<TableInfo> indices,
        EsqlExecutionInfo executionInfo,
        ListenerResult listenerResult,
        QueryBuilder requestFilter,
        ActionListener<ListenerResult> listener
    ) {
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (indices.size() == 1) {
            // known to be unavailable from the enrich policy API call
            Map<String, Exception> unavailableClusters = listenerResult.enrichResolution.getUnavailableClusters();
            TableInfo tableInfo = indices.get(0);
            TableIdentifier table = tableInfo.id();

            Map<String, OriginalIndices> clusterIndices = indicesExpressionGrouper.groupIndices(IndicesOptions.DEFAULT, table.index());
            for (Map.Entry<String, OriginalIndices> entry : clusterIndices.entrySet()) {
                final String clusterAlias = entry.getKey();
                String indexExpr = Strings.arrayToCommaDelimitedString(entry.getValue().indices());
                executionInfo.swapCluster(clusterAlias, (k, v) -> {
                    assert v == null : "No cluster for " + clusterAlias + " should have been added to ExecutionInfo yet";
                    if (unavailableClusters.containsKey(k)) {
                        return new EsqlExecutionInfo.Cluster(
                            clusterAlias,
                            indexExpr,
                            executionInfo.isSkipUnavailable(clusterAlias),
                            EsqlExecutionInfo.Cluster.Status.SKIPPED,
                            0,
                            0,
                            0,
                            0,
                            List.of(new ShardSearchFailure(unavailableClusters.get(k))),
                            new TimeValue(0)
                        );
                    } else {
                        return new EsqlExecutionInfo.Cluster(clusterAlias, indexExpr, executionInfo.isSkipUnavailable(clusterAlias));
                    }
                });
            }
            // if the preceding call to the enrich policy API found unavailable clusters, recreate the index expression to search
            // based only on available clusters (which could now be an empty list)
            String indexExpressionToResolve = EsqlSessionCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            if (indexExpressionToResolve.isEmpty()) {
                // if this was a pure remote CCS request (no local indices) and all remotes are offline, return an empty IndexResolution
                listener.onResponse(
                    new ListenerResult(
                        IndexResolution.valid(new EsIndex(table.index(), Map.of(), Map.of())),
                        listenerResult.lookupIndices,
                        listenerResult.enrichResolution,
                        listenerResult.fieldNames
                    )
                );
            } else {
                // call the EsqlResolveFieldsAction (field-caps) to resolve indices and get field types
                indexResolver.resolveAsMergedMapping(
                    indexExpressionToResolve,
                    listenerResult.fieldNames,
                    requestFilter,
                    listener.map(indexResolution -> listenerResult.withIndexResolution(indexResolution))
                );
            }
        } else {
            try {
                // occurs when dealing with local relations (row a = 1)
                listener.onResponse(
                    new ListenerResult(
                        IndexResolution.invalid("[none specified]"),
                        listenerResult.lookupIndices,
                        listenerResult.enrichResolution,
                        listenerResult.fieldNames
                    )
                );
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    private boolean analyzeCCSIndices(
        EsqlExecutionInfo executionInfo,
        Set<String> targetClusters,
        Set<EnrichPolicyResolver.UnresolvedPolicy> unresolvedPolicies,
        ListenerResult listenerResult,
        ActionListener<LogicalPlan> logicalPlanListener,
        ActionListener<ListenerResult> l
    ) {
        IndexResolution indexResolution = listenerResult.indices;
        EsqlSessionCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);
        EsqlSessionCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, indexResolution.unavailableClusters());
        if (executionInfo.isCrossClusterSearch() && executionInfo.getClusterStateCount(EsqlExecutionInfo.Cluster.Status.RUNNING) == 0) {
            // for a CCS, if all clusters have been marked as SKIPPED, nothing to search so send a sentinel Exception
            // to let the LogicalPlanActionListener decide how to proceed
            logicalPlanListener.onFailure(new NoClustersToSearchException());
            return true;
        }

        Set<String> newClusters = enrichPolicyResolver.groupIndicesPerCluster(
            indexResolution.get().concreteIndices().toArray(String[]::new)
        ).keySet();
        // If new clusters appear when resolving the main indices, we need to resolve the enrich policies again
        // or exclude main concrete indices. Since this is rare, it's simpler to resolve the enrich policies again.
        // TODO: add a test for this
        if (targetClusters.containsAll(newClusters) == false
            // do not bother with a re-resolution if only remotes were requested and all were offline
            && executionInfo.getClusterStateCount(EsqlExecutionInfo.Cluster.Status.RUNNING) > 0) {
            enrichPolicyResolver.resolvePolicies(
                newClusters,
                unresolvedPolicies,
                l.map(enrichResolution -> listenerResult.withEnrichResolution(enrichResolution))
            );
            return true;
        }
        return false;
    }

    private static void analyzeAndMaybeRetry(
        TriFunction<IndexResolution, IndexResolution, EnrichResolution, LogicalPlan> analyzeAction,
        QueryBuilder requestFilter,
        ListenerResult listenerResult,
        ActionListener<LogicalPlan> logicalPlanListener,
        ActionListener<ListenerResult> l
    ) {
        LogicalPlan plan = null;
        var filterPresentMessage = requestFilter == null ? "without" : "with";
        var attemptMessage = requestFilter == null ? "the only" : "first";
        LOGGER.debug("Analyzing the plan ({} attempt, {} filter)", attemptMessage, filterPresentMessage);

        try {
            plan = analyzeAction.apply(listenerResult.indices, listenerResult.lookupIndices, listenerResult.enrichResolution);
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
                    l.onResponse(listenerResult);
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

    static Set<String> fieldNames(LogicalPlan parsed, Set<String> enrichPolicyMatchFields) {
        if (false == parsed.anyMatch(plan -> plan instanceof Aggregate || plan instanceof Project)) {
            // no explicit columns selection, for example "from employees"
            return IndexResolver.ALL_FIELDS;
        }

        Holder<Boolean> projectAll = new Holder<>(false);
        parsed.forEachExpressionDown(UnresolvedStar.class, us -> {// explicit "*" fields selection
            if (projectAll.get()) {
                return;
            }
            projectAll.set(true);
        });
        if (projectAll.get()) {
            return IndexResolver.ALL_FIELDS;
        }

        AttributeSet references = new AttributeSet();
        // "keep" attributes are special whenever a wildcard is used in their name
        // ie "from test | eval lang = languages + 1 | keep *l" should consider both "languages" and "*l" as valid fields to ask for
        AttributeSet keepCommandReferences = new AttributeSet();
        AttributeSet keepJoinReferences = new AttributeSet();

        parsed.forEachDown(p -> {// go over each plan top-down
            if (p instanceof RegexExtract re) { // for Grok and Dissect
                // remove other down-the-tree references to the extracted fields
                for (Attribute extracted : re.extractedFields()) {
                    references.removeIf(attr -> matchByName(attr, extracted.name(), false));
                }
                // but keep the inputs needed by Grok/Dissect
                references.addAll(re.input().references());
            } else if (p instanceof Enrich enrich) {
                AttributeSet enrichRefs = Expressions.references(enrich.enrichFields());
                enrichRefs = enrichRefs.combine(enrich.matchField().references());
                // Enrich adds an EmptyAttribute if no match field is specified
                // The exact name of the field will be added later as part of enrichPolicyMatchFields Set
                enrichRefs.removeIf(attr -> attr instanceof EmptyAttribute);
                references.addAll(enrichRefs);
            } else if (p instanceof LookupJoin join) {
                keepJoinReferences.addAll(join.config().matchFields());  // TODO: why is this empty
                if (join.config().type() instanceof JoinTypes.UsingJoinType usingJoinType) {
                    keepJoinReferences.addAll(usingJoinType.columns());
                }
            } else {
                references.addAll(p.references());
                if (p instanceof UnresolvedRelation ur && ur.indexMode() == IndexMode.TIME_SERIES) {
                    // METRICS aggs generally rely on @timestamp without the user having to mention it.
                    references.add(new UnresolvedAttribute(ur.source(), MetadataAttribute.TIMESTAMP_FIELD));
                }
                // special handling for UnresolvedPattern (which is not an UnresolvedAttribute)
                p.forEachExpression(UnresolvedNamePattern.class, up -> {
                    var ua = new UnresolvedAttribute(up.source(), up.name());
                    references.add(ua);
                    if (p instanceof Keep) {
                        keepCommandReferences.add(ua);
                    }
                });
                if (p instanceof Keep) {
                    keepCommandReferences.addAll(p.references());
                }
            }

            // remove any already discovered UnresolvedAttributes that are in fact aliases defined later down in the tree
            // for example "from test | eval x = salary | stats max = max(x) by gender"
            // remove the UnresolvedAttribute "x", since that is an Alias defined in "eval"
            AttributeSet planRefs = p.references();
            p.forEachExpressionDown(Alias.class, alias -> {
                // do not remove the UnresolvedAttribute that has the same name as its alias, ie "rename id = id"
                // or the UnresolvedAttributes that are used in Functions that have aliases "STATS id = MAX(id)"
                if (planRefs.names().contains(alias.name())) {
                    return;
                }
                references.removeIf(attr -> matchByName(attr, alias.name(), keepCommandReferences.contains(attr)));
            });
        });
        // Add JOIN ON column references afterward to avoid Alias removal
        references.addAll(keepJoinReferences);

        // remove valid metadata attributes because they will be filtered out by the IndexResolver anyway
        // otherwise, in some edge cases, we will fail to ask for "*" (all fields) instead
        references.removeIf(a -> a instanceof MetadataAttribute || MetadataAttribute.isSupported(a.name()));
        Set<String> fieldNames = references.names();

        if (fieldNames.isEmpty() && enrichPolicyMatchFields.isEmpty()) {
            // there cannot be an empty list of fields, we'll ask the simplest and lightest one instead: _index
            return IndexResolver.INDEX_METADATA_FIELD;
        } else {
            fieldNames.addAll(subfields(fieldNames));
            fieldNames.addAll(enrichPolicyMatchFields);
            fieldNames.addAll(subfields(enrichPolicyMatchFields));
            return fieldNames;
        }
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

    private record ListenerResult(
        IndexResolution indices,
        IndexResolution lookupIndices,
        EnrichResolution enrichResolution,
        Set<String> fieldNames
    ) {
        ListenerResult withEnrichResolution(EnrichResolution newEnrichResolution) {
            return new ListenerResult(indices(), lookupIndices(), newEnrichResolution, fieldNames());
        }

        ListenerResult withIndexResolution(IndexResolution newIndexResolution) {
            return new ListenerResult(newIndexResolution, lookupIndices(), enrichResolution(), fieldNames());
        }

        ListenerResult withLookupIndexResolution(IndexResolution newIndexResolution) {
            return new ListenerResult(indices(), newIndexResolution, enrichResolution(), fieldNames());
        }
    };
}
