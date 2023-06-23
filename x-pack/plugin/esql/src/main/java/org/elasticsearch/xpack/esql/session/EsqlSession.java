/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.Mapper;
import org.elasticsearch.xpack.ql.analyzer.TableInfo;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.MappingException;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.xpack.ql.util.ActionListeners.map;

public class EsqlSession {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSession.class);

    private final String sessionId;
    private final EsqlConfiguration configuration;
    private final IndexResolver indexResolver;
    private final EnrichPolicyResolver enrichPolicyResolver;

    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final FunctionRegistry functionRegistry;
    private final LogicalPlanOptimizer logicalPlanOptimizer;

    private final Mapper mapper;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;

    public EsqlSession(
        String sessionId,
        EsqlConfiguration configuration,
        IndexResolver indexResolver,
        EnrichPolicyResolver enrichPolicyResolver,
        PreAnalyzer preAnalyzer,
        FunctionRegistry functionRegistry,
        LogicalPlanOptimizer logicalPlanOptimizer,
        Mapper mapper,
        Verifier verifier
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
    }

    public String sessionId() {
        return sessionId;
    }

    public void execute(EsqlQueryRequest request, ActionListener<PhysicalPlan> listener) {
        LOGGER.debug("ESQL query:\n{}", request.query());
        optimizedPhysicalPlan(parse(request.query(), request.params()), listener.map(plan -> plan.transformUp(FragmentExec.class, f -> {
            QueryBuilder filter = request.filter();
            if (filter != null) {
                var fragmentFilter = f.esFilter();
                // TODO: have an ESFilter and push down to EsQueryExec / EsSource
                // This is an ugly hack to push the filter parameter to Lucene
                // TODO: filter integration testing
                filter = fragmentFilter != null ? boolQuery().filter(fragmentFilter).must(filter) : filter;
                LOGGER.debug("Fold filter {} to EsQueryExec", filter);
                f = new FragmentExec(f.source(), f.fragment(), filter);
            }
            return f;
        })));
    }

    private LogicalPlan parse(String query, List<TypedParamValue> params) {
        var parsed = new EsqlParser().createStatement(query, params);
        LOGGER.debug("Parsed logical plan:\n{}", parsed);
        return parsed;
    }

    public void analyzedPlan(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, (indices, policies) -> {
            Analyzer analyzer = new Analyzer(new AnalyzerContext(configuration, functionRegistry, indices, policies), verifier);
            var plan = analyzer.analyze(parsed);
            LOGGER.debug("Analyzed plan:\n{}", plan);
            return plan;
        }, listener);
    }

    private <T> void preAnalyze(LogicalPlan parsed, BiFunction<IndexResolution, EnrichResolution, T> action, ActionListener<T> listener) {
        PreAnalyzer.PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        Set<String> policyNames = new HashSet<>(preAnalysis.policyNames);
        EnrichResolution resolution = new EnrichResolution(ConcurrentCollections.newConcurrentSet(), enrichPolicyResolver.allPolicyNames());
        AtomicReference<IndexResolution> resolvedIndex = new AtomicReference<>();
        ActionListener<Void> groupedListener = ActionListener.wrap(unused -> {
            assert resolution.resolvedPolicies().size() == policyNames.size()
                : resolution.resolvedPolicies().size() + " != " + policyNames.size();
            assert resolvedIndex.get() != null : "index wasn't resolved";
            listener.onResponse(action.apply(resolvedIndex.get(), resolution));
        }, listener::onFailure);
        try (RefCountingListener refs = new RefCountingListener(groupedListener)) {
            preAnalyzeIndices(parsed, refs.acquire(resolvedIndex::set));
            for (String policyName : policyNames) {
                enrichPolicyResolver.resolvePolicy(policyName, refs.acquire(resolution.resolvedPolicies()::add));
            }
        }
    }

    private <T> void preAnalyzeIndices(LogicalPlan parsed, ActionListener<IndexResolution> listener) {
        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (preAnalysis.indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (preAnalysis.indices.size() == 1) {
            TableInfo tableInfo = preAnalysis.indices.get(0);
            TableIdentifier table = tableInfo.id();
            indexResolver.resolveAsMergedMapping(table.index(), false, Map.of(), listener);
        } else {
            try {
                // occurs when dealing with local relations (row a = 1)
                listener.onResponse(IndexResolution.invalid("[none specified]"));
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    public void optimizedPlan(LogicalPlan logicalPlan, ActionListener<LogicalPlan> listener) {
        analyzedPlan(logicalPlan, map(listener, p -> {
            var plan = logicalPlanOptimizer.optimize(p);
            LOGGER.debug("Optimized logicalPlan plan:\n{}", plan);
            return plan;
        }));
    }

    public void physicalPlan(LogicalPlan optimized, ActionListener<PhysicalPlan> listener) {
        optimizedPlan(optimized, map(listener, p -> {
            var plan = mapper.map(p);
            LOGGER.debug("Physical plan:\n{}", plan);
            return plan;
        }));
    }

    public void optimizedPhysicalPlan(LogicalPlan logicalPlan, ActionListener<PhysicalPlan> listener) {
        physicalPlan(logicalPlan, map(listener, p -> {
            var plan = physicalPlanOptimizer.optimize(p);
            LOGGER.debug("Optimized physical plan:\n{}", plan);
            return plan;
        }));
    }
}
