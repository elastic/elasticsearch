/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.MappingException;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer.PreAnalysis;
import org.elasticsearch.xpack.sql.analysis.analyzer.TableInfo;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.session.Cursor.Page;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.action.ActionListener.wrap;

public class SqlSession implements Session {

    private final Client client;

    private final FunctionRegistry functionRegistry;
    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final Optimizer optimizer;
    private final Planner planner;
    private final PlanExecutor planExecutor;
    
    private final SqlConfiguration configuration;

    public SqlSession(SqlConfiguration configuration, Client client, FunctionRegistry functionRegistry,
            IndexResolver indexResolver,
            PreAnalyzer preAnalyzer,
            Verifier verifier,
            Optimizer optimizer,
            Planner planner,
            PlanExecutor planExecutor) {
        this.client = client;
        this.functionRegistry = functionRegistry;

        this.indexResolver = indexResolver;
        this.preAnalyzer = preAnalyzer;
        this.optimizer = optimizer;
        this.planner = planner;
        this.verifier = verifier;

        this.configuration = configuration;
        this.planExecutor = planExecutor;
    }

    public FunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public Client client() {
        return client;
    }

    public Planner planner() {
        return planner;
    }

    public IndexResolver indexResolver() {
        return indexResolver;
    }

    public Optimizer optimizer() {
        return optimizer;
    }
    
    public Verifier verifier() {
        return verifier;
    }

    public PlanExecutor planExecutor() {
        return planExecutor;
    }

    private LogicalPlan doParse(String sql, List<SqlTypedParamValue> params) {
        return new SqlParser().createStatement(sql, params, configuration.zoneId());
    }

    public void analyzedPlan(LogicalPlan parsed, boolean verify, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, c -> {
            Analyzer analyzer = new Analyzer(configuration, functionRegistry, c, verifier);
            return analyzer.analyze(parsed, verify);
        }, listener);
    }

    public void debugAnalyzedPlan(LogicalPlan parsed, ActionListener<RuleExecutor<LogicalPlan>.ExecutionInfo> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(null);
            return;
        }

        preAnalyze(parsed, r -> {
            Analyzer analyzer = new Analyzer(configuration, functionRegistry, r, verifier);
            return analyzer.debugAnalyze(parsed);
        }, listener);
    }

    private <T> void preAnalyze(LogicalPlan parsed, Function<IndexResolution, T> action, ActionListener<T> listener) {
        PreAnalysis preAnalysis = preAnalyzer.preAnalyze(parsed);
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (preAnalysis.indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (preAnalysis.indices.size() == 1) {
            TableInfo tableInfo = preAnalysis.indices.get(0);
            TableIdentifier table = tableInfo.id();

            String cluster = table.cluster();

            if (Strings.hasText(cluster) && !indexResolver.clusterName().equals(cluster)) {
                listener.onFailure(new MappingException("Cannot inspect indices in cluster/catalog [{}]", cluster));
            }

            boolean includeFrozen = configuration.includeFrozen() || tableInfo.isFrozen();
            indexResolver.resolveAsMergedMapping(table.index(), null, includeFrozen,
                    wrap(indexResult -> listener.onResponse(action.apply(indexResult)), listener::onFailure));
        } else {
            try {
                // occurs when dealing with local relations (SELECT 5+2)
                listener.onResponse(action.apply(IndexResolution.invalid("[none specified]")));
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

    public void optimizedPlan(LogicalPlan verified, ActionListener<LogicalPlan> listener) {
        analyzedPlan(verified, true, wrap(v -> listener.onResponse(optimizer.optimize(v)), listener::onFailure));
    }

    public void physicalPlan(LogicalPlan optimized, boolean verify, ActionListener<PhysicalPlan> listener) {
        optimizedPlan(optimized, wrap(o -> listener.onResponse(planner.plan(o, verify)), listener::onFailure));
    }

    public void sql(String sql, List<SqlTypedParamValue> params, ActionListener<Page> listener) {
        sqlExecutable(sql, params, wrap(e -> e.execute(this, listener), listener::onFailure));
    }

    public void sqlExecutable(String sql, List<SqlTypedParamValue> params, ActionListener<PhysicalPlan> listener) {
        try {
            physicalPlan(doParse(sql, params), true, listener);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public SqlConfiguration configuration() {
        return configuration;
    }
}
