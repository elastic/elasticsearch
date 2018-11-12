/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer.PreAnalysis;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.analysis.index.MappingException;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.action.ActionListener.wrap;

public class SqlSession {

    private final Client client;

    private final FunctionRegistry functionRegistry;
    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final Optimizer optimizer;
    private final Planner planner;

    // TODO rename to `configuration`
    private final Configuration settings;

    public SqlSession(SqlSession other) {
        this(other.settings, other.client, other.functionRegistry, other.indexResolver,
                other.preAnalyzer, other.optimizer,other.planner);
    }

    public SqlSession(Configuration settings, Client client, FunctionRegistry functionRegistry,
            IndexResolver indexResolver,
            PreAnalyzer preAnalyzer,
            Optimizer optimizer,
            Planner planner) {
        this.client = client;
        this.functionRegistry = functionRegistry;

        this.indexResolver = indexResolver;
        this.preAnalyzer = preAnalyzer;
        this.optimizer = optimizer;
        this.planner = planner;

        this.settings = settings;
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

    private LogicalPlan doParse(String sql, List<SqlTypedParamValue> params) {
        return new SqlParser().createStatement(sql, params);
    }

    private void analyzedPlan(LogicalPlan parsed, boolean verify, ActionListener<LogicalPlan> listener, Map<String,
            CounterMetric> featuresMetrics) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, c -> {
            Analyzer analyzer = new Analyzer(functionRegistry, c, settings.timeZone());
            LogicalPlan p = analyzer.analyzeWithMetrics(parsed, featuresMetrics);
            return verify ? analyzer.verify(p, null) : p;
        }, listener);
    }
    
    public void analyzedPlan(LogicalPlan parsed, boolean verify, ActionListener<LogicalPlan> listener) {
        analyzedPlan(parsed, verify, listener, null);
    }
    
    public void analyzedPlanWithMetrics(LogicalPlan parsed, boolean verify, ActionListener<LogicalPlan> listener,
            Map<String, CounterMetric> featuresMetrics) {
        analyzedPlan(parsed, verify, listener, featuresMetrics);
    }

    public void debugAnalyzedPlan(LogicalPlan parsed, ActionListener<RuleExecutor<LogicalPlan>.ExecutionInfo> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(null);
            return;
        }

        preAnalyze(parsed, r -> {
            Analyzer analyzer = new Analyzer(functionRegistry, r, settings.timeZone());
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
            TableIdentifier table = preAnalysis.indices.get(0);

            String cluster = table.cluster();

            if (Strings.hasText(cluster) && !indexResolver.clusterName().equals(cluster)) {
                listener.onFailure(new MappingException("Cannot inspect indices in cluster/catalog [{}]", cluster));
            }

            indexResolver.resolveAsMergedMapping(table.index(), null,
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

    public void optimizedPlanWithMetrics(LogicalPlan verified, ActionListener<LogicalPlan> listener,
            Map<String, CounterMetric> featuresMetrics) {
        optimizedPlan(verified, listener, featuresMetrics);
    }
    
    public void optimizedPlan(LogicalPlan verified, ActionListener<LogicalPlan> listener) {
        optimizedPlan(verified, listener, null);
    }
    
    private void optimizedPlan(LogicalPlan verified, ActionListener<LogicalPlan> listener, Map<String, CounterMetric> featuresMetrics) {
        analyzedPlan(verified, true, wrap(v -> listener.onResponse(optimizer.optimize(v)), listener::onFailure), featuresMetrics);
    }

    public void physicalPlan(LogicalPlan optimized, boolean verify, ActionListener<PhysicalPlan> listener,
            Map<String, CounterMetric> featuresMetrics) {
        optimizedPlan(optimized, wrap(o -> listener.onResponse(planner.plan(o, verify)), listener::onFailure), featuresMetrics);
    }

    public void sql(String sql, List<SqlTypedParamValue> params, ActionListener<SchemaRowSet> listener,
            Map<String, CounterMetric> featuresMetrics) {
        sqlExecutableWithMetrics(sql, params, wrap(e -> e.execute(this, listener), listener::onFailure), featuresMetrics);
    }

    public void sqlExecutableWithMetrics(String sql, List<SqlTypedParamValue> params, ActionListener<PhysicalPlan> listener,
            Map<String, CounterMetric> featuresMetrics) {
        try {
            physicalPlan(doParse(sql, params), true, listener, featuresMetrics);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }
    
    public void sqlExecutable(String sql, List<SqlTypedParamValue> params, ActionListener<PhysicalPlan> listener) {
        try {
            physicalPlan(doParse(sql, params), true, listener, null);
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    public Configuration settings() {
        return settings;
    }
}
