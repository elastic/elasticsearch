/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.sql.analysis.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.execution.search.SourceGenerator;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionRegistry;
import org.elasticsearch.xpack.sql.optimizer.Optimizer;
import org.elasticsearch.xpack.sql.plan.physical.CommandExec;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.planner.Planner;
import org.elasticsearch.xpack.sql.planner.PlanningException;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.stats.QueryMetric;

import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;

public class PlanExecutor {
    private final Client client;
    private final NamedWriteableRegistry writableRegistry;

    private final FunctionRegistry functionRegistry;

    private final IndexResolver indexResolver;
    private final PreAnalyzer preAnalyzer;
    private final Verifier verifier;
    private final Optimizer optimizer;
    private final Planner planner;
    
    private final Metrics metrics;

    public PlanExecutor(Client client, IndexResolver indexResolver, NamedWriteableRegistry writeableRegistry) {
        this.client = client;
        this.writableRegistry = writeableRegistry;

        this.indexResolver = indexResolver;
        this.functionRegistry = new SqlFunctionRegistry();
        
        this.metrics = new Metrics();

        this.preAnalyzer = new PreAnalyzer();
        this.verifier = new Verifier(metrics);
        this.optimizer = new Optimizer();
        this.planner = new Planner();
    }

    private SqlSession newSession(Configuration cfg) {
        return new SqlSession(cfg, client, functionRegistry, indexResolver, preAnalyzer, verifier, optimizer, planner, this);
    }

    public void searchSource(Configuration cfg, String sql, List<SqlTypedParamValue> params, ActionListener<SearchSourceBuilder> listener) {
        metrics.translate();

        newSession(cfg).sqlExecutable(sql, params, wrap(exec -> {
            if (exec instanceof EsQueryExec) {
                EsQueryExec e = (EsQueryExec) exec;
                listener.onResponse(SourceGenerator.sourceBuilder(e.queryContainer(), cfg.filter(), cfg.pageSize()));
            }
            // try to provide a better resolution of what failed
            else {
                String message = null;
                if (exec instanceof LocalExec) {
                    message = "Cannot generate a query DSL for an SQL query that either " +
                            "its WHERE clause evaluates to FALSE or doesn't operate on a table (missing a FROM clause)";
                } else if (exec instanceof CommandExec) {
                    message = "Cannot generate a query DSL for a special SQL command " +
                            "(e.g.: DESCRIBE, SHOW)";
                } else {
                    message = "Cannot generate a query DSL";
                }
                listener.onFailure(new PlanningException(message + ", sql statement: [{}]", sql));
            }
        }, listener::onFailure));
    }

    public void sql(Configuration cfg, String sql, List<SqlTypedParamValue> params, ActionListener<Page> listener) {
        QueryMetric metric = QueryMetric.from(cfg.mode(), cfg.clientId());
        metrics.total(metric);

        newSession(cfg).sql(sql, params, wrap(listener::onResponse, ex -> {
            metrics.failed(metric);
            listener.onFailure(ex);
        }));
    }

    public void nextPage(Configuration cfg, Cursor cursor, ActionListener<Page> listener) {
        QueryMetric metric = QueryMetric.from(cfg.mode(), cfg.clientId());
        metrics.total(metric);
        metrics.paging(metric);

        cursor.nextPage(cfg, client, writableRegistry, wrap(listener::onResponse, ex -> {
            metrics.failed(metric);
            listener.onFailure(ex);
        }));
    }

    public void cleanCursor(Configuration cfg, Cursor cursor, ActionListener<Boolean> listener) {
        cursor.clear(cfg, client, listener);
    }
    
    public Metrics metrics() {
        return this.metrics;
    }
}
