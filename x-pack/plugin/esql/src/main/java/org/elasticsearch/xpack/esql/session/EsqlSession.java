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
import org.elasticsearch.xpack.esql.analyzer.Analyzer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.physical.Mapper;
import org.elasticsearch.xpack.esql.plan.physical.Optimizer;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.analyzer.PreAnalyzer;
import org.elasticsearch.xpack.ql.analyzer.TableInfo;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.MappingException;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.action.ActionListener.wrap;

public class EsqlSession {

    private static final Logger LOGGER = LogManager.getLogger(EsqlSession.class);

    private final IndexResolver indexResolver;
    private final FunctionRegistry functionRegistry;
    private final EsqlConfiguration configuration;

    public EsqlSession(IndexResolver indexResolver, FunctionRegistry functionRegistry, EsqlConfiguration configuration) {
        this.indexResolver = indexResolver;
        this.functionRegistry = functionRegistry;
        this.configuration = configuration;
    }

    public void execute(String query, ActionListener<PhysicalPlan> listener) {
        LogicalPlan parsed;
        LOGGER.debug("ESQL query:\n{}", query);
        try {
            parsed = parse(query);
            LOGGER.debug("Parsed logical plan:\n{}", parsed);
        } catch (ParsingException pe) {
            listener.onFailure(pe);
            return;
        }

        analyzedPlan(parsed, ActionListener.wrap(plan -> {
            LOGGER.debug("Analyzed logical plan:\n{}", plan);
            Mapper mapper = new Mapper();
            PhysicalPlan physicalPlan = mapper.map(plan);
            LOGGER.debug("Physical plan:\n{}", physicalPlan);
            Optimizer optimizer = new Optimizer(configuration);
            physicalPlan = optimizer.optimize(physicalPlan);
            LOGGER.debug("Optimized physical plan:\n{}", physicalPlan);
            listener.onResponse(physicalPlan);
        }, listener::onFailure));
    }

    private LogicalPlan parse(String query) {
        return new EsqlParser().createStatement(query);
    }

    public void analyzedPlan(LogicalPlan parsed, ActionListener<LogicalPlan> listener) {
        if (parsed.analyzed()) {
            listener.onResponse(parsed);
            return;
        }

        preAnalyze(parsed, r -> {
            Analyzer analyzer = new Analyzer(r, functionRegistry, configuration);
            return analyzer.analyze(parsed);
        }, listener);
    }

    private <T> void preAnalyze(LogicalPlan parsed, Function<IndexResolution, T> action, ActionListener<T> listener) {
        PreAnalyzer.PreAnalysis preAnalysis = new PreAnalyzer().preAnalyze(parsed);
        // TODO we plan to support joins in the future when possible, but for now we'll just fail early if we see one
        if (preAnalysis.indices.size() > 1) {
            // Note: JOINs are not supported but we detect them when
            listener.onFailure(new MappingException("Queries with multiple indices are not supported"));
        } else if (preAnalysis.indices.size() == 1) {
            TableInfo tableInfo = preAnalysis.indices.get(0);
            TableIdentifier table = tableInfo.id();

            indexResolver.resolveAsMergedMapping(
                table.index(),
                false,
                Map.of(),
                wrap(indexResult -> listener.onResponse(action.apply(indexResult)), listener::onFailure)
            );
        } else {
            try {
                // occurs when dealing with local relations (row a = 1)
                listener.onResponse(action.apply(IndexResolution.invalid("[none specified]")));
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }
    }

}
