/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;

public class TestPlannerOptimizer {
    private final EsqlParser parser;
    private final Analyzer analyzer;
    private final LogicalPlanOptimizer logicalOptimizer;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final Mapper mapper;
    private final Configuration config;

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer) {
        this(config, analyzer, new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small())));
    }

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer, LogicalPlanOptimizer logicalOptimizer) {
        this.analyzer = analyzer;
        this.config = config;
        this.logicalOptimizer = logicalOptimizer;

        parser = new EsqlParser();
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        mapper = new Mapper();

    }

    public PhysicalPlan plan(String query) {
        return plan(query, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public PhysicalPlan plan(String query, SearchStats stats) {
        return plan(query, stats, analyzer);
    }

    public PhysicalPlan plan(String query, SearchStats stats, Analyzer analyzer) {
        var physical = optimizedPlan(physicalPlan(query, analyzer), stats);
        return physical;
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        PlainActionFuture<PhysicalPlan> result = new PlainActionFuture<>();

        physicalPlanOptimizer.optimize(plan, ActionListener.wrap(physicalPlan -> {
            physicalPlan = EstimatesRowSize.estimateRowSize(0, physicalPlan);
            // System.out.println("* Physical After\n" + p);
            // the real execution breaks the plan at the exchange and then decouples the plan
            // this is of no use in the unit tests, which checks the plan as a whole instead of each
            // individually hence why here the plan is kept as is

            var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
                new LocalLogicalOptimizerContext(config, FoldContext.small(), searchStats)
            );

            var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
                new LocalPhysicalOptimizerContext(config, FoldContext.small(), searchStats),
                true
            );

            PlannerUtils.localPlan(physicalPlan, logicalTestOptimizer, physicalTestOptimizer, ActionListener.wrap(l -> {
                // handle local reduction alignment
                var alignedL = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);
                result.onResponse(alignedL);
            }, result::onFailure));
        }, result::onFailure));

        // System.out.println("* Localized DataNode Plan\n" + l);
        return result.actionGet();
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        var logical = optimizeLogical(analyze(analyzer, parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        var physical = mapper.map(logical);
        return physical;
    }

    private LogicalPlan optimizeLogical(LogicalPlan logicalPlan) {
        PlainActionFuture<LogicalPlan> optimizedPlanFuture = new PlainActionFuture<>();
        logicalOptimizer.optimize(logicalPlan, optimizedPlanFuture);
        return optimizedPlanFuture.actionGet();
    }
}
