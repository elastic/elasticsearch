/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

public class TestPlannerOptimizer {
    private final Analyzer analyzer;
    private final LogicalPlanOptimizer logicalOptimizer;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final Mapper mapper;
    private final Configuration config;

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer) {
        this(
            config,
            analyzer,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), analyzer.context().minimumVersion()))
        );
    }

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer, LogicalPlanOptimizer logicalOptimizer) {
        this.analyzer = analyzer;
        this.config = config;
        this.logicalOptimizer = logicalOptimizer;

        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config, analyzer.context().minimumVersion()));
        mapper = new Mapper();

    }

    public PhysicalPlan plan(String query) {
        return plan(query, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public PhysicalPlan plan(String query, SearchStats stats) {
        return plan(query, stats, analyzer);
    }

    public PhysicalPlan plan(String query, SearchStats stats, Analyzer analyzer) {
        return plan(query, stats, analyzer, null);
    }

    public PhysicalPlan plan(String query, SearchStats stats, Analyzer analyzer, @Nullable QueryBuilder esFilter) {
        PhysicalPlan plan = PlannerUtils.integrateEsFilterIntoFragment(physicalPlan(query, analyzer), esFilter);
        return optimizedPlan(plan, stats);
    }

    public PhysicalPlan plan(String query, SearchStats stats, EsqlFlags esqlFlags) {
        return optimizedPlan(physicalPlan(query, analyzer), stats, esqlFlags);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        return optimizedPlan(plan, searchStats, new EsqlFlags(true));
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats, EsqlFlags esqlFlags) {
        // System.out.println("* Physical Before\n" + plan);
        var physicalPlan = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(plan));
        // System.out.println("* Physical After\n" + physicalPlan);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is

        var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(config, FoldContext.small(), searchStats)
        );
        var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, esqlFlags, config, FoldContext.small(), searchStats),
            true
        );
        var l = PlannerUtils.localPlan(physicalPlan, logicalTestOptimizer, physicalTestOptimizer, null);

        // handle local reduction alignment
        l = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query)));
        // System.out.println("Logical\n" + logical);
        return mapper.map(new Versioned<>(logical, analyzer.context().minimumVersion()));
    }
}
