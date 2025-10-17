/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PLANNER_SETTINGS;

public class TestPlannerOptimizer {
    private final EsqlParser parser;
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

        parser = new EsqlParser();
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
        var physical = optimizedPlan(physicalPlan(query, analyzer), stats);
        return physical;
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
            new LocalPhysicalOptimizerContext(TEST_PLANNER_SETTINGS, esqlFlags, config, FoldContext.small(), searchStats),
            true
        );
        var l = PlannerUtils.localPlan(physicalPlan, logicalTestOptimizer, physicalTestOptimizer);

        // handle local reduction alignment
        l = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        // System.out.println("Logical\n" + logical);
        return mapper.map(new Versioned<>(logical, analyzer.context().minimumVersion()));
    }

    public PhysicalPlan dataNodePlan(String query, SearchStats searchStats, EsqlFlags esqlFlags) {
        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query)));
        PhysicalPlan physical = mapper.map(new Versioned<>(logical, TransportVersion.current()));
        var dataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physical, config).v2();
        var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(config, FoldContext.small(), searchStats)
        );
        var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(TEST_PLANNER_SETTINGS, esqlFlags, config, FoldContext.small(), searchStats),
            true
        );
        return PlannerUtils.localPlan(dataNodePlan, logicalTestOptimizer, physicalTestOptimizer);
    }

    public TestPlannerOptimizer withAnalyzer(Analyzer newAnalyzer) {
        return new TestPlannerOptimizer(config, newAnalyzer, logicalOptimizer);
    }

    public TestPlannerOptimizer withTaskConcurrency(int taskConcurrency) {
        return withQueryPragma(Settings.builder().put(QueryPragmas.TASK_CONCURRENCY.getKey(), taskConcurrency).build());
    }

    public TestPlannerOptimizer withQueryPragma(Settings pragmas) {
        Settings pragmaSettings = Settings.builder().put(config.pragmas().getSettings()).put(pragmas).build();
        Configuration newConfig = new Configuration(
            config.zoneId(),
            config.locale(),
            config.username(),
            config.clusterName(),
            new QueryPragmas(pragmaSettings),
            config.resultTruncationMaxSize(false),
            config.resultTruncationDefaultSize(false),
            config.query(),
            config.profile(),
            config.tables(),
            config.getQueryStartTimeNanos(),
            config.allowPartialResults(),
            config.resultTruncationMaxSize(true),
            config.resultTruncationDefaultSize(true)
        );
        return new TestPlannerOptimizer(newConfig, analyzer, logicalOptimizer);
    }
}
