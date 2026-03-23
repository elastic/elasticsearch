/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.hamcrest.Matcher;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertPlanError;
import static org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize.estimateRowSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Helper for building optimized coordinating node and local plans.
 */
public class TestPlans {
    private static final Logger log = LogManager.getLogger(TestPlans.class);

    private final List<Rule<LogicalPlan, LogicalPlan>> preanalyzers;
    private final Analyzer analyzer;
    private String query;
    private SearchStats searchStats = EsqlTestUtils.TEST_SEARCH_STATS;
    private boolean stringLikeOnIndex = EsqlFlags.ESQL_STRING_LIKE_ON_INDEX.getDefault(Settings.EMPTY);
    private int roundToPushdownThreshold = EsqlFlags.ESQL_ROUNDTO_PUSHDOWN_THRESHOLD.getDefault(Settings.EMPTY);
    @Nullable
    private QueryBuilder esFilter;
    private Function<LogicalOptimizerContext, LogicalPlanOptimizer> localPlanOptimizerBuilder = LogicalPlanOptimizer::new;

    private LogicalPlan parsed;
    private LogicalPlan coordinatorLogicalUnoptimized;
    private LogicalPlan coordinatorLogicalOptimized;
    private PhysicalPlan coordinatorPhysicalPlanUnoptimized;
    private PhysicalPlan coordinatorPhysicalPlanOptimized;
    private PhysicalPlan dataNodePlanOptimized;

    TestPlans(List<Rule<LogicalPlan, LogicalPlan>> preanalyzers, Analyzer analyzer, String query) {
        this.preanalyzers = preanalyzers;
        this.analyzer = analyzer;
        this.query = query;
    }

    /**
     * In the query replace {@code $now-1h} and {@code $now} with "an hour ago"
     * and "now".
     */
    public TestPlans replaceNow() {
        var now = Instant.now();
        query = query.replace("$now-1h", "\"" + now.minus(1, ChronoUnit.HOURS) + "\"");
        query = query.replace("$now", "\"" + now + "\"");
        coordinatorLogicalUnoptimized = null;
        coordinatorLogicalOptimized = null;
        coordinatorPhysicalPlanUnoptimized = null;
        coordinatorPhysicalPlanOptimized = null;
        dataNodePlanOptimized = null;
        return this;
    }

    public TestPlans searchStats(SearchStats searchStats) {
        this.searchStats = searchStats;
        dataNodePlanOptimized = null;
        return this;
    }

    public TestPlans roundToPushdownThreshold(int roundToPushdownThreshold) {
        this.roundToPushdownThreshold = roundToPushdownThreshold;
        dataNodePlanOptimized = null;
        return this;
    }

    /**
     * Add a filter.
     */
    public TestPlans esFilter(QueryBuilder esFilter) {
        this.esFilter = esFilter;
        coordinatorPhysicalPlanOptimized = null;
        dataNodePlanOptimized = null;
        return this;
    }

    /**
     * Replace the default {@link LogicalPlanOptimizer}.
     */
    public TestPlans localPlanOptimizerBuilder(Function<LogicalOptimizerContext, LogicalPlanOptimizer> localPlanOptimizerBuilder) {
        this.localPlanOptimizerBuilder = localPlanOptimizerBuilder;
        return this;
    }

    public LogicalPlan parsed() {
        if (parsed == null) {
            parsed = TEST_PARSER.parseQuery(query, new QueryParams());
        }
        return parsed;
    }

    /**
     * Parse the query and analyze.
     */
    public LogicalPlan coordinatorLogicalUnoptimized() {
        if (coordinatorLogicalUnoptimized == null) {
            LogicalPlan logicalPlan = parsed();
            for (Rule<LogicalPlan, LogicalPlan> preanalyzer : preanalyzers) {
                logicalPlan = preanalyzer.apply(logicalPlan);
            }
            coordinatorLogicalUnoptimized = analyzer.analyze(logicalPlan);
            log.trace("coordinator logical unoptimized:\n{}", coordinatorLogicalUnoptimized);
        }
        return coordinatorLogicalUnoptimized;
    }

    /**
     * Assert references from the {@link #coordinatorLogicalUnoptimized()} plan.
     */
    public TestPlans assertReferences(Matcher<Collection<? extends Attribute>> matcher) {
        AttributeSet.Builder references = AttributeSet.builder();
        coordinatorLogicalUnoptimized().forEachDown(lp -> references.addAll(lp.references()));
        assertThat(references.build(), matcher);
        return this;
    }

    /**
     * Assert that {@link #coordinatorLogicalUnoptimized()} references <strong>nothing</strong>.
     */
    public TestPlans assertNoReferences() {
        return assertReferences(empty());
    }

    /**
     * Assert that {@link #coordinatorLogicalUnoptimized()} references <strong>something</strong>.
     */
    public TestPlans assertSomeReferences() {
        return assertReferences(not(empty()));
    }

    public LogicalPlan coordinatorLogicalOptimized() {
        if (coordinatorLogicalOptimized == null) {
            LogicalPlanOptimizer optimizer = localPlanOptimizerBuilder.apply(logicalOptimizerContext());
            coordinatorLogicalOptimized = optimizer.optimize(coordinatorLogicalUnoptimized());
            log.trace("coordinator logical optimized:\n{}", coordinatorLogicalOptimized);
        }
        return coordinatorLogicalOptimized;
    }

    public String coordinatorLogicalPlanOptimizationError(Matcher<String> messageMatcher) {
        LogicalPlanOptimizer optimizer = localPlanOptimizerBuilder.apply(logicalOptimizerContext());
        return assertPlanError(
            true,
            query,
            VerificationException.class,
            messageMatcher,
            () -> optimizer.optimize(coordinatorLogicalUnoptimized())
        );
    }

    public LogicalOptimizerContext logicalOptimizerContext() {
        return new LogicalOptimizerContext(analyzer.context().configuration(), FoldContext.small(), analyzer.context().minimumVersion());
    }

    /**
     * The {@link LogicalPlan#output()} of the first {@link LeafPlan} in {@link #coordinatorLogicalOptimized()}.
     */
    public List<Attribute> coordinatorLogicalOptimizedLeafOutput() {
        return coordinatorLogicalOptimized().collect(LeafPlan.class).getFirst().output();
    }

    public PhysicalPlan coordinatorPhysicalPlanUnoptimized() {
        if (coordinatorPhysicalPlanUnoptimized == null) {
            coordinatorPhysicalPlanUnoptimized = new Mapper().map(
                new Versioned<>(coordinatorLogicalOptimized(), analyzer.context().minimumVersion())
            );
            log.trace("coordinator physical unoptimized:\n{}", coordinatorPhysicalPlanUnoptimized);
        }
        return coordinatorPhysicalPlanUnoptimized;
    }

    public PhysicalPlan coordinatorPhysicalPlanOptimized() {
        if (coordinatorPhysicalPlanOptimized == null) {
            PhysicalPlan plan = PlannerUtils.integrateEsFilterIntoFragment(coordinatorPhysicalPlanUnoptimized(), esFilter);
            PhysicalPlanOptimizer optimizer = new PhysicalPlanOptimizer(
                new PhysicalOptimizerContext(analyzer.context().configuration(), analyzer.context().minimumVersion())
            );
            coordinatorPhysicalPlanOptimized = estimateRowSize(0, optimizer.optimize(plan));
            log.trace("coordinator physical optimized:\n{}", coordinatorPhysicalPlanUnoptimized);
        }
        return coordinatorPhysicalPlanOptimized;
    }

    public PhysicalPlan dataNodePlanOptimized() {
        if (dataNodePlanOptimized == null) {
            var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
                new LocalLogicalOptimizerContext(analyzer.context().configuration(), FoldContext.small(), searchStats)
            );
            var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
                new LocalPhysicalOptimizerContext(
                    PlannerSettings.DEFAULTS,
                    new EsqlFlags(stringLikeOnIndex, roundToPushdownThreshold),
                    analyzer.context().configuration(),
                    FoldContext.small(),
                    searchStats
                ),
                true
            );
            PhysicalPlan plan = PlannerUtils.localPlan(
                coordinatorPhysicalPlanOptimized(),
                logicalTestOptimizer,
                physicalTestOptimizer,
                null
            );
            dataNodePlanOptimized = localRelationshipAlignment(plan);
            log.trace("data node physical optimized:\n{}", coordinatorPhysicalPlanUnoptimized);
        }
        return dataNodePlanOptimized;
    }

    static PhysicalPlan localRelationshipAlignment(PhysicalPlan l) {
        // handle local reduction alignment
        return l.transformUp(ExchangeExec.class, exg -> {
            PhysicalPlan pl = exg;
            if (exg.inBetweenAggs() && exg.child() instanceof LocalSourceExec lse) {
                var output = exg.output();
                if (lse.output().equals(output) == false) {
                    pl = exg.replaceChild(new LocalSourceExec(lse.source(), output, lse.supplier()));
                }
            }
            return pl;
        });
    }
}
