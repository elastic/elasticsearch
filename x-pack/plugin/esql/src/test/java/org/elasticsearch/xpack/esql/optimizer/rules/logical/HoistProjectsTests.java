/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.RuleExecutionException;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class HoistProjectsTests extends AbstractLogicalPlanOptimizerTests {

    public HoistProjectsTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testOneRunHoistsTheProjectAboveEveryLookupBlock() {
        LogicalPlan plan = new HoistProjects().apply(substituted(lookupBlocks(3)));

        var project = as(plan, Project.class);
        as(project.child(), Limit.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        List<Join> joins = plan.collect(Join.class);
        assertThat(joins, hasSize(3));
        assertThat(joins.stream().map(Join::left).toList(), everyItem(not(instanceOf(Project.class))));
        assertThat(Expressions.names(project.projections()), hasItems("lang0", "lang1", "lang2"));
    }

    public void testOneRunHoistsTheProjectPastEvalsBetweenLookupBlocks() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS lang
            | EVAL upper = TO_UPPER(lang)
            | EVAL language_code = languages + 1
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS next_lang
            """));

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        Join upperJoin = plan.collect(Join.class).getFirst();
        var eval = as(upperJoin.left(), Eval.class);
        assertThat(eval.collect(Project.class), hasSize(0));
    }

    public void testProjectIsNotHoistedPastInlineJoin() {
        LogicalPlan plan = new HoistProjects().apply(substituted("""
            FROM test
            | RENAME languages AS language_code
            | INLINE STATS max_salary = MAX(salary) BY language_code
            """));

        var inlineJoin = plan.collect(InlineJoin.class).getFirst();
        as(inlineJoin.left(), Project.class);
    }

    public void testOperatorBatchRunsDoNotGrowWithTheNumberOfLookupBlocks() {
        assertThat(operatorRunsToConverge(lookupBlocks(30)), equalTo(operatorRunsToConverge(lookupBlocks(3))));
    }

    public void testOperatorBatchRunsDoNotGrowWithFiltersOnLookedUpColumns() {
        String block = "\n| WHERE lang%d IS NOT NULL";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithDissectBetweenLookupBlocks() {
        String block = "\n| DISSECT first_name \"%%{part%d}\"";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithUriPartsBetweenLookupBlocks() {
        assumeTrue("requires uri_parts command capability", EsqlCapabilities.Cap.URI_PARTS_COMMAND.isEnabled());
        String block = "\n| URI_PARTS uri%d = first_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testOperatorBatchRunsDoNotGrowWithEnrichBetweenLookupBlocks() {
        String block = "\n| ENRICH languages_idx ON languages WITH enriched%d = language_name";
        assertThat(operatorRunsToConverge(lookupBlocks(30, block)), equalTo(operatorRunsToConverge(lookupBlocks(3, block))));
    }

    public void testLookupChainLongerThanTheRuleExecutionLimitOptimizes() {
        int blocks = 110;
        String query = lookupBlocks(blocks);
        LogicalPlan analyzed = defaultAnalyzer().query(query);

        LogicalPlan plan = logicalOptimizer.optimize(analyzed);

        as(plan, Project.class);
        assertThat(plan.collect(Project.class), hasSize(1));
        assertThat(plan.collect(Join.class), hasSize(blocks));
        assertThat(Expressions.names(plan.output()), equalTo(Expressions.names(analyzed.output())));
    }

    private static String lookupBlocks(int count) {
        return lookupBlocks(count, "");
    }

    private static String lookupBlocks(int count, String extraCommandFormat) {
        StringBuilder query = new StringBuilder("FROM test");
        for (int i = 0; i < count; i++) {
            query.append("\n| EVAL language_code = languages + ")
                .append(i)
                .append("\n| LOOKUP JOIN languages_lookup ON language_code\n| RENAME language_name AS lang")
                .append(i)
                .append(String.format(Locale.ROOT, extraCommandFormat, i))
                .append("\n| DROP language_code");
        }
        return query.toString();
    }

    private LogicalPlan substituted(String query) {
        return new TestSubstitutionOnlyOptimizer(logicalOptimizerCtx).optimize(defaultAnalyzer().query(query));
    }

    private int operatorRunsToConverge(String query) {
        LogicalPlan analyzed = defaultAnalyzer().query(query);
        for (int runs = 1; runs <= 100; runs++) {
            try {
                new OperatorRunsLimitedOptimizer(logicalOptimizerCtx, runs).optimize(analyzed);
                return runs;
            } catch (RuleExecutionException e) {
                assertThat(e.getMessage(), containsString("Rule execution limit [" + runs + "] reached"));
            }
        }
        throw new AssertionError("operator batch did not converge in 100 runs");
    }

    private static final class OperatorRunsLimitedOptimizer extends LogicalPlanOptimizer {
        private final int maxRuns;

        OperatorRunsLimitedOptimizer(LogicalOptimizerContext context, int maxRuns) {
            super(context);
            this.maxRuns = maxRuns;
        }

        @Override
        protected List<RuleExecutor.Batch<LogicalPlan>> batches() {
            String operatorsName = operators().name();
            List<RuleExecutor.Batch<LogicalPlan>> batches = new ArrayList<>();
            for (RuleExecutor.Batch<LogicalPlan> batch : super.batches()) {
                batches.add(
                    batch.name().equals(operatorsName)
                        ? new RuleExecutor.Batch<>(batch.name(), new RuleExecutor.Limiter(maxRuns), batch.rules())
                        : batch
                );
            }
            return batches;
        }
    }
}
