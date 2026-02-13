/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.containsIgnoringIds;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests.releaseBuildForInlineStats;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.DeduplicateAggsTests.aggFieldName;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.DeduplicateAggsTests.aliased;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PruneColumnsTests extends AbstractLogicalPlanOptimizerTests {

    public void testPruneUnusedEval() {
        var plan = plan("""
              from test
            | eval garbage = salary + 3
            | keep salary
            """);

        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testPruneChainedEval() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3
            | eval garbage_b = emp_no / garbage_a, garbage_c = garbage_a
            | eval garbage_x = 1 - garbage_b/garbage_c
            | keep salary
            """);
        var keep = as(plan, Project.class);
        var limit = as(keep.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    public void testPruneChainedEvalNoProjection() {
        var plan = plan("""
              from test
            | eval garbage = salary + 3
            | eval garbage = emp_no / garbage, garbage = garbage
            | eval garbage = 1
            """);
        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);

        assertEquals(1, eval.fields().size());
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertEquals(alias.name(), "garbage");
        var literal = as(alias.child(), Literal.class);
        assertEquals(1, literal.value());
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#1345) AS c]]
     *   \_EsRelation[test][_meta_field{f}#1346, emp_no{f}#1340, first_name{f}#..]
     * }</pre>
     */
    public void testPruneEvalDueToStats() {
        var plan = plan("""
              from test
            | eval garbage_a = salary + 3, x = salary
            | eval garbage_b = x + 3
            | stats c = count(x)
            """);

        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var aggs = aggregate.aggregates();
        assertThat(Expressions.names(aggs), contains("c"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        var source = as(aggregate.child(), EsRelation.class);
    }

    public void testPruneUnusedAggSimple() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | keep c
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        assertThat(agg.aggregates(), hasSize(1));
        var aggOne = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggOne.name(), is("c"));
        var count = as(aggOne.child(), Count.class);
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#19) AS x]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }</pre>
     */
    public void testPruneUnusedAggMixedWithEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = c
            | keep x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("x"));
        aggFieldName(agg.aggregates().get(0), Count.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    public void testPruneUnusedAggsChainedAgg() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(aggs, hasSize(1));
        assertThat(Expressions.names(aggs), contains("c"));
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[c{r}#342]]
     * \_Limit[1000[INTEGER]]
     *   \_Filter[min{r}#348 > 10[INTEGER]]
     *     \_Aggregate[[],[COUNT(salary{f}#367) AS c, MIN(salary{f}#367) AS min]]
     *       \_EsRelation[test][_meta_field{f}#368, emp_no{f}#362, first_name{f}#36..]
     * }</pre>
     */
    public void testPruneMixedAggInsideUnusedEval() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | where y > 10
            | eval z = c
            | keep c
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var agg = as(filter.child(), Aggregate.class);
        assertThat(agg.groupings(), hasSize(0));
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c", "min"));
        aggFieldName(aggs.get(0), Count.class, "salary");
        aggFieldName(aggs.get(1), Min.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Eval[[max{r}#6 + min{r}#9 + c{r}#3 AS x, min{r}#9 AS y, c{r}#3 AS z]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[COUNT(salary{f}#26) AS c, MAX(salary{f}#26) AS max, MIN(salary{f}#26) AS min]]
     *     \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     * }</pre>
     */
    public void testNoPruningWhenDealingJustWithEvals() {
        var plan = plan("""
              from test
            | stats c = count(salary), max = max(salary), min = min(salary)
            | eval x = max + min + c
            | eval y = min
            | eval z = c
            """);

        var eval = as(plan, Eval.class);
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[y{r}#6 AS z]]
     * \_Eval[[emp_no{f}#11 + 1[INTEGER] AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    public void testNoPruningWhenChainedEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, y = x + 1, z = y
            | keep z
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("z"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("y"));
        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[salary{f}#20 AS x, emp_no{f}#15 AS y]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }</pre>
     */
    public void testPruningDuplicateEvals() {
        var plan = plan("""
              from test
            | eval x = emp_no, x = salary
            | eval y = salary
            | eval y = emp_no
            | keep x, y
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("x", "y"));
        var child = aliased(projections.get(0), FieldAttribute.class);
        assertThat(child.name(), is("salary"));
        child = aliased(projections.get(1), FieldAttribute.class);
        assertThat(child.name(), is("emp_no"));

        var limit = as(project.child(), Limit.class);
        var source = as(limit.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#12 AS x#8, emp_no{f}#12]]
     * \_TopN[[Order[emp_no{f}#12,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     */
    public void testInlinestatsGetsPrunedEntirely() {
        var query = """
            FROM employees
            | INLINE STATS x = avg(salary) BY emp_no
            | EVAL x = emp_no
            | SORT x
            | KEEP x, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "emp_no")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var relation = as(topN.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#16, count{r}#7]]
     * \_TopN[[Order[emp_no{f}#16,ASC,LAST]],5[INTEGER]]
     *   \_InlineJoin[LEFT,[salaryK{r}#5],[salaryK{r}#5],[salaryK{r}#5]]
     *     |_Eval[[salary{f}#21 / 1000[INTEGER] AS salaryK#5]]
     *     | \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_Aggregate[[salaryK{r}#5],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count#7, salaryK{r}#5]]
     *       \_StubRelation[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *              languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21, salaryK{r}#5]]
     */
    public void testDoubleInlineStatsWithEvalGetsPrunedEntirely() {
        var query = """
            FROM employees
            | SORT languages DESC
            | EVAL salaryK = salary/1000
            | INLINE STATS count = COUNT(*) BY salaryK
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | KEEP emp_no, count
            | SORT emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "count")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        var ref = as(order.child(), FieldAttribute.class);
        assertThat(ref.name(), is("emp_no"));
        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("salaryK")));
        // Left
        var eval = as(inlineJoin.left(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("salaryK")));
        var relation = as(eval.child(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("salaryK")));
        assertThat(Expressions.names(agg.aggregates()), is(List.of("count", "salaryK")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#19 AS x#15, emp_no{f}#19]]
     * \_TopN[[Order[emp_no{f}#19,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     */
    public void testDoubleInlineStatsGetsPrunedEntirely() {
        var query = """
            FROM employees
            | INLINE STATS x = avg(salary) BY emp_no
            | INLINE STATS y = avg(salary) BY languages
            | EVAL y = emp_no
            | EVAL x = y
            | SORT x
            | KEEP x, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "emp_no")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var relation = as(topN.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#15 AS x#11, a{r}#7, emp_no{f}#15]]
     * \_Limit[1[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#15],[emp_no{f}#15],[emp_no{r}#15]]
     *     |_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *     \_Aggregate[[emp_no{f}#15],[COUNTDISTINCT(languages{f}#18,true[BOOLEAN]) AS a#7, emp_no{f}#15]]
     *       \_StubRelation[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, l
     *          anguages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     */
    public void testInlineStatsGetsPrunedPartially() {
        var query = """
            FROM employees
            | INLINE STATS x = AVG(salary), a = COUNT_DISTINCT(languages) BY emp_no
            | EVAL x = emp_no
            | KEEP x, a, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "a", "emp_no")));
        var upperLimit = asLimit(project.child(), 1, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("a", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    // same as above
    public void testTripleInlineStatsGetsPrunedPartially() {
        var query = """
            FROM employees
            | INLINE STATS x = AVG(salary), a = COUNT_DISTINCT(languages) BY emp_no
            | INLINE STATS y = AVG(salary), b = COUNT_DISTINCT(languages) BY emp_no
            | EVAL x = emp_no
            | INLINE STATS z = AVG(salary), c = COUNT_DISTINCT(languages), d = AVG(languages) BY last_name
            | KEEP x, a, emp_no
            | LIMIT 1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("x", "a", "emp_no")));
        var upperLimit = asLimit(project.child(), 1, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("a", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#26, salaryK{r}#4, count{r}#6, min{r}#19]]
     * \_TopN[[Order[emp_no{f}#26,ASC,LAST]],5[INTEGER]]
     *   \_InlineJoin[LEFT,[salaryK{r}#4],[salaryK{r}#4]]
     *     |_Project[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34, job.raw{f}#35,
     *              languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, count{r}#6, salaryK{r}#4, hire_date_string{r}#10,
     *              date{r}#15]]
     *     | \_Dissect[hire_date_string{r}#10,Parser[pattern=%{date}, appendSeparator=,
     *            parser=org.elasticsearch.dissect.DissectParser@77d1afc3],[date{r}#15]] <-- TODO: Dissect & Eval could/should be dropped
     *     |   \_Eval[[TOSTRING(hire_date{f}#33) AS hire_date_string#10]]
     *     |     \_InlineJoin[LEFT,[salaryK{r}#4],[salaryK{r}#4]]
     *     |       |_Eval[[salary{f}#31 / 10000[INTEGER] AS salaryK#4]]
     *     |       | \_EsRelation[test][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     *     |       \_Aggregate[[salaryK{r}#4],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count#6, salaryK{r}#4]]
     *     |         \_StubRelation[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34,
     *                      job.raw{f}#35, languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, salaryK{r}#4]]
     *     \_Aggregate[[salaryK{r}#4],[MIN($$MV_COUNT(langua>$MIN$0{r$}#37,true[BOOLEAN]) AS min#19, salaryK{r}#4]]
     *       \_Eval[[MVCOUNT(languages{f}#29) AS $$MV_COUNT(langua>$MIN$0#37]]
     *         \_StubRelation[[_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, gender{f}#28, hire_date{f}#33, job{f}#34, job.raw{f}#35,
     *              languages{f}#29, last_name{f}#30, long_noidx{f}#36, salary{f}#31, count{r}#6, salaryK{r}#4, sum{r}#13,
     *              hire_date_string{r}#10, date{r}#15, $$MV_COUNT(langua>$MIN$0{r$}#37]]
     */
    public void testTripleInlineStatsMultipleAssignmentsGetsPrunedPartially() {
        // TODO: reenable 1st sort, pull the 2nd further up when #132417 is in
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | DISSECT hire_date_string "%{date}"
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no, salaryK, count, min
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var employeesFields = List.of(
            "_meta_field",
            "emp_no",
            "first_name",
            "gender",
            "hire_date",
            "job",
            "job.raw",
            "languages",
            "last_name",
            "long_noidx",
            "salary"
        );

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "salaryK", "count", "min")));
        var topN = as(project.child(), TopN.class);
        var outerinline = as(topN.child(), InlineJoin.class);
        //
        var expectedOutterOutput = List.of("emp_no", "languages", "count", "min", "salaryK");
        assertThat(Expressions.names(outerinline.output()), is(expectedOutterOutput));
        // outer left
        var outerProject = as(outerinline.left(), Project.class);
        var dissect = as(outerProject.child(), Dissect.class);
        var eval = as(dissect.child(), Eval.class);
        var innerinline = as(eval.child(), InlineJoin.class);
        var expectedInnerOutput = new ArrayList<>(employeesFields);
        expectedInnerOutput.addAll(List.of("count", "salaryK"));
        assertThat(Expressions.names(innerinline.output()), is(expectedInnerOutput));
        // inner left
        eval = as(innerinline.left(), Eval.class);
        var relation = as(eval.child(), EsRelation.class);
        // inner right
        var agg = as(innerinline.right(), Aggregate.class);
        var stub = as(agg.child(), StubRelation.class);
        // outer right
        agg = as(outerinline.right(), Aggregate.class);
        eval = as(agg.child(), Eval.class);
        stub = as(eval.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#917]]
     * \_TopN[[Order[emp_no{f}#917,ASC,LAST]],5[INTEGER]]
     *   \_Dissect[hire_date_string{r}#898,Parser[pattern=%{date}, appendSeparator=,
     *          parser=org.elasticsearch.dissect.DissectParser@46132aa7],[date{r}#903]] <-- TODO: Dissect & Eval could/should be dropped
     *     \_Eval[[TOSTRING(hire_date{f}#918) AS hire_date_string#898]]
     *       \_EsRelation[employees][emp_no{f}#917, hire_date{f}#918, languages{f}#913, ..]
     */
    public void testTripleInlineStatsMultipleAssignmentsGetsPrunedEntirely() {
        // same as the above query, but only keep emp_no
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | DISSECT hire_date_string "%{date}"
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));
        var topN = as(project.child(), TopN.class);
        var dissect = as(topN.child(), Dissect.class);
        var eval = as(dissect.child(), Eval.class);
        var relation = as(eval.child(), EsRelation.class);
    }

    /*
     * Project[[emp_no{f}#1556]]
     * \_TopN[[Order[emp_no{f}#1556,ASC,LAST]],5[INTEGER]]
     *   \_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1561]]
     *     |_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1560]]
     *     | |_Join[LEFT,[languages{f}#1552],[languages{f}#1552],[language_code{f}#1559]]
     *     | | |_EsRelation[employees][emp_no{f}#1556, hire_date{f}#1557, languages{f}#155..]
     *     | | \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1559]
     *     | \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1560]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#1561]
     */
    public void testTripleInlineStatsWithLookupJoinGetsPrunedEntirely() {
        var query = """
            FROM employees
            // | SORT languages DESC
            | EVAL salaryK = salary / 10000
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS count = COUNT(*) BY salaryK
            | EVAL hire_date_string = hire_date::keyword
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS sum = SUM(languages) BY hire_date_string
            | LOOKUP JOIN languages_lookup ON language_code
            | INLINE STATS min = MIN(MV_COUNT(languages)) BY salaryK
            | SORT emp_no
            | KEEP emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));
        var topN = as(project.child(), TopN.class);

        var outterjoin = as(topN.child(), Join.class);
        var middlejoin = as(outterjoin.left(), Join.class);
        var innerjoin = as(middlejoin.left(), Join.class);

        var innerJoinLeftRelation = as(innerjoin.left(), EsRelation.class);
        var innerJoinRightRelation = as(innerjoin.right(), EsRelation.class);

        var middleJoinRightRelation = as(middlejoin.right(), EsRelation.class);
        var outerJoinRightRelation = as(outterjoin.right(), EsRelation.class);
    }

    /*
     * Project[[avg{r}#14, decades{r}#10]]
     * \_Eval[[$$SUM$avg$0{r$}#35 / $$COUNT$avg$1{r$}#36 AS avg#14]]
     *   \_Limit[1000[INTEGER],false]
     *     \_Aggregate[[decades{r}#10],[SUM(salary{f}#29,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$avg$0#35, COUNT(salary{f}#29,
     *              true[BOOLEAN]) AS $$COUNT$avg$1#36, decades{r}#10]]
     *       \_Eval[[DATEDIFF(years[KEYWORD],hire_date{f}#31,1755625790494[DATETIME]) AS age#4, age{r}#4 / 10[INTEGER] AS decades#7,
     *                   decades{r}#7 * 10[INTEGER] AS decades#10]]
     *         \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     */
    public void testInlineStatsWithAggGetsPrunedEntirely() {
        var query = """
            FROM employees
            | EVAL age = DATE_DIFF("years", hire_date, NOW())
            | EVAL decades = age / 10, decades = decades * 10
            | STATS avg = AVG(salary) BY decades
            | EVAL idecades = decades / 2
            | INLINE STATS iavg = AVG(avg) BY idecades
            | KEEP avg, decades
            """;

        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "decades")));
        var eval = as(project.child(), Eval.class);
        var limit = asLimit(eval.child(), 1000, false);
        var aggregate = as(limit.child(), Aggregate.class);
        eval = as(aggregate.child(), Eval.class);
        var source = as(eval.child(), EsRelation.class);
    }

    /*
     * Project[[avg{r}#15, decades{r}#11, avgavg{r}#25]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[],[]]
     *     |_Project[[avg{r}#15, decades{r}#11]]
     *     | \_Eval[[$$SUM$avg$0{r$}#40 / $$COUNT$avg$1{r$}#41 AS avg#15]]
     *     |   \_Aggregate[[decades{r}#11],[SUM(salary{f}#34,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#40,
     * COUNT(salary{f}#34,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#41, decades{r}#11]]
     *     |     \_Eval[[DATEDIFF(years[KEYWORD],hire_date{f}#36,1768997427103[DATETIME]) AS age#5, age{r}#5 / 10[INTEGER] AS decades#8,
     * decades{r}#8 * 10[INTEGER] AS decades#11]]
     *     |       \_EsRelation[employees][_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, ..]
     *     \_Project[[avgavg{r}#25]]
     *       \_Eval[[$$SUM$avgavg$0{r$}#44 / $$COUNT$avgavg$1{r$}#45 AS avgavg#25]]
     *         \_Aggregate[[],[SUM(avg{r}#15,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avgavg$0#44, COUNT(avg{r}#15,
     * true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avgavg$1#45]]
     *           \_StubRelation[[avg{r}#15, decades{r}#11, iavg{r}#21, idecades{r}#18]]
     */
    public void testInlineStatsWithAggAndInlineStatsGetsPruned() {
        var query = """
            FROM employees
            | EVAL age = DATE_DIFF("years", hire_date, NOW())
            | EVAL decades = age / 10, decades = decades * 10
            | STATS avg = AVG(salary) BY decades
            | EVAL idecades = decades / 2
            | INLINE STATS iavg = AVG(avg) BY idecades
            | INLINE STATS avgavg = AVG(avg)
            | KEEP avg, decades, avgavg
            """;

        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "decades", "avgavg")));
        var limit = asLimit(project.child(), 1000, false);
        var inlineJoin = as(limit.child(), InlineJoin.class);

        // Left branch: Project with avg, decades
        var leftProject = as(inlineJoin.left(), Project.class);
        assertThat(Expressions.names(leftProject.projections()), is(List.of("avg", "decades")));
        var leftEval = as(leftProject.child(), Eval.class);
        var leftAggregate = as(leftEval.child(), Aggregate.class);
        assertThat(Expressions.names(leftAggregate.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "decades")));
        var leftEval2 = as(leftAggregate.child(), Eval.class);
        var leftRelation = as(leftEval2.child(), EsRelation.class);

        // Right branch: Project with avgavg
        var rightProject = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(rightProject.projections()), is(List.of("avgavg")));
        var rightEval = as(rightProject.child(), Eval.class);
        var rightAggregate = as(rightEval.child(), Aggregate.class);
        assertThat(Expressions.names(rightAggregate.output()), is(List.of("$$SUM$avgavg$0", "$$COUNT$avgavg$1")));
        assertThat(rightAggregate.groupings(), empty());
        var rightStub = as(rightAggregate.child(), StubRelation.class);
    }

    /*
     * Project[[abbrev{f}#19, scalerank{f}#21 AS backup_scalerank#4, language_name{f}#28 AS scalerank#11]]
     * \_TopN[[Order[abbrev{f}#19,DESC,FIRST]],5[INTEGER]]
     *   \_Join[LEFT,[scalerank{f}#21],[scalerank{f}#21],[language_code{f}#27]]
     *     |_EsRelation[airports][abbrev{f}#19, city{f}#25, city_location{f}#26, coun..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#27, language_name{f}#28]
     */
    public void testInlineStatsWithLookupJoin() {
        var query = """
            FROM airports
            | EVAL backup_scalerank = scalerank
            | RENAME scalerank AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name as scalerank
            | DROP language_code
            | INLINE STATS count=COUNT(*) BY scalerank
            | SORT abbrev DESC
            | KEEP abbrev, *scalerank
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }

        var plan = planAirports(query);
        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("abbrev", "backup_scalerank", "scalerank")));
        var topN = as(project.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), equalTo("abbrev"));
        var join = as(topN.child(), Join.class);
        assertThat(Expressions.names(join.config().leftFields()), is(List.of("scalerank")));
        var left = as(join.left(), EsRelation.class);
        assertThat(left.concreteQualifiedIndices(), is(Set.of("airports")));
        var right = as(join.right(), EsRelation.class);
        assertThat(right.concreteQualifiedIndices(), is(Set.of("languages_lookup")));
    }

    /*
     * Project[[avg{r}#4, emp_no{f}#9, first_name{f}#10]]
     * \_Limit[10[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#9],[emp_no{f}#9],[emp_no{r}#9]]
     *     |_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_Project[[avg{r}#4, emp_no{f}#9]]
     *       \_Eval[[$$SUM$avg$0{r$}#20 / $$COUNT$avg$1{r$}#21 AS avg#4]]
     *         \_Aggregate[[emp_no{f}#9],[SUM(salary{f}#14,true[BOOLEAN]) AS $$SUM$avg$0#20, COUNT(salary{f}#14,true[BOOLEAN]) AS $$COUNT$
     *              avg$1#21, emp_no{f}#9]]
     *           \_StubRelation[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     */
    public void testInlineStatsWithAvg() {
        var query = """
            FROM employees
            | INLINE STATS avg = AVG(salary) BY emp_no
            | KEEP avg, emp_no, first_name
            | LIMIT 10
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "emp_no", "first_name")));
        var upperLimit = asLimit(project.child(), 10, false);
        var inlineJoin = as(upperLimit.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        // Right
        project = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), contains("avg", "emp_no"));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{r}#5]]
     * \_Limit[1000[INTEGER],false]
     *   \_LocalRelation[[salary{r}#3, emp_no{r}#5, gender{r}#7],
     *      org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@9d5b596d]
     */
    public void testInlineStatsWithRow() {
        var query = """
            ROW salary = 12300, emp_no = 5, gender = "F"
            | EVAL salaryK = salary/1000
            | INLINE STATS sum = SUM(salaryK) BY gender
            | KEEP emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no")));
        var limit = asLimit(project.child(), 1000, false);
        var localRelation = as(limit.child(), LocalRelation.class);
        assertThat(
            localRelation.output(),
            containsIgnoringIds(
                new ReferenceAttribute(EMPTY, "salary", INTEGER),
                new ReferenceAttribute(EMPTY, "emp_no", INTEGER),
                new ReferenceAttribute(EMPTY, "gender", KEYWORD)
            )
        );
    }

    /**
     * Project[[first_name{r}#32]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[_fork{r}#33 == fork1[KEYWORD]]
     *     \_Fork[[first_name{r}#32, _fork{r}#33]]
     *       |_Project[[first_name{f}#11, _fork{r}#7]]
     *       | \_Eval[[fork1[KEYWORD] AS _fork#7]]
     *       |   \_Limit[1000[INTEGER],false,false]
     *       |     \_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *       \_Project[[first_name{f}#22, _fork{r}#7]]
     *         \_Eval[[fork2[KEYWORD] AS _fork#7]]
     *           \_Limit[1000[INTEGER],false,false]
     *             \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     */
    public void testPruneColumnsInForkBranchesSimpleEvalOutsideBranches() {
        var query = """
                FROM employees
                | KEEP first_name
                | EVAL x = 1.0
                | DROP x
                | FORK
                    (WHERE true)
                    (WHERE true)
                | WHERE _fork == "fork1"
                | DROP _fork
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("first_name"));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fork = as(filter.child(), Fork.class);
        assertThat(fork.output(), hasSize(2));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("first_name", "_fork"));
        for (LogicalPlan branch : fork.children()) {
            var branchProject = as(branch, Project.class);
            assertThat(branchProject.projections().size(), equalTo(2));
            assertThat(Expressions.names(branchProject.projections()), containsInAnyOrder("first_name", "_fork"));
            var branchEval = as(branchProject.child(), Eval.class);
            var alias = as(branchEval.fields().getFirst(), Alias.class);
            assertThat(alias.name(), equalTo("_fork"));
            var limitInBranch = as(branchEval.child(), Limit.class);
            var relation = as(limitInBranch.child(), EsRelation.class);
            assertThat(relation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("emp_no", "first_name"));
        }
    }

    /**
     * Project[[_fork{r}#76, emp_no{r}#62, x{r}#73, y{r}#74, z{r}#75]]
     * \_TopN[[Order[_fork{r}#76,ASC,LAST], Order[emp_no{r}#62,ASC,LAST]],1000[INTEGER],false]
     *   \_Fork[[emp_no{r}#62, x{r}#73, y{r}#74, z{r}#75, _fork{r}#76]]
     *     |_Project[[emp_no{f}#37, x{r}#15, y{r}#16, z{r}#17, _fork{r}#5]]
     *     | \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *     |   \_Grok[a{r}#10,Parser[pattern=%{WORD:x} %{WORD:y} %{WORD:z}, grok=org.elasticsearch.grok.Grok@1702ca8e],[x{r}#15, y{r}#
     * 16, z{r}#17]]
     *     |     \_Eval[[CONCAT(first_name{f}#38, [KEYWORD],TOSTRING(emp_no{f}#37), [KEYWORD],last_name{f}#41) AS a#10]]
     *     |       \_Limit[1000[INTEGER],false,false]
     *     |         \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#37)]
     *     |           \_EsRelation[employees][_meta_field{f}#43, emp_no{f}#37, first_name{f}#38, ..]
     *     \_Project[[emp_no{f}#48, x{r}#27, y{r}#28, z{r}#29, _fork{r}#5]]
     *       \_Eval[[fork2[KEYWORD] AS _fork#5]]
     *         \_Grok[b{r}#22,Parser[pattern=%{WORD:x} %{WORD:y} %{WORD:z}, grok=org.elasticsearch.grok.Grok@37d58d35],[x{r}#27, y{r}#
     * 28, z{r}#29]]
     *           \_Eval[[CONCAT(last_name{f}#52, [KEYWORD],TOSTRING(emp_no{f}#48), [KEYWORD],first_name{f}#49) AS b#22]]
     *             \_Limit[1000[INTEGER],false,false]
     *               \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#48)]
     *                 \_EsRelation[employees][_meta_field{f}#54, emp_no{f}#48, first_name{f}#49, ..]
     */
    public void testPruneColumnsInForkBranchesDropNestedEvalsFromOutputIfNotNeeded() {
        // In this test, the EVALs that create 'a' and 'b' inside each branch should be dropped from the output
        // since they are not needed after the GROK
        var query = """
              FROM employees
             | WHERE emp_no == 10048 OR emp_no == 10081
             | FORK (EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                     | GROK a "%{WORD:x} %{WORD:y} %{WORD:z}" )
                    (EVAL b = CONCAT(last_name, " ", emp_no::keyword, " ", first_name)
                     | GROK b "%{WORD:x} %{WORD:y} %{WORD:z}" )
             | KEEP _fork, emp_no, x, y, z
             | SORT _fork, emp_no
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(5));
        assertThat(Expressions.names(project.projections()), containsInAnyOrder("_fork", "emp_no", "x", "y", "z"));
        var topN = as(project.child(), TopN.class);
        var fork = as(topN.child(), Fork.class);
        assertThat(fork.output(), hasSize(5));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("emp_no", "x", "y", "z", "_fork"));
        for (LogicalPlan branch : fork.children()) {
            var branchProject = as(branch, Project.class);
            assertThat(branchProject.projections().size(), equalTo(5));
            assertThat(Expressions.names(branchProject.projections()), containsInAnyOrder("emp_no", "x", "y", "z", "_fork"));
            var branchEval = as(branchProject.child(), Eval.class);
            var forkAlias = as(branchEval.fields().getFirst(), Alias.class);
            assertThat(forkAlias.name(), equalTo("_fork"));
            var grok = as(branchEval.child(), Grok.class);
            assertThat(grok.extractedFields().size(), equalTo(3));
            assertThat(Expressions.names(grok.extractedFields()), containsInAnyOrder("x", "y", "z"));
            var evalInBranch = as(grok.child(), Eval.class);
            var aliasInBranch = as(evalInBranch.fields().getFirst(), Alias.class);
            // The EVAL that created 'a' or 'b' should still be present in the branch even if not part of the output
            assertThat(aliasInBranch.name(), anyOf(equalTo("a"), equalTo("b")));
            var limitInBranch = as(evalInBranch.child(), Limit.class);
            var filter = as(limitInBranch.child(), Filter.class);
            assertThat(filter.toString(), containsString("IN(10048[INTEGER],10081[INTEGER],emp_no"));
            var relation = as(filter.child(), EsRelation.class);
            assertThat(
                relation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
                hasItems("emp_no", "first_name", "last_name")
            );
        }
    }

    /**
     * Limit[10000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#36, COUNT(emp_no{r}#109,true[BOOLEAN],PT0S[
     * TIME_DURATION]) AS d#39,MAX(_fork{r}#124,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#42,
     *          COUNT(s{r}#119,true[BOOLEAN],PT0S[TIME_DURATION]) AS ls#45]]
     *   \_Fork[[emp_no{r}#109, s{r}#119, _fork{r}#124]]
     *     |_Project[[emp_no{f}#46, s{r}#6, _fork{r}#7]]
     *     | \_Eval[[fork1[KEYWORD] AS _fork#7]]
     *     |   \_Dissect[a{r}#21,Parser[pattern=%{x} %{y} %{z}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@53c057b
     * 1],[x{r}#22, y{r}#23, z{r}#24]]
     *     |     \_Eval[[10[INTEGER] AS s#6, CONCAT(first_name{f}#47, [KEYWORD],TOSTRING(emp_no{f}#46), [KEYWORD],last_name{f}#50) AS
     * a#21]]
     *     |       \_Limit[1000[INTEGER],false,false]
     *     |         \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#46)]
     *     |           \_EsRelation[employees][_meta_field{f}#52, emp_no{f}#46, first_name{f}#47, ..]
     *     |_Project[[emp_no{r}#91, s{r}#101, _fork{r}#7]]
     *     | \_Eval[[fork2[KEYWORD] AS _fork#7, null[INTEGER] AS emp_no#91, null[INTEGER] AS s#101]]
     *     |   \_Limit[1000[INTEGER],false,false]
     *     |     \_LocalRelation[[$$COUNT$COUNT(*)::keywo>$0{r$}#125],Page{blocks=[ConstantNullBlock[positions=1]]}]
     *     |_Project[[emp_no{f}#68, s{r}#6, _fork{r}#7]]
     *     | \_Eval[[fork3[KEYWORD] AS _fork#7]]
     *     |   \_TopN[[Order[emp_no{f}#68,ASC,LAST]],2[INTEGER],false]
     *     |     \_Eval[[10[INTEGER] AS s#6]]
     *     |       \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#68)]
     *     |         \_EsRelation[employees][_meta_field{f}#74, emp_no{f}#68, first_name{f}#69, ..]
     *     \_Project[[emp_no{f}#79, s{r}#6, _fork{r}#7]]
     *       \_Eval[[10[INTEGER] AS s#6, fork4[KEYWORD] AS _fork#7]]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#79)]
     *             \_EsRelation[employees][_meta_field{f}#85, emp_no{f}#79, first_name{f}#80, ..]
     */
    public void testPruneColumnsInForkBranchesPruneIfAggregation() {
        var query = """
            FROM employees
            | WHERE emp_no == 10048 OR emp_no == 10081
            | EVAL s = 10
            | FORK ( EVAL a = CONCAT(first_name, " ", emp_no::keyword, " ", last_name)
                    | DISSECT a "%{x} %{y} %{z}"
                    | EVAL y = y::keyword )
                   ( STATS x = COUNT(*)::keyword, y = MAX(emp_no)::keyword, z = MIN(emp_no)::keyword )
                   ( SORT emp_no ASC | LIMIT 2 | EVAL x = last_name )
                   ( EVAL x = "abc" | EVAL y = "aaa" )
            | STATS count(*), d = count(emp_no), m = max(_fork), ls = count(s)
            """;
        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(4));
        assertThat(Expressions.names(aggregate.aggregates()), containsInAnyOrder("count(*)", "d", "m", "ls"));
        var fork = as(aggregate.child(), Fork.class);
        assertThat(fork.output().size(), equalTo(3));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), containsInAnyOrder("emp_no", "s", "_fork"));

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        assertThat(firstBranchProject.projections().size(), equalTo(3));
        var evalInFirstBranch = as(firstBranchProject.child(), Eval.class);
        var dissect = as(evalInFirstBranch.child(), Dissect.class);
        assertThat(Expressions.names(dissect.extractedFields()), containsInAnyOrder("x", "y", "z"));
        var evalAfterDissect = as(dissect.child(), Eval.class);
        assertThat(Expressions.names(evalAfterDissect.fields()), containsInAnyOrder("a", "s"));
        var limitInFirstBranch = as(evalAfterDissect.child(), Limit.class);
        var filterInFirstBranch = as(limitInFirstBranch.child(), Filter.class);
        var firstBranchRelation = as(filterInFirstBranch.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "last_name")
        );

        var secondBranch = fork.children().get(1);
        var secondBranchProject = as(secondBranch, Project.class);
        assertThat(secondBranchProject.projections().size(), equalTo(3));
        var evalInSecondBranch = as(secondBranchProject.child(), Eval.class);
        var limitInSecondBranch = as(evalInSecondBranch.child(), Limit.class);
        var localRelation = as(limitInSecondBranch.child(), LocalRelation.class);
        assertThat(
            localRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("$$COUNT$COUNT(*)::keywo>$0")
        );

        var thirdBranch = fork.children().get(2);
        var thirdBranchProject = as(thirdBranch, Project.class);
        assertThat(thirdBranchProject.projections().size(), equalTo(3));
        assertThat(Expressions.names(thirdBranchProject.projections()), containsInAnyOrder("emp_no", "s", "_fork"));
        var evalInThirdBranch = as(thirdBranchProject.child(), Eval.class);
        var topNInThirdBranch = as(evalInThirdBranch.child(), TopN.class);
        var evalBeforeTopN = as(topNInThirdBranch.child(), Eval.class);
        var filterInThirdBranch = as(evalBeforeTopN.child(), Filter.class);
        var thirdBranchRelation = as(filterInThirdBranch.child(), EsRelation.class);
        assertThat(
            thirdBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "last_name")
        );

        var fourthBranch = fork.children().get(3);
        var fourthBranchProject = as(fourthBranch, Project.class);
        assertThat(fourthBranchProject.projections().size(), equalTo(3));
        assertThat(Expressions.names(fourthBranchProject.projections()), containsInAnyOrder("emp_no", "s", "_fork"));
        var evalInFourthBranch = as(fourthBranchProject.child(), Eval.class);
        var limitInFourthBranch = as(evalInFourthBranch.child(), Limit.class);
        var filterInFourthBranch = as(limitInFourthBranch.child(), Filter.class);
        var fourthBranchRelation = as(filterInFourthBranch.child(), EsRelation.class);
        assertThat(
            fourthBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "last_name")
        );
    }

    /**
     * Project[[languages{r}#33]]
     * \_Limit[10000[INTEGER],false,false]
     *   \_Filter[_fork{r}#34 == fork1[KEYWORD]]
     *     \_Fork[[languages{r}#33, _fork{r}#34]]
     *       |_Project[[languages{r}#6, _fork{r}#8]]
     *       | \_Eval[[123[INTEGER] AS languages#6, fork1[KEYWORD] AS _fork#8]]
     *       |   \_Limit[4[INTEGER],false,false]
     *       |     \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *       \_Project[[languages{r}#6, _fork{r}#8]]
     *         \_Eval[[123[INTEGER] AS languages#6, fork2[KEYWORD] AS _fork#8]]
     *           \_Limit[4[INTEGER],false,false]
     *             \_EsRelation[employees][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     */
    public void testPruneColumnsInForkBranchesShouldRespectOuterAlias() {
        var query = """
            from employees metadata _index
            | drop languages
            | eval languages = 123
            | keep languages
            | limit 4
            | fork
                (where true)
                (where true)
            | where _fork == "fork1"
            | drop _fork
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("languages"));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fork = as(filter.child(), Fork.class);
        assertThat(fork.output(), hasSize(2));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("languages", "_fork"));
        for (LogicalPlan branch : fork.children()) {
            var branchProject = as(branch, Project.class);
            assertThat(branchProject.projections().size(), equalTo(2));
            assertThat(Expressions.names(branchProject.projections()), containsInAnyOrder("languages", "_fork"));
            var branchEval = as(branchProject.child(), Eval.class);
            var fields = branchEval.fields();
            assertThat(fields.size(), equalTo(2));
            var languagesAlias = as(fields.get(0), Alias.class);
            assertThat(languagesAlias.name(), equalTo("languages"));
            var forkAlias = as(fields.get(1), Alias.class);
            assertThat(forkAlias.name(), equalTo("_fork"));
            var limitInBranch = as(branchEval.child(), Limit.class);
            var relation = as(limitInBranch.child(), EsRelation.class);
            assertThat(relation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("emp_no", "first_name"));
        }
    }

    /**
     * Project[[languages{r}#55]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[_fork{r}#57 == fork1[KEYWORD]]
     *     \_Fork[[languages{r}#55, _fork{r}#57]]
     *       |_Project[[x{r}#15 AS languages#9, _fork{r}#13]]
     *       | \_Eval[[1[INTEGER] AS x#15, fork1[KEYWORD] AS _fork#13]]
     *       |   \_Limit[1000[INTEGER],false,false]
     *       |     \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     *       \_Project[[languages{r}#43, _fork{r}#13]]
     *         \_Eval[[fork2[KEYWORD] AS _fork#13, null[INTEGER] AS languages#43]]
     *           \_Limit[1000[INTEGER],false,false]
     *             \_EsRelation[employees][_meta_field{f}#38, emp_no{f}#32, first_name{f}#33, ..]
     */
    public void testPruneColumnsInForkBranchesShouldKeepAliasWithSameNameAsColumn() {
        var query = """
            from employees
            | drop languages
            | fork
                (eval x = 1 | rename x as lang | rename lang as languages | eval a = "aardvark" | rename a as foo)
                (where true)
            | where _fork == "fork1"
            | drop _fork
            | keep languages
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("languages"));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fork = as(filter.child(), Fork.class);
        assertThat(fork.output(), hasSize(2));
        assertThat(fork.children(), hasSize(2));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("languages", "_fork"));

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        assertThat(firstBranchProject.projections().size(), equalTo(2));
        assertThat(Expressions.names(firstBranchProject.projections()), containsInAnyOrder("languages", "_fork"));
        var firstBranchEval = as(firstBranchProject.child(), Eval.class);
        var firstBranchFields = firstBranchEval.fields();
        assertThat(firstBranchFields.size(), equalTo(2));
        assertThat(Expressions.names(firstBranchFields), containsInAnyOrder("x", "_fork"));
        var limitInFirstBranch = as(firstBranchEval.child(), Limit.class);
        var firstBranchRelation = as(limitInFirstBranch.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name")
        );

        var secondBranch = fork.children().get(1);
        var secondBranchProject = as(secondBranch, Project.class);
        assertThat(secondBranchProject.projections().size(), equalTo(2));
        assertThat(Expressions.names(secondBranchProject.projections()), containsInAnyOrder("languages", "_fork"));
        var secondBranchEval = as(secondBranchProject.child(), Eval.class);
        var secondBranchFields = secondBranchEval.fields();
        assertThat(secondBranchFields.size(), equalTo(2));
        assertThat(Expressions.names(secondBranchFields), containsInAnyOrder("languages", "_fork"));
        var limitInSecondBranch = as(secondBranchEval.child(), Limit.class);
        var secondBranchRelation = as(limitInSecondBranch.child(), EsRelation.class);
        assertThat(
            secondBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name")
        );
    }

    /**
     * <pre>{@code
     * Project[[_meta_field{r}#48, emp_no{r}#49, first_name{r}#50, gender{r}#51, hire_date{r}#52, job{r}#53, job.raw{r}#54, l
     * ast_name{r}#55, long_noidx{r}#56, salary{r}#57]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[_fork{r}#59 == fork1[KEYWORD]]
     *     \_Fork[[_meta_field{r}#48, emp_no{r}#49, first_name{r}#50, gender{r}#51, hire_date{r}#52, job{r}#53, job.raw{r}#54, l
     * ast_name{r}#55, long_noidx{r}#56, salary{r}#57, _fork{r}#59]]
     *       |_Project[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, l
     * ast_name{f}#19, long_noidx{f}#25, salary{f}#20, _fork{r}#9]]
     *       | \_Eval[[fork1[KEYWORD] AS _fork#9]]
     *       |   \_Limit[1000[INTEGER],false,false]
     *       |     \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *       \_Project[[_meta_field{f}#43, emp_no{f}#37, first_name{f}#38, gender{f}#39, hire_date{f}#44, job{f}#45, job.raw{f}#46, l
     * ast_name{f}#41, long_noidx{f}#47, salary{f}#42, _fork{r}#9]]
     *         \_Eval[[fork3[KEYWORD] AS _fork#9]]
     *           \_Limit[1000[INTEGER],false,false]
     *             \_EsRelation[employees][_meta_field{f}#43, emp_no{f}#37, first_name{f}#38, ..]
     * }</pre>
     */
    public void testPruneColumnsInForkBranchesShouldPruneNestedEvalsIfColumnIsDropped() {
        var query = """
            from employees
            | fork
                (eval x = 1 | rename x as lang | rename lang as languages)
                (where false)
                (where true)
            | where _fork == "fork1"
            | drop _fork
            | drop languages
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(10));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var fork = as(filter.child(), Fork.class);
        assertThat(fork.output().size(), equalTo(11));

        // second branch is completely removed
        assertEquals(2, fork.children().size());

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        assertThat(firstBranchProject.projections().size(), equalTo(11));
        var firstBranchEval = as(firstBranchProject.child(), Eval.class);
        var forkAliasInFirstBranch = as(firstBranchEval.fields().getFirst(), Alias.class);
        assertThat(forkAliasInFirstBranch.name(), equalTo("_fork"));
        var limitInFirstBranch = as(firstBranchEval.child(), Limit.class);
        var firstBranchRelation = as(limitInFirstBranch.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "gender", "hire_date", "job", "job.raw", "last_name", "long_noidx", "salary")
        );

        var thirdBranch = fork.children().get(1);
        var thirdBranchProject = as(thirdBranch, Project.class);
        assertThat(thirdBranchProject.projections().size(), equalTo(11));
        var thirdBranchEval = as(thirdBranchProject.child(), Eval.class);
        var forkAliasInThirdBranch = as(thirdBranchEval.fields().getFirst(), Alias.class);
        assertThat(forkAliasInThirdBranch.name(), equalTo("_fork"));
        var limitInThirdBranch = as(thirdBranchEval.child(), Limit.class);
        var thirdBranchRelation = as(limitInThirdBranch.child(), EsRelation.class);
        assertThat(
            thirdBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "gender", "hire_date", "job", "job.raw", "last_name", "long_noidx", "salary")
        );
    }

    /**
     * Project[[a{r}#45]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Fork[[a{r}#45]]
     *     |_Project[[a{r}#5]]
     *     | \_Limit[1000[INTEGER],false,false]
     *     |   \_Aggregate[[],[MAX(salary{f}#26,true[BOOLEAN],PT0S[TIME_DURATION]) AS a#5]]
     *     |     \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     *     \_Project[[a{r}#14]]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_Aggregate[[],[MAX(salary{f}#37,true[BOOLEAN],PT0S[TIME_DURATION]) AS a#14]]
     *           \_EsRelation[employees][_meta_field{f}#38, emp_no{f}#32, first_name{f}#33, ..]
     */
    public void testPruneColumnsInForkBranchesKeepOnlyNeededAggs() {
        var query = """
            from employees
             | fork (stats a = max(salary), b = min(salary) | KEEP b, a | EVAL x = 1 )
                    (stats a = max(salary), b = min(salary))
             | KEEP a
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("a"));
        var limit = as(project.child(), Limit.class);
        var fork = as(limit.child(), Fork.class);
        assertThat(fork.output().size(), equalTo(1));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("a"));
        for (LogicalPlan branch : fork.children()) {
            var branchProject = as(branch, Project.class);
            assertThat(branchProject.projections().size(), equalTo(1));
            assertThat(Expressions.names(branchProject.projections()), contains("a"));
            var limitInBranch = as(branchProject.child(), Limit.class);
            var aggregateInBranch = as(limitInBranch.child(), Aggregate.class);
            assertThat(aggregateInBranch.aggregates().size(), equalTo(1));
            assertThat(Expressions.names(aggregateInBranch.aggregates()), contains("a"));
            var relation = as(aggregateInBranch.child(), EsRelation.class);
            assertThat(relation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("salary"));
        }
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_InlineJoin[LEFT,[],[]]
     *   |_InlineJoin[LEFT,[languages{f}#21],[languages{r}#21]]
     *   | |_Project[[languages{f}#21, salary{f}#23]]
     *   | | \_EsRelation[employees][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *   | \_Project[[languages1{r}#7, languages{f}#21]]
     *   |   \_Eval[[MVAVG(languages{f}#21) AS languages1#7]]
     *   |     \_Aggregate[[languages{f}#21],[languages{f}#21]]
     *   |       \_StubRelation[[languages{f}#21, salary{f}#23]]
     *   \_Project[[languages2{r}#14, avg{r}#17]]
     *     \_Eval[[$$SUM$avg$0{r$}#31 / $$COUNT$avg$1{r$}#32 AS avg#17]]
     *       \_Aggregate[[],[SUM(languages1{r}#7,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS languages2#14, SUM(salary{f}
     * #23,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#31, COUNT(salary{f}#23,true[BOOLEAN],PT0S[TIME_DURATION])
     * AS $$COUNT$avg$1#32]]
     *         \_StubRelation[[salary{f}#23, languages1{r}#7, avg{r}#10, languages{f}#21]]
     */
    public void testDoubleInlineStatsNotPrunning_With_MV_Functions() {
        var query = """
            FROM employees
            | KEEP languages, salary
            | INLINE STATS languages1 = MV_AVG(languages), avg = AVG(salary) BY languages
            | INLINE STATS languages2 = SUM(languages1), avg = AVG(salary)
            """;

        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        var topJoin = as(limit.child(), InlineJoin.class);
        // Left
        var leftJoin = as(topJoin.left(), InlineJoin.class);
        var leftProject = as(leftJoin.left(), Project.class);
        assertMap(Expressions.names(leftProject.projections()), is(List.of("languages", "salary")));
        as(leftProject.child(), EsRelation.class);
        var rightProject = as(leftJoin.right(), Project.class);
        assertMap(Expressions.names(rightProject.projections()), is(List.of("languages1", "languages")));
        var rightEval = as(rightProject.child(), Eval.class);
        var aggregate = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(aggregate.groupings()), is(List.of("languages")));
        as(aggregate.child(), StubRelation.class);
        // Right
        var rightProjectTop = as(topJoin.right(), Project.class);
        assertMap(Expressions.names(rightProjectTop.projections()), is(List.of("languages2", "avg")));
        var rightEvalTop = as(rightProjectTop.child(), Eval.class);
        var aggregateTop = as(rightEvalTop.child(), Aggregate.class);
        assertMap(Expressions.names(aggregateTop.aggregates()), is(List.of("languages2", "$$SUM$avg$0", "$$COUNT$avg$1")));
        as(aggregateTop.child(), StubRelation.class);
    }

    /*
     * Project[[salary{f}#15, aaaaa{r}#7]]
     * \_Limit[10[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[languages{f}#13],[languages{r}#13]]
     *     |_Project[[languages{f}#13, salary{f}#15]]
     *     | \_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *     \_Project[[aaaaa{r}#7, languages{f}#13]]
     *       \_Eval[[$$SUM$aaaaa$0{r$}#21 / $$COUNT$aaaaa$1{r$}#22 AS aaaaa#7]]
     *         \_Aggregate[[languages{f}#13],[SUM(salary{f}#15,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$aaaaa$0#21
     * , COUNT(salary{f}#15,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$aaaaa$1#22, languages{f}#13]]
     *           \_StubRelation[[languages{f}#13, salary{f}#15]]
     */
    public void testDropInlineStatsGrouping() {
        var query = """
            FROM employees
            | KEEP languages, salary
            | INLINE STATS aaaaa = AVG(salary) BY languages
            | DROP languages
            | LIMIT 10
            """;

        var plan = as(optimizedPlan(query), Project.class);
        var limit = as(plan.child(), Limit.class);
        var join = as(limit.child(), InlineJoin.class);
        // Left
        var leftProject = as(join.left(), Project.class);
        assertMap(Expressions.names(leftProject.projections()), is(List.of("languages", "salary")));
        as(leftProject.child(), EsRelation.class);
        // Right
        var rightProject = as(join.right(), Project.class);
        assertMap(Expressions.names(rightProject.projections()), is(List.of("aaaaa", "languages")));
        var rightEval = as(rightProject.child(), Eval.class);
        var aggregate = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(aggregate.aggregates()), is(List.of("$$SUM$aaaaa$0", "$$COUNT$aaaaa$1", "languages")));
        as(aggregate.child(), StubRelation.class);
    }

    /*
     * Project[[salary{f}#16, aaaaa{r}#7]]
     * \_Limit[10[INTEGER],false,false]
     *   \_InlineJoin[LEFT,[languages{f}#14],[languages{r}#14]]
     *     |_Project[[languages{f}#14, salary{f}#16]]
     *     | \_EsRelation[employees][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *     \_Project[[aaaaa{r}#7, languages{f}#14]]
     *       \_Eval[[$$SUM$aaaaa$0{r$}#22 / $$COUNT$aaaaa$1{r$}#23 AS aaaaa#7]]
     *         \_Aggregate[[languages{f}#14],[SUM(salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$aaaaa$0#22
     * , COUNT(salary{f}#16,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$aaaaa$1#23, languages{f}#14]]
     *           \_StubRelation[[languages{f}#14, salary{f}#16]]
     */
    public void testKeepExceptInlineStatsGrouping() {
        var query = """
            FROM employees
            | KEEP languages, salary
            | INLINE STATS aaaaa = AVG(salary) BY languages
            | KEEP salary, aaaaa
            | LIMIT 10
            """;

        var plan = optimizedPlan(query);
        Project project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);
        var join = as(limit.child(), InlineJoin.class);
        // Left
        var leftProject = as(join.left(), Project.class);
        assertMap(Expressions.names(leftProject.projections()), is(List.of("languages", "salary")));
        as(leftProject.child(), EsRelation.class);
        // Right
        var rightProject = as(join.right(), Project.class);
        assertMap(Expressions.names(rightProject.projections()), is(List.of("aaaaa", "languages")));
        var rightEval = as(rightProject.child(), Eval.class);
        var aggregate = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(aggregate.aggregates()), is(List.of("$$SUM$aaaaa$0", "$$COUNT$aaaaa$1", "languages")));
        as(aggregate.child(), StubRelation.class);
    }

    /*
     * Project[[salary{f}#26, languages1{r}#7, languages2{r}#14, avg{r}#17]]
     * \_TopN[[Order[salary{f}#26,ASC,LAST]],10[INTEGER],false]
     *   \_InlineJoin[LEFT,[languages{f}#24],[languages{r}#24]]
     *     |_InlineJoin[LEFT,[languages{f}#24],[languages{r}#24]]
     *     | |_Project[[languages{f}#24, salary{f}#26]]
     *     | | \_EsRelation[employees][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     *     | \_Project[[languages1{r}#7, languages{f}#24]]
     *     |   \_Eval[[MVAVG(languages{f}#24) AS languages1#7]]
     *     |     \_Aggregate[[languages{f}#24],[languages{f}#24]]
     *     |       \_StubRelation[[languages{f}#24, salary{f}#26]]
     *     \_Project[[languages2{r}#14, avg{r}#17, languages{f}#24]]
     *       \_Eval[[$$SUM$avg$0{r$}#34 / $$COUNT$avg$1{r$}#35 AS avg#17]]
     *         \_Aggregate[[languages{f}#24],[SUM(languages1{r}#7,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS languages2#14
     * , SUM(salary{f}#26,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#34,
     * COUNT(salary{f}#26,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#35, languages{f}#24]]
     *           \_StubRelation[[salary{f}#26, languages1{r}#7, avg{r}#10, languages{f}#24]]
     */
    public void testDropDoubleInlineStatsGrouping() {
        var query = """
            FROM employees
            | KEEP languages, salary
            | INLINE STATS languages1 = MV_AVG(languages), avg = AVG(salary) BY languages
            | INLINE STATS languages2 = SUM(languages1), avg = AVG(salary) BY languages
            | DROP languages
            | SORT salary
            | LIMIT 10
            """;

        var plan = optimizedPlan(query);
        Project project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        var join = as(topN.child(), InlineJoin.class);
        // Left
        var leftJoin = as(join.left(), InlineJoin.class);
        var leftProject = as(leftJoin.left(), Project.class);
        assertMap(Expressions.names(leftProject.projections()), is(List.of("languages", "salary")));
        as(leftProject.child(), EsRelation.class);
        var rightProject = as(leftJoin.right(), Project.class);
        assertMap(Expressions.names(rightProject.projections()), is(List.of("languages1", "languages")));
        var rightEval = as(rightProject.child(), Eval.class);
        var aggregate = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(aggregate.groupings()), is(List.of("languages")));
        as(aggregate.child(), StubRelation.class);
        // Right
        var rightProjectTop = as(join.right(), Project.class);
        assertMap(Expressions.names(rightProjectTop.projections()), is(List.of("languages2", "avg", "languages")));
        var rightEvalTop = as(rightProjectTop.child(), Eval.class);
        var aggregateTop = as(rightEvalTop.child(), Aggregate.class);
        assertMap(Expressions.names(aggregateTop.aggregates()), is(List.of("languages2", "$$SUM$avg$0", "$$COUNT$avg$1", "languages")));
    }

    /*
     * Project[[salary{f}#30, languages1{r}#9, long_noidx{f}#35, languages2{r}#17, avg{r}#20, gender{f}#27]]
     * \_TopN[[Order[salary{f}#30,ASC,LAST]],10[INTEGER],false]
     *   \_InlineJoin[LEFT,[languages{f}#28, gender{f}#27],[languages{r}#28, gender{r}#27]]
     *     |_InlineJoin[LEFT,[languages{f}#28, long_noidx{f}#35],[languages{r}#28, long_noidx{r}#35]]
     *     | |_Project[[languages{f}#28, salary{f}#30, long_noidx{f}#35, gender{f}#27]]
     *     | | \_EsRelation[employees][_meta_field{f}#31, emp_no{f}#25, first_name{f}#26, ..]
     *     | \_Project[[languages1{r}#9, languages{f}#28, long_noidx{f}#35]]
     *     |   \_Eval[[MVAVG(long_noidx{f}#35) AS languages1#9]]
     *     |     \_Aggregate[[languages{f}#28, long_noidx{f}#35],[languages{f}#28, long_noidx{f}#35]]
     *     |       \_StubRelation[[languages{f}#28, salary{f}#30, long_noidx{f}#35, gender{f}#27]]
     *     \_Project[[languages2{r}#17, avg{r}#20, languages{f}#28, gender{f}#27]]
     *       \_Eval[[$$SUM$avg$0{r$}#38 / $$COUNT$avg$1{r$}#39 AS avg#20]]
     *         \_Aggregate[[languages{f}#28, gender{f}#27],[SUM(languages1{r}#9,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS
     *  languages2#17, SUM(salary{f}#30,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#38,
     *  COUNT(salary{f}#30,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#39, languages{f}#28, gender{f}#27]]
     *           \_StubRelation[[salary{f}#30, gender{f}#27, languages1{r}#9, avg{r}#12, languages{f}#28, long_noidx{f}#35]]
     */
    public void testDropDoubleInlineStats_PartialMultipleGroupings() {
        var query = """
            FROM employees
            | KEEP languages, salary, long_noidx, gender
            | INLINE STATS languages1 = MV_AVG(long_noidx), avg = AVG(salary) BY languages, long_noidx
            | INLINE STATS languages2 = SUM(languages1), avg = AVG(salary) BY languages, gender
            | DROP languages
            | SORT salary
            | LIMIT 10
            """;

        var plan = optimizedPlan(query);
        Project project = as(plan, Project.class);
        var topN = as(project.child(), TopN.class);
        var join = as(topN.child(), InlineJoin.class);
        // Left
        var leftJoin = as(join.left(), InlineJoin.class);
        var leftProject = as(leftJoin.left(), Project.class);
        assertMap(Expressions.names(leftProject.projections()), is(List.of("languages", "salary", "long_noidx", "gender")));
        as(leftProject.child(), EsRelation.class);
        var rightProject = as(leftJoin.right(), Project.class);
        assertMap(Expressions.names(rightProject.projections()), is(List.of("languages1", "languages", "long_noidx")));
        var rightEval = as(rightProject.child(), Eval.class);
        var aggregate = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(aggregate.groupings()), is(List.of("languages", "long_noidx")));
        as(aggregate.child(), StubRelation.class);
        // Right
        var rightProjectTop = as(join.right(), Project.class);
        assertMap(Expressions.names(rightProjectTop.projections()), is(List.of("languages2", "avg", "languages", "gender")));
        var rightEvalTop = as(rightProjectTop.child(), Eval.class);
        var aggregateTop = as(rightEvalTop.child(), Aggregate.class);
        assertMap(
            Expressions.names(aggregateTop.aggregates()),
            is(List.of("languages2", "$$SUM$avg$0", "$$COUNT$avg$1", "languages", "gender"))
        );
    }

    /**
     * Project[[!languages, $$languages$converted_to$long{f$}#12 AS x#6]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[union_types_index*][!first_name, id{f}#7, !languages, !last_name, !sala..]
     */
    public void testExplicitRetainOriginalFieldWithCast() {
        LogicalPlan plan = planUnionIndex("""
            FROM union_types_index*
            | KEEP languages
            | EVAL x = languages::long
            """);

        Project topProject = as(plan, Project.class);
        var projections = topProject.projections();
        assertThat(projections, hasSize(2));
        assertThat(projections.get(0).name(), equalTo("languages"));
        assertThat(projections.get(0).dataType(), equalTo(UNSUPPORTED));

        Alias xAlias = as(projections.get(1), Alias.class);
        assertThat(xAlias.name(), equalTo("x"));
        assertThat(xAlias.dataType(), equalTo(LONG));
        FieldAttribute syntheticFieldAttr = as(xAlias.child(), FieldAttribute.class);
        assertThat(syntheticFieldAttr.name(), equalTo("$$languages$converted_to$long"));
        ReferenceAttribute xRef = as(topProject.output().get(1), ReferenceAttribute.class);
        assertThat(xRef, is(xAlias.toAttribute()));

        Limit limit = asLimit(topProject.child(), 1000, false);
        EsRelation relation = as(limit.child(), EsRelation.class);
        assertCommonIncompatibleDataTypesEsRelation(relation);

        var relationOutput = relation.output();
        var syntheticField = relationOutput.get(5);
        assertThat(syntheticField.name(), equalTo("$$languages$converted_to$long"));
        assertThat(syntheticField.dataType(), equalTo(LONG));
        assertThat(syntheticFieldAttr.id(), equalTo(syntheticField.id()));
    }

    /*
     * Project[[id{f}#8]]
     * \_Limit[10[INTEGER],false,false]
     *   \_EsRelation[union_types_index*][!first_name, id{f}#8, !languages, !last_name, !sala..]
     */
    public void testPruneUnsupportedFieldsWithLimitCommand() {
        var query = """
            from union_types_index*
            | eval x = languages::long
            | limit 10
            | keep id
            """;

        var plan = planUnionIndex(query);
        Project project = as(plan, Project.class);

        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("id"));
        var limit = as(project.child(), Limit.class);
        var relation = as(limit.child(), EsRelation.class);
        var relationOutput = relation.output();
        assertCommonIncompatibleDataTypesEsRelation(relation);
        var syntheticField = relationOutput.get(5);
        assertThat(syntheticField.name(), equalTo("$$languages$converted_to$long"));
        assertThat(syntheticField.dataType(), equalTo(LONG));
    }

    /*
     * Limit[10000[INTEGER],false,false]
     * \_Project[[id{f}#7]]
     *   \_Limit[10[INTEGER],false,false]
     *     \_Project[[id{f}#7]]
     *       \_EsRelation[union_types_index*][!first_name, id{f}#7, !languages, !last_name, !sala..]
     */
    public void testPruneColumnsInProject_WithLimit() {
        var query = """
            from union_types_index*
            | eval x = languages::long
            | limit 10
            | keep id
            """;

        var analyzedPlan = unionIndexAnalyzer.analyze(parser.parseQuery(query));

        // run through this rule only because this is the one which enables the project pruning in this case and it's
        // how PruneColumns does its thing in case of Project prunning
        var prunedEval = new ReplaceAliasingEvalWithProject().apply(analyzedPlan);
        Limit topLimit = as(prunedEval, Limit.class);
        Project topProject = as(topLimit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        Limit limit = as(topProject.child(), Limit.class);
        Project project = as(limit.child(), Project.class);
        assertThat(project.projections().size(), equalTo(7));
        assertThat(
            Expressions.names(project.projections()),
            contains("first_name", "id", "languages", "last_name", "salary_change", "$$languages$converted_to$long", "x")
        );

        var prunedProject = new PruneColumns().apply(prunedEval);

        topLimit = as(prunedProject, Limit.class);
        topProject = as(topLimit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        limit = as(topProject.child(), Limit.class);
        project = as(limit.child(), Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("id"));
        EsRelation relation = as(project.child(), EsRelation.class);
        assertCommonIncompatibleDataTypesEsRelation(relation);
        var relationOutput = relation.output();
        var syntheticField = relationOutput.get(5);
        assertThat(syntheticField.name(), equalTo("$$languages$converted_to$long"));
        assertThat(syntheticField.dataType(), equalTo(LONG));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[id{f}#11]]
     *   \_LookupJoin[LEFT,[language_code{r}#5],[language_code{f}#13],false,null]
     *     |_Project[[id{f}#11, $$languages$converted_to$long{f$}#15, $$languages$converted_to$long{f$}#15 AS language_code#5]]
     *     | \_EsRelation[union_types_index*][!first_name, id{f}#11, !languages, !last_name, !sal..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#13]
     */
    public void testPruneColumnsInProject_WithLookupJoin() {
        var query = """
            from union_types_index*
            | eval language_code = languages::long
            | lookup join languages_lookup on language_code
            | keep id
            """;

        var analyzedPlan = unionIndexAnalyzer.analyze(parser.parseQuery(query));

        // run through this rule only because this is the one which enables the project pruning in this case and it's
        // how PruneColumns does its thing in case of Project prunning
        var prunedEval = new ReplaceAliasingEvalWithProject().apply(analyzedPlan);
        /*
         * Limit[1000[INTEGER],false,false]
         * \_Project[[id{f}#12]]
         *   \_LookupJoin[LEFT,[language_code{r}#5],[language_code{f}#13],false,null]
         *     |_Project[[!first_name, id{f}#12, !languages, !last_name, !salary_change, $$languages$converted_to$long{f$}#15, $$langua
         * ges$converted_to$long{f$}#15 AS language_code#5]]
         *     | \_EsRelation[union_types_index*][!first_name, id{f}#12, !languages, !last_name, !sal..]
         *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#13, language_name{f}#14]
         */
        Limit limit = as(prunedEval, Limit.class);
        Project topProject = as(limit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        LookupJoin lookupJoin = as(topProject.child(), LookupJoin.class);
        Project project = as(lookupJoin.left(), Project.class);
        assertThat(project.projections().size(), equalTo(7));
        assertThat(
            Expressions.names(project.projections()),
            contains("first_name", "id", "languages", "last_name", "salary_change", "$$languages$converted_to$long", "language_code")
        );

        var prunedProject = new PruneColumns().apply(prunedEval);

        limit = as(prunedProject, Limit.class);
        topProject = as(limit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        lookupJoin = as(topProject.child(), LookupJoin.class);
        project = as(lookupJoin.left(), Project.class);
        assertThat(project.projections().size(), equalTo(3));
        assertThat(Expressions.names(project.projections()), contains("id", "$$languages$converted_to$long", "language_code"));
        EsRelation relation = as(project.child(), EsRelation.class);
        assertCommonIncompatibleDataTypesEsRelation(relation);
        var relationOutput = relation.output();
        var syntheticField = relationOutput.get(5);
        assertThat(syntheticField.name(), equalTo("$$languages$converted_to$long"));
        assertThat(syntheticField.dataType(), equalTo(LONG));
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Project[[id{f}#12]]
     *   \_Dissect[x{r}#5,Parser[pattern=%{foo}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@18e5d3b5],[foo{r}#6]]
     *     \_Project[[id{f}#12, $$languages$converted_to$keyword{f$}#14, $$languages$converted_to$keyword{f$}#14 AS x#5]]
     *       \_EsRelation[union_types_index*][!first_name, id{f}#12, !languages, !last_name, !sal..]
     */
    public void testPruneColumnsInProject_WithDissect() {
        var query = """
            from union_types_index*
            | eval x = languages::string
            | dissect x "%{foo}"
            | keep id
            """;

        var analyzedPlan = unionIndexAnalyzer.analyze(parser.parseQuery(query));

        // run through this rule only because this is the one which enables the project pruning in this case and it's
        // how PruneColumns does its thing in case of Project prunning
        var prunedEval = new ReplaceAliasingEvalWithProject().apply(analyzedPlan);

        /*
         * Limit[1000[INTEGER],false,false]
         * \_Project[[id{f}#10]]
         *   \_Dissect[x{r}#5,Parser[pattern=%{foo}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@16d0b308],[foo{r}#6]]
         *     \_Project[[!first_name, id{f}#10, !languages, !last_name, !salary_change, $$languages$converted_to$keyword{f$}#14, $$lan
         * guages$converted_to$keyword{f$}#14 AS x#5]]
         *       \_EsRelation[union_types_index*][!first_name, id{f}#10, !languages, !last_name, !sal..]
         */
        Limit topLimit = as(prunedEval, Limit.class);
        Project topProject = as(topLimit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        Dissect dissect = as(topProject.child(), Dissect.class);
        Project project = as(dissect.child(), Project.class);
        assertThat(project.projections().size(), equalTo(7));
        assertThat(
            Expressions.names(project.projections()),
            contains("first_name", "id", "languages", "last_name", "salary_change", "$$languages$converted_to$keyword", "x")
        );

        var prunedProject = new PruneColumns().apply(prunedEval);

        topLimit = as(prunedProject, Limit.class);
        topProject = as(topLimit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        dissect = as(topProject.child(), Dissect.class);
        project = as(dissect.child(), Project.class);
        assertThat(project.projections().size(), equalTo(3));
        assertThat(Expressions.names(project.projections()), contains("id", "$$languages$converted_to$keyword", "x"));
        EsRelation relation = as(project.child(), EsRelation.class);
        assertCommonIncompatibleDataTypesEsRelation(relation);
        var relationOutput = relation.output();
        var syntheticField = relationOutput.get(5);
        assertThat(syntheticField.name(), equalTo("$$languages$converted_to$keyword"));
        assertThat(syntheticField.dataType(), equalTo(KEYWORD));
    }

    public static void assertCommonIncompatibleDataTypesEsRelation(EsRelation relation) {
        assertEquals("union_types_index*", relation.indexPattern());
        var relationOutput = relation.output();
        assertThat(relationOutput, hasSize(6));
        assertThat(relationOutput.get(0).name(), equalTo("first_name"));
        assertThat(relationOutput.get(0).dataType(), equalTo(UNSUPPORTED));
        assertThat(relationOutput.get(1).name(), equalTo("id"));
        assertThat(relationOutput.get(1).dataType(), equalTo(KEYWORD));
        assertThat(relationOutput.get(2).name(), equalTo("languages"));
        assertThat(relationOutput.get(2).dataType(), equalTo(UNSUPPORTED));
        assertThat(relationOutput.get(3).name(), equalTo("last_name"));
        assertThat(relationOutput.get(3).dataType(), equalTo(UNSUPPORTED));
        assertThat(relationOutput.get(4).name(), equalTo("salary_change"));
        assertThat(relationOutput.get(4).dataType(), equalTo(UNSUPPORTED));
    }
}
