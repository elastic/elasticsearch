/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
     * {@snippet lang="text":
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#1345) AS c]]
     *   \_EsRelation[test][_meta_field{f}#1346, emp_no{f}#1340, first_name{f}#..]
     * }
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
     * {@snippet lang="text":
     * Limit[1000[INTEGER]]
     * \_Aggregate[[],[COUNT(salary{f}#19) AS x]]
     *   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     * }
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
     * {@snippet lang="text":
     * Project[[c{r}#342]]
     * \_Limit[1000[INTEGER]]
     *   \_Filter[min{r}#348 > 10[INTEGER]]
     *     \_Aggregate[[],[COUNT(salary{f}#367) AS c, MIN(salary{f}#367) AS min]]
     *       \_EsRelation[test][_meta_field{f}#368, emp_no{f}#362, first_name{f}#36..]
     * }
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
     * {@snippet lang="text":
     * Eval[[max{r}#6 + min{r}#9 + c{r}#3 AS x, min{r}#9 AS y, c{r}#3 AS z]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[COUNT(salary{f}#26) AS c, MAX(salary{f}#26) AS max, MIN(salary{f}#26) AS min]]
     *     \_EsRelation[test][_meta_field{f}#27, emp_no{f}#21, first_name{f}#22, ..]
     * }
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
     * {@snippet lang="text":
     * Project[[y{r}#6 AS z]]
     * \_Eval[[emp_no{f}#11 + 1[INTEGER] AS y]]
     *   \_Limit[1000[INTEGER]]
     *     \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }
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
     * {@snippet lang="text":
     * Project[[salary{f}#20 AS x, emp_no{f}#15 AS y]]
     * \_Limit[1000[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     * }
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#12 AS x#8, emp_no{f}#12]]
     * \_TopN[[Order[emp_no{f}#12,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     * }
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#16, count{r}#7]]
     * \_TopN[[Order[emp_no{f}#16,ASC,LAST]],5[INTEGER]]
     *   \_InlineJoin[LEFT,[salaryK{r}#5],[salaryK{r}#5],[salaryK{r}#5]]
     *     |_Eval[[salary{f}#21 / 1000[INTEGER] AS salaryK#5]]
     *     | \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_Aggregate[[salaryK{r}#5],[COUNT(*[KEYWORD],true[BOOLEAN]) AS count#7, salaryK{r}#5]]
     *       \_StubRelation[[_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, gender{f}#18, hire_date{f}#23, job{f}#24, job.raw{f}#25,
     *              languages{f}#19, last_name{f}#20, long_noidx{f}#26, salary{f}#21, salaryK{r}#5]]
     * }
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#19 AS x#15, emp_no{f}#19]]
     * \_TopN[[Order[emp_no{f}#19,ASC,LAST]],1[INTEGER]]
     *   \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#15 AS x#11, a{r}#7, emp_no{f}#15]]
     * \_Limit[1[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#15],[emp_no{f}#15],[emp_no{r}#15]]
     *     |_EsRelation[test][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *     \_Aggregate[[emp_no{f}#15],[COUNTDISTINCT(languages{f}#18,true[BOOLEAN]) AS a#7, emp_no{f}#15]]
     *       \_StubRelation[[_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, gender{f}#17, hire_date{f}#22, job{f}#23, job.raw{f}#24, l
     *          anguages{f}#18, last_name{f}#19, long_noidx{f}#25, salary{f}#20]]
     * }
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#27, salaryK{r}#5, count{r}#7, min{r}#20]]
     * \_TopN[[Order[emp_no{f}#27,ASC,LAST]],5[INTEGER],false]
     *   \_InlineJoin[LEFT,[salaryK{r}#5],[salaryK{r}#5]]
     *     |_Project[[emp_no{f}#27, languages{f}#30, count{r}#7, salaryK{r}#5]]
     *     | \_InlineJoin[LEFT,[salaryK{r}#5],[salaryK{r}#5]]
     *     |   |_Eval[[salary{f}#32 / 10000[INTEGER] AS salaryK#5]]
     *     |   | \_EsRelation[employees][_meta_field{f}#33, emp_no{f}#27, first_name{f}#28, ..]
     *     |   \_Aggregate[[salaryK{r}#5],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count#7, salaryK{r}#5]]
     *     |     \_StubRelation[[_meta_field{f}#33, emp_no{f}#27, first_name{f}#28, gender{f}#29, hire_date{f}#34, job{f}#35, job.raw{f}#36,
     *            languages{f}#30, last_name{f}#31, long_noidx{f}#37, salary{f}#32, salaryK{r}#5]]
     *     \_Aggregate[[salaryK{r}#5],[MIN($$MV_COUNT(langua>$MIN$0{r$}#38,true[BOOLEAN],PT0S[TIME_DURATION]) AS min#20, salaryK{r}#5]]
     *       \_Eval[[MVCOUNT(languages{f}#30) AS $$MV_COUNT(langua>$MIN$0#38]]
     *         \_StubRelation[[_meta_field{f}#33, emp_no{f}#27, first_name{f}#28, gender{f}#29, hire_date{f}#34, job{f}#35, job.raw{f}#36,
     *         languages{f}#30, last_name{f}#31, long_noidx{f}#37, salary{f}#32, count{r}#7, salaryK{r}#5, sum{r}#14,
     *         hire_date_string{r}#11, date{r}#16, $$MV_COUNT(langua>$MIN$0{r$}#38]]
     * }
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
        // Dissect and Eval(hire_date_string) have been pruned since 'date' field is not used
        var innerinline = as(outerProject.child(), InlineJoin.class);
        var expectedInnerOutput = new ArrayList<>(employeesFields);
        expectedInnerOutput.addAll(List.of("count", "salaryK"));
        assertThat(Expressions.names(innerinline.output()), is(expectedInnerOutput));
        // inner left
        var eval = as(innerinline.left(), Eval.class);
        var relation = as(eval.child(), EsRelation.class);
        // inner right
        var agg = as(innerinline.right(), Aggregate.class);
        var stub = as(agg.child(), StubRelation.class);
        // outer right
        agg = as(outerinline.right(), Aggregate.class);
        eval = as(agg.child(), Eval.class);
        stub = as(eval.child(), StubRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[emp_no{f}#24]]
     * \_TopN[[Order[emp_no{f}#24,ASC,LAST]],5[INTEGER],false]
     *   \_EsRelation[employees][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     * }
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
        // Dissect and Eval(hire_date_string) have been pruned since 'date' field is not used
        var relation = as(topN.child(), EsRelation.class);
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
     * {@snippet lang="text":
     * Project[[first_name{r}#34]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Fork[[first_name{r}#34]]
     *     |_Project[[first_name{f}#13]]
     *     | \_Limit[1000[INTEGER],false,false]
     *     |   \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *     \_Project[[first_name{f}#24]]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_EsRelation[employees][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     * }
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
                | WHERE _fork == "fork1" OR _fork == "fork2"
                | DROP _fork
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("first_name"));
        var limit = as(project.child(), Limit.class);
        var fork = as(limit.child(), Fork.class);
        assertThat(fork.output(), hasSize(1));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toList()), equalTo(List.of("first_name")));
        for (LogicalPlan branch : fork.children()) {
            var branchProject = as(branch, Project.class);
            assertThat(branchProject.projections().size(), equalTo(1));
            assertThat(Expressions.names(branchProject.projections()), equalTo(List.of("first_name")));
            var branchLimit = as(branchProject.child(), Limit.class);
            var relation = as(branchLimit.child(), EsRelation.class);
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
            var topNBranch = as(branchProject.child(), TopN.class);
            var branchEval = as(topNBranch.child(), Eval.class);
            var forkAlias = as(branchEval.fields().getFirst(), Alias.class);
            assertThat(forkAlias.name(), equalTo("_fork"));
            var grok = as(branchEval.child(), Grok.class);
            assertThat(grok.extractedFields().size(), equalTo(3));
            assertThat(Expressions.names(grok.extractedFields()), containsInAnyOrder("x", "y", "z"));
            var evalInBranch = as(grok.child(), Eval.class);
            var aliasInBranch = as(evalInBranch.fields().getFirst(), Alias.class);
            // The EVAL that created 'a' or 'b' should still be present in the branch even if not part of the output
            assertThat(aliasInBranch.name(), anyOf(equalTo("a"), equalTo("b")));
            var filter = as(evalInBranch.child(), Filter.class);
            assertThat(filter.toString(), containsString("IN(10048[INTEGER],10081[INTEGER],emp_no"));
            var relation = as(filter.child(), EsRelation.class);
            assertThat(
                relation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
                hasItems("emp_no", "first_name", "last_name")
            );
        }
    }

    /**
     * {@snippet lang="text":
     * Limit[10000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count(*)#36, COUNT(emp_no{r}#109,true[BOOLEAN],PT0S[
     * TIME_DURATION]) AS d#39, MAX(_fork{r}#124,true[BOOLEAN],PT0S[TIME_DURATION]) AS m#42, COUNT(s{r}#119,true[BOOLEAN],PT0S[
     * TIME_DURATION]) AS ls#45]]
     *   \_Fork[[emp_no{r}#109, s{r}#119, _fork{r}#124]]
     *     |_Project[[emp_no{f}#46, s{r}#6, _fork{r}#7]]
     *     | \_Eval[[10[INTEGER] AS s#6, fork1[KEYWORD] AS _fork#7]]
     *     |   \_Limit[1000[INTEGER],false,false]
     *     |     \_Filter[IN(10048[INTEGER],10081[INTEGER],emp_no{f}#46)]
     *     |       \_EsRelation[employees][_meta_field{f}#52, emp_no{f}#46, first_name{f}#47, ..]
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
     * }
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
        // Dissect and Eval(a) have been pruned since x, y, z fields are not used in the final aggregation
        var evalInFirstBranch = as(firstBranchProject.child(), Eval.class);
        assertThat(Expressions.names(evalInFirstBranch.fields()), containsInAnyOrder("s", "_fork"));
        var filterInFirstBranch = as(evalInFirstBranch.child(), Filter.class);
        var firstBranchRelation = as(filterInFirstBranch.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name", "last_name")
        );

        var secondBranch = fork.children().get(1);
        var secondBranchProject = as(secondBranch, Project.class);
        assertThat(secondBranchProject.projections().size(), equalTo(3));
        // x, y, z from STATS are pruned since they are not used in the final aggregation
        var evalInSecondBranch = as(secondBranchProject.child(), Eval.class);
        var localRelation = as(evalInSecondBranch.child(), LocalRelation.class);
        assertThat(
            localRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("$$COUNT$COUNT(*)::keywo>$0")
        );

        var thirdBranch = fork.children().get(2);
        var thirdBranchProject = as(thirdBranch, Project.class);
        assertThat(thirdBranchProject.projections().size(), equalTo(3));
        assertThat(Expressions.names(thirdBranchProject.projections()), containsInAnyOrder("emp_no", "s", "_fork"));
        // x (= last_name) is pruned since it is not used in the final aggregation
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
        // x (= "abc") and y (= "aaa") are pruned since they are not used in the final aggregation
        var evalInFourthBranch = as(fourthBranchProject.child(), Eval.class);
        var filterInFourthBranch = as(evalInFourthBranch.child(), Filter.class);
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
     * {@snippet lang="text":
     * Project[[languages{r}#57]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Fork[[languages{r}#57]]
     *     |_Project[[x{r}#14 AS languages#9]]
     *     | \_Eval[[1[INTEGER] AS x#14]]
     *     |   \_Limit[1000[INTEGER],false,false]
     *     |     \_EsRelation[employees][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     *     \_Project[[languages{r}#45]]
     *       \_Eval[[null[INTEGER] AS languages#45]]
     *         \_Limit[1000[INTEGER],false,false]
     *           \_EsRelation[employees][_meta_field{f}#40, emp_no{f}#34, first_name{f}#35, ..]
     * }
     */
    public void testPruneColumnsInForkBranchesShouldKeepAliasWithSameNameAsColumn() {
        var query = """
            from employees
            | drop languages
            | fork
                (eval x = 1 | rename x as lang | rename lang as languages | eval a = "aardvark" | rename a as foo)
                (where true)
            | where _fork == "fork1" OR _fork == "fork2"
            | drop _fork
            | keep languages
            """;
        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        assertThat(project.projections().size(), equalTo(1));
        assertThat(Expressions.names(project.projections()), contains("languages"));
        var limit = as(project.child(), Limit.class);
        var fork = as(limit.child(), Fork.class);
        assertThat(fork.output(), hasSize(1));
        assertThat(fork.children(), hasSize(2));
        assertThat(fork.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("languages"));

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        assertThat(Expressions.names(firstBranchProject.projections()), equalTo(List.of("languages")));
        var firstBranchEval = as(firstBranchProject.child(), Eval.class);
        var firstBranchFields = firstBranchEval.fields();
        assertThat(Expressions.names(firstBranchFields), equalTo(List.of("x")));
        var firstBranchLimit = as(firstBranchEval.child(), Limit.class);
        var firstBranchRelation = as(firstBranchLimit.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name")
        );

        var secondBranch = fork.children().get(1);
        var secondBranchProject = as(secondBranch, Project.class);
        assertThat(Expressions.names(secondBranchProject.projections()), equalTo(List.of("languages")));
        var secondBranchEval = as(secondBranchProject.child(), Eval.class);
        var secondBranchFields = secondBranchEval.fields();
        assertThat(Expressions.names(secondBranchFields), equalTo(List.of("languages")));
        var secondBranchLimit = as(secondBranchEval.child(), Limit.class);
        var secondBranchRelation = as(secondBranchLimit.child(), EsRelation.class);
        assertThat(
            secondBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
            hasItems("emp_no", "first_name")
        );
    }

    /**
     * {@snippet lang="text":
     * Project[[_meta_field{r}#50, emp_no{r}#51, first_name{r}#52, gender{r}#53, hire_date{r}#54, job{r}#55, job.raw{r}#56, l
     * ast_name{r}#57, long_noidx{r}#58, salary{r}#59]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Fork[[_meta_field{r}#50, emp_no{r}#51, first_name{r}#52, gender{r}#53, hire_date{r}#54, job{r}#55, job.raw{r}#56, l
     * ast_name{r}#57, long_noidx{r}#58, salary{r}#59]]
     *     \_Project[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26, l
     * ast_name{f}#21, long_noidx{f}#27, salary{f}#22]]
     *       \_Limit[1000[INTEGER],false,false]
     *         \_EsRelation[employees][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }
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
        var fork = as(limit.child(), Fork.class);
        assertThat(fork.output().size(), equalTo(10));

        // second and third branches are completely removed
        assertEquals(1, fork.children().size());

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        assertThat(firstBranchProject.projections().size(), equalTo(10));
        var limitFirstBranch = as(firstBranchProject.child(), Limit.class);
        var firstBranchRelation = as(limitFirstBranch.child(), EsRelation.class);
        assertThat(
            firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()),
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
            var aggregateInBranch = as(branch, Aggregate.class);
            assertThat(aggregateInBranch.aggregates().size(), equalTo(1));
            assertThat(Expressions.names(aggregateInBranch.aggregates()), contains("a"));
            var relation = as(aggregateInBranch.child(), EsRelation.class);
            assertThat(relation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("salary"));
        }
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS y#28]]
     *   \_Fork[[]]
     *     |_Project[[]]
     *     | \_Filter[NOT(STARTSWITH(gender{f}#31,v[KEYWORD]))]
     *     |   \_Aggregate[[gender{f}#31],[gender{f}#31]]
     *     |     \_EsRelation[employees][_meta_field{f}#35, emp_no{f}#29, first_name{f}#30, ..]
     *     |_Project[[]]
     *     | \_Filter[salary{f}#45 > 100000[INTEGER]]
     *     |   \_Aggregate[[salary{f}#45],[salary{f}#45]]
     *     |     \_EsRelation[employees][_meta_field{f}#46, emp_no{f}#40, first_name{f}#41, ..]
     *     \_Project[[]]
     *       \_Filter[emp_no{f}#51 > 10000[INTEGER]]
     *         \_EsRelation[employees][_meta_field{f}#57, emp_no{f}#51, first_name{f}#52, ..]
     * }
     */
    public void testPruneColumnsInForkBranchesWithFinalCountStats() {
        var query = """
             from employees
             // reusing the same column name as in the final STATS
            | FORK ( stats salary = count(*)::int, z = count(*) by gender
                    | where NOT starts_with(gender, "v")
                    | eval gender = gender::keyword )
                   ( stats gender = values(gender), z = count(*) by salary
                    | where salary > 100000 )
                   ( where emp_no > 10000
                    | eval gender = gender::keyword )
            | sort salary
            | stats y = count(*) // this removes all existing columns
            """;

        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(1));
        assertThat(Expressions.names(aggregate.aggregates()), contains("y"));
        var fork = as(aggregate.child(), Fork.class);

        assertThat(fork.output().size(), equalTo(0));
        assertThat(fork.children().size(), equalTo(3));

        var firstBranch = fork.children().getFirst();
        var firstBranchProject = as(firstBranch, Project.class);
        var firstBranchFilter = as(firstBranchProject.child(), Filter.class);
        var firstBranchAggregate = as(firstBranchFilter.child(), Aggregate.class);
        assertThat(firstBranchAggregate.aggregates().size(), equalTo(1));
        assertThat(Expressions.names(firstBranchAggregate.aggregates()), contains("gender"));
        var firstBranchRelation = as(firstBranchAggregate.child(), EsRelation.class);
        assertThat(firstBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("gender"));

        var secondBranch = fork.children().get(1);
        var secondBranchProject = as(secondBranch, Project.class);
        var secondBranchFilter = as(secondBranchProject.child(), Filter.class);
        var secondBranchAggregate = as(secondBranchFilter.child(), Aggregate.class);
        assertThat(secondBranchAggregate.aggregates().size(), equalTo(1));
        assertThat(Expressions.names(secondBranchAggregate.aggregates()), contains("salary"));
        var secondBranchRelation = as(secondBranchAggregate.child(), EsRelation.class);
        assertThat(secondBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("salary"));

        var thirdBranch = fork.children().get(2);
        var thirdBranchProject = as(thirdBranch, Project.class);
        var thirdBranchFilter = as(thirdBranchProject.child(), Filter.class);
        var thirdBranchRelation = as(thirdBranchFilter.child(), EsRelation.class);
        assertThat(thirdBranchRelation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("emp_no", "gender"));
    }

    /**
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS y#12]]
     *   \_Fork[[]]
     *     \_Project[[]]
     *       \_Filter[emp_no{f}#13 > 10000[INTEGER]]
     *         \_Aggregate[[emp_no{f}#13],[emp_no{f}#13]]
     *           \_EsRelation[employees][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     * }
     */
    public void testPruneColumnsInForkWithSingleBranchAndFinalStats() {
        var query = """
                 from employees
                | stats x = count(*), y = count(*) by emp_no
                | FORK (where emp_no > 10000)
                | sort x
                | stats y = count(*)
            """;

        var plan = optimizedPlan(query);
        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(aggregate.aggregates().size(), equalTo(1));
        assertThat(Expressions.names(aggregate.aggregates()), contains("y"));
        var fork = as(aggregate.child(), Fork.class);
        assertThat(fork.output().size(), equalTo(0));
        assertThat(fork.children().size(), equalTo(1));

        var project = as(fork.children().getFirst(), Project.class);
        assertThat(project.projections().size(), equalTo(0));
        var filter = as(project.child(), Filter.class);
        var branchAggregate = as(filter.child(), Aggregate.class);
        assertThat(branchAggregate.aggregates().size(), equalTo(1));
        assertThat(Expressions.names(branchAggregate.aggregates()), contains("emp_no"));
        var relation = as(branchAggregate.child(), EsRelation.class);
        assertThat(relation.output().stream().map(Attribute::name).collect(Collectors.toSet()), hasItems("emp_no"));
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

        var analyzedPlan = unionIndexAnalyzer().query(query);

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

        var analyzedPlan = unionIndexAnalyzer().query(query);

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
     * \_Project[[id{f}#14]]
     *   \_Rerank[reranking-inference-id[KEYWORD],1000[INTEGER],test[KEYWORD],[x{r}#5 AS x#7],_score{m}#16]
     *     \_Project[[id{f}#14, $$languages$converted_to$keyword{f$}#15, $$languages$converted_to$keyword{f$}#15 AS x#5]]
     *       \_EsRelation[union_types_index*][!first_name, id{f}#14, !languages, !last_name, !sal..]
     */
    public void testPruneColumnsInProject_WithGeneratingPlan_Rerank() {
        var query = """
            from union_types_index*
            | eval x = languages::string
            | rerank "test" ON x WITH { "inference_id" : "reranking-inference-id" }
            | keep id
            """;

        var analyzedPlan = unionIndexAnalyzer().query(query);

        // run through this rule only because this is the one which enables the project pruning in this case and it's
        // how PruneColumns does its thing in case of Project prunning
        var prunedEval = new ReplaceAliasingEvalWithProject().apply(analyzedPlan);

        /*
         * Limit[1000[INTEGER],false,false]
         * \_Project[[id{f}#13]]
         *   \_Rerank[reranking-inference-id[KEYWORD],1000[INTEGER],test[KEYWORD],[x{r}#5 AS x#7],_score{m}#16]
         *     \_Project[[!first_name, id{f}#13, !languages, !last_name, !salary_change, $$languages$converted_to$keyword{f$}#15, $$lan
         * guages$converted_to$keyword{f$}#15 AS x#5]]
         *       \_EsRelation[union_types_index*][!first_name, id{f}#13, !languages, !last_name, !sal..]
         */
        Limit topLimit = as(prunedEval, Limit.class);
        Project topProject = as(topLimit.child(), Project.class);
        assertThat(topProject.projections().size(), equalTo(1));
        assertThat(Expressions.names(topProject.projections()), contains("id"));
        Rerank rerank = as(topProject.child(), Rerank.class);
        Project project = as(rerank.child(), Project.class);
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
        rerank = as(topProject.child(), Rerank.class);
        project = as(rerank.child(), Project.class);
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

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[date{r}#60]]
     * \_Dissect[message{r}#59,Parser[pattern=%{date} - %{level} - %{ip}, appendSeparator=, parser=org.elasticsearch.dissect.Dis
     * sectParser@981fdf8],[date{r}#60, level{r}#61, ip{r}#62]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_LocalRelation[[message{r}#59],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     * 2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 2d 20 65 72 72 6f 72 20 2d 20 31 39 32 2e 31 36 38 2e 31 2e 31]]]]}]
     * }
     */
    public void testCannotPartiallyPruneDissectFields() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z - error - 192.168.1.1"
            | DISSECT message "%{date} - %{level} - %{ip}"
            | KEEP date
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("date")));
        var dissect = as(project.child(), Dissect.class);
        // partial pruning is not supported, all fields remain
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("date", "level", "ip")));
        var limit = as(dissect.child(), Limit.class);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[message{r}#46]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_LocalRelation[[message{r}#46],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=...]]]}]
     * }
     */
    public void testPruneAllDissectFields() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z - error - 192.168.1.1"
            | DISSECT message "%{date} - %{level} - %{ip}"
            | KEEP message
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("message"));
        var limit = as(project.child(), Limit.class);
        // Dissect is completely removed since 'date', 'level' and 'ip' are not used
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[extracted_last{r}#19],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count#23, extracted_last{r}#19]]
     *   \_Dissect[full_name{r}#17,Parser[pattern=%{extracted_first} %{extracted_last}, appendSeparator=, parser=org.elasticsearch
     * .dissect.DissectParser@43bbc401],[extracted_first{r}#18, extracted_last{r}#19]]
     *     \_Eval[[CONCAT(first_name{f}#25, [KEYWORD],last_name{f}#28) AS full_name#17]]
     *       \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     * }
     */
    public void testCannotPartiallyPruneDissectFieldsInAgg() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | STATS count = COUNT(*) BY extracted_last
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), is(List.of("count", "extracted_last")));
        var dissect = as(agg.child(), Dissect.class);
        // partial pruning is not supported, all fields remain
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("extracted_first", "extracted_last")));
        var eval = as(dissect.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("full_name")));
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[date{r}#99]]
     * \_Grok[message{r}#93,Parser[pattern=%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}, grok=o
     * rg.elasticsearch.grok.Grok@2991a7bb],[date{r}#99, email{r}#100, ip{r}#101, num{r}#102]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_LocalRelation[[message{r}#93],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     * 2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 31 39 32 2e 31 36 38 2e 31 2e 31 20 75 73 65 72 40 65 78 61 6d 70 6c 65 2e 63 6f
     * 6d 20 34 32]]]]}]
     * }
     */
    public void testCannotPartiallyPruneGrokFields() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z 192.168.1.1 user@example.com 42"
            | GROK message "%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"
            | KEEP date
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("date")));
        var grok = as(project.child(), Grok.class);
        // partial pruning is not supported, all fields remain
        assertThat(Expressions.names(grok.extractedFields()), is(List.of("date", "email", "ip", "num")));
        var limit = as(grok.child(), Limit.class);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Project[[message{r}#53]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_LocalRelation[[message{r}#53],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=...]]]}]
     * }
     */
    public void testPruneAllGrokFields() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z 192.168.1.1 user@example.com 42"
            | GROK message "%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"
            | KEEP message
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("message"));
        var limit = as(project.child(), Limit.class);
        // Grok is completely removed since 'date', 'ip', 'email' and 'num' are not used
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[extracted_last{r}#43],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count#46, extracted_last{r}#43]]
     *   \_Grok[full_name{r}#38,Parser[pattern=%{WORD:extracted_first} %{WORD:extracted_last}, grok=org.elasticsearch.grok.Grok
     * @1df7d04d],[extracted_first{r}#42, extracted_last{r}#43]]
     *     \_Eval[[CONCAT(first_name{f}#48, [KEYWORD],last_name{f}#51) AS full_name#38]]
     *       \_EsRelation[test][_meta_field{f}#53, emp_no{f}#47, first_name{f}#48, ..]
     * }
     */
    public void testCannotPartiallyPruneGrokFieldsInAgg() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | GROK full_name "%{WORD:extracted_first} %{WORD:extracted_last}"
            | STATS count = COUNT(*) BY extracted_last
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), is(List.of("count", "extracted_last")));
        var grok = as(agg.child(), Grok.class);
        // partial pruning is not supported, all fields remain
        assertThat(Expressions.names(grok.extractedFields()), is(List.of("extracted_first", "extracted_last")));
        var eval = as(grok.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("full_name")));
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[_meta_field{f}#300, emp_no{f}#294, first_name{f}#295, gender{f}#296, hire_date{f}#301, job{f}#302, job.raw{f}
     * #303, languages{f}#297, last_name{f}#298, long_noidx{f}#304, salary{f}#299]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#300, emp_no{f}#294, first_name{f}#29..]
     * }
     */
    public void testPruneDissectFieldsViaDrop() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | DROP extracted_first, extracted_last, full_name
            """);

        var project = as(plan, Project.class);
        var projections = Expressions.names(project.projections());
        assertThat(projections.contains("extracted_first"), is(false));
        assertThat(projections.contains("extracted_last"), is(false));
        assertThat(projections.contains("full_name"), is(false));
        var limit = as(project.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[_meta_field{f}#265, emp_no{f}#259, first_name{f}#260, gender{f}#261, hire_date{f}#266, job{f}#267, job.raw{f}
     * #268, languages{f}#262, last_name{f}#263, long_noidx{f}#269, salary{f}#264]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#265, emp_no{f}#259, first_name{f}#26..]
     * }
     */
    public void testPruneGrokFieldsViaDrop() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | GROK full_name "%{WORD:extracted_first} %{WORD:extracted_last}"
            | DROP extracted_first, extracted_last, full_name
            """);

        var project = as(plan, Project.class);
        var projections = Expressions.names(project.projections());
        assertThat(projections.contains("extracted_first"), is(false));
        assertThat(projections.contains("extracted_last"), is(false));
        assertThat(projections.contains("full_name"), is(false));
        var limit = as(project.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[message{r}#142, date{r}#143]]
     * \_Dissect[message{r}#142,Parser[pattern=%{date} - %{level} - %{ip}, appendSeparator=, parser=org.elasticsearch.dissect.Di
     * ssectParser@223d640a],[date{r}#143, level{r}#144, ip{r}#145]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_LocalRelation[[message{r}#142],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     *  2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 2d 20 65 72 72 6f 72 20 2d 20 31 39 32 2e 31 36 38 2e 31 2e 31]]]]}]
     * }
     */
    public void testCannotPartiallyPruneDissectFieldsViaDrop() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z - error - 192.168.1.1"
            | DISSECT message "%{date} - %{level} - %{ip}"
            | DROP level, ip
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("message", "date")));
        var dissect = as(project.child(), Dissect.class);
        // partial pruning is not supported, all fields remain
        assertThat(dissect.extractedFields(), hasSize(3));
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("date", "level", "ip")));
        var limit = as(dissect.child(), Limit.class);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[message{r}#77, date{r}#83, level{r}#85, ip{r}#87]]
     * \_Eval[[overwritten[KEYWORD] AS date#83, overwritten[KEYWORD] AS level#85, overwritten[KEYWORD] AS ip#87]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_LocalRelation[[message{r}#77],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     * 2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 2d 20 65 72 72 6f 72 20 2d 20 31 39 32 2e 31 36 38 2e 31 2e 31]]]]}]
     * }
     */
    public void testPruneDissectFieldsShadowedByEval() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z - error - 192.168.1.1"
            | DISSECT message "%{date} - %{level} - %{ip}"
            | EVAL date = "overwritten", level = "overwritten", ip = "overwritten"
            | KEEP message, date, level, ip
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(4));
        assertThat(Expressions.names(project.projections()), contains("message", "date", "level", "ip"));
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(3));
        assertThat(Expressions.names(eval.fields()), contains("date", "level", "ip"));
        var limit = as(eval.child(), Limit.class);
        // Dissect is completely removed since all its fields are overwritten by EVAL
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[message{r}#227, date{r}#238]]
     * \_Eval[[overwritten[KEYWORD] AS date#238]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_LocalRelation[[message{r}#227],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     *  2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 31 39 32 2e 31 36 38 2e 31 2e 31 20 75 73 65 72 40 65 78 61 6d 70 6c 65 2e 63 6f
     *  6d 20 34 32]]]]}]
     * }
     */
    public void testPruneGrokFieldsShadowedByEval() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z 192.168.1.1 user@example.com 42"
            | GROK message "%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"
            | EVAL date = "overwritten", ip = "overwritten", email = "overwritten", num = 0
            | KEEP message, date
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), contains("message", "date"));
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        assertThat(Expressions.names(eval.fields()), contains("date"));
        var limit = as(eval.child(), Limit.class);
        // Grok is completely removed since all its fields are overwritten by EVAL
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[first_name{f}#117 AS extracted_first#113, extracted_last{r}#109]]
     * \_Dissect[full_name{r}#107,Parser[pattern=%{extracted_first} %{extracted_last}, appendSeparator=, parser=org.elasticsearc
     * h.dissect.DissectParser@4eafab7f],[extracted_first{r}#108, extracted_last{r}#109]]
     *   \_Eval[[CONCAT(first_name{f}#117, [KEYWORD],last_name{f}#120) AS full_name#107]]
     *     \_Limit[1000[INTEGER],false,false]
     *       \_EsRelation[test][_meta_field{f}#122, emp_no{f}#116, first_name{f}#11..]
     * }
     */
    public void testCannotPartiallyPruneDissectFieldShadowedByRename() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | RENAME first_name AS extracted_first
            | KEEP extracted_first, extracted_last
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("extracted_first", "extracted_last")));
        // partial pruning is not supported, both extracted fields remain; extracted_first from dissect is shadowed by RENAME
        var dissect = as(project.child(), Dissect.class);
        assertThat(dissect.extractedFields(), hasSize(2));
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("extracted_first", "extracted_last")));
        var eval = as(dissect.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("full_name")));
        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[emp_no{f}#79, avg_salary{r}#74]]
     * \_TopN[[Order[emp_no{f}#79,ASC,LAST]],5[INTEGER],false]
     *   \_InlineJoin[LEFT,[extracted_last{r}#70],[extracted_last{r}#70]]
     *     |_Dissect[full_name{r}#68,Parser[pattern=%{extracted_first} %{extracted_last}, appendSeparator=, parser=org.elasticsearch
     * .dissect.DissectParser@4d4b3df6],[extracted_first{r}#69, extracted_last{r}#70]]
     *     | \_Eval[[CONCAT(first_name{f}#80, [KEYWORD],last_name{f}#83) AS full_name#68]]
     *     |   \_EsRelation[employees][_meta_field{f}#85, emp_no{f}#79, first_name{f}#80, ..]
     *     \_Project[[avg_salary{r}#74, extracted_last{r}#70]]
     *       \_Eval[[$$SUM$avg_salary$0{r$}#90 / $$COUNT$avg_salary$1{r$}#91 AS avg_salary#74]]
     *         \_Aggregate[[extracted_last{r}#70],[SUM(salary{f}#84,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg_s
     * alary$0#90, COUNT(salary{f}#84,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg_salary$1#91, extracted_last{r}#70]]
     *           \_StubRelation[[_meta_field{f}#85, emp_no{f}#79, first_name{f}#80, gender{f}#81, hire_date{f}#86, job{f}#87, job.raw{f}#88,
     * languages{f}#82, last_name{f}#83, long_noidx{f}#89, salary{f}#84, full_name{r}#68, extracted_first{r}#69, extracted_last{r}#70]]
     * }
     */
    public void testCannotPartiallyPruneDissectWithInlineStats() {
        var query = """
            FROM employees
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | INLINE STATS avg_salary = AVG(salary) BY extracted_last
            | KEEP emp_no, avg_salary
            | SORT emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "avg_salary")));
        var topN = as(project.child(), TopN.class);
        var inlineJoin = as(topN.child(), InlineJoin.class);
        // Left side: partial pruning is not supported, both extracted fields remain
        var dissect = as(inlineJoin.left(), Dissect.class);
        assertThat(dissect.extractedFields(), hasSize(2));
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("extracted_first", "extracted_last")));
        var eval = as(dissect.child(), Eval.class);
        as(eval.child(), EsRelation.class);
        // Right side: aggregation
        var rightProject = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(rightProject.projections()), is(List.of("avg_salary", "extracted_last")));
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[emp_no{f}#63, emp_no{f}#63 AS avg_salary#59]]
     * \_TopN[[Order[emp_no{f}#63,ASC,LAST]],5[INTEGER],false]
     *   \_EsRelation[employees][_meta_field{f}#69, emp_no{f}#63, first_name{f}#64, ..]
     * }
     */
    public void testPruneGrokWithInlineStatsPrunedEntirely() {
        var query = """
            FROM employees
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | GROK full_name "%{WORD:extracted_first} %{WORD:extracted_last}"
            | INLINE STATS avg_salary = AVG(salary) BY extracted_last
            | EVAL avg_salary = emp_no
            | KEEP emp_no, avg_salary
            | SORT emp_no
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "avg_salary")));
        // Both inline stats and grok are fully pruned since avg_salary is overwritten with emp_no
        var topN = as(project.child(), TopN.class);
        as(topN.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[emp_no{f}#108, language_name{f}#120]]
     * \_Limit[1000[INTEGER],true,false]
     *   \_Join[LEFT,[languages{f}#111],[language_code{f}#119],null]
     *     |_Limit[1000[INTEGER],false,false]
     *     | \_EsRelation[test][_meta_field{f}#114, emp_no{f}#108, first_name{f}#10..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#119, language_name{f}#120]
     * }
     */
    public void testPruneDissectWithLookupJoin() {
        var plan = plan("""
            FROM test
            | EVAL language_code = languages
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | LOOKUP JOIN languages_lookup ON language_code
            | DROP extracted_first, extracted_last, full_name
            | KEEP emp_no, language_name
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), contains("emp_no", "language_name"));
        var limit = as(project.child(), Limit.class);
        var join = as(limit.child(), Join.class);
        // Dissect is fully pruned since extracted_first and extracted_last are DROPped
        var leftLimit = as(join.left(), Limit.class);
        as(leftLimit.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[emp_no{f}#31, language_name{f}#43]]
     * \_Limit[1000[INTEGER],true,false]
     *   \_Join[LEFT,[languages{f}#34],[language_code{f}#42],null]
     *     |_Limit[1000[INTEGER],false,false]
     *     | \_EsRelation[test][_meta_field{f}#37, emp_no{f}#31, first_name{f}#32, ..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#42, language_name{f}#43]
     * }
     */
    public void testPruneGrokWithLookupJoin() {
        var plan = plan("""
            FROM test
            | EVAL language_code = languages
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | GROK full_name "%{WORD:extracted_first} %{WORD:extracted_last}"
            | LOOKUP JOIN languages_lookup ON language_code
            | DROP extracted_first, extracted_last, full_name
            | KEEP emp_no, language_name
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), contains("emp_no", "language_name"));
        var limit = as(project.child(), Limit.class);
        var join = as(limit.child(), Join.class);
        // Grok is fully pruned since extracted_first and extracted_last are DROPped
        var leftLimit = as(join.left(), Limit.class);
        as(leftLimit.child(), EsRelation.class);
        as(join.right(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[message{r}#122]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_LocalRelation[[message{r}#122],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     *  2d 30 31 2d 32 33 20 65 72 72 6f 72 20 31 39 32 2e 31 36 38 2e 31 2e 31]]]]}]
     * }
     */
    public void testPruneChainedDissectAndGrok() {
        var plan = plan("""
            ROW message = "2023-01-23 error 192.168.1.1"
            | DISSECT message "%{date} %{rest}"
            | GROK rest "%{WORD:level} %{IP:ip}"
            | KEEP message
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.names(project.projections()), contains("message"));
        var limit = as(project.child(), Limit.class);
        // Both dissect and grok are removed
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[level{r}#12]]
     * \_Grok[rest{r}#6,Parser[pattern=%{WORD:level} %{IP:ip}, grok=org.elasticsearch.grok.Grok@883ecd6],[ip{r}#11, level{r}#1
     * 2]]
     *   \_Dissect[message{r}#4,Parser[pattern=%{date} %{rest}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@4
     * 814ecff],[date{r}#5, rest{r}#6]]
     *     \_Limit[1000[INTEGER],false,false]
     *       \_LocalRelation[[message{r}#4],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     *  2d 30 31 2d 32 33 20 65 72 72 6f 72 20 31 39 32 2e 31 36 38 2e 31 2e 31]]]]}]
     * }
     */
    public void testCannotPartiallyPruneChainedDissectAndGrok() {
        var plan = plan("""
            ROW message = "2023-01-23 error 192.168.1.1"
            | DISSECT message "%{date} %{rest}"
            | GROK rest "%{WORD:level} %{IP:ip}"
            | KEEP level
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.names(project.projections()), is(List.of("level")));
        var grok = as(project.child(), Grok.class);
        // partial pruning is not supported, all fields remain
        assertThat(grok.extractedFields(), hasSize(2));
        assertThat(Expressions.names(grok.extractedFields()), is(List.of("ip", "level")));
        var dissect = as(grok.child(), Dissect.class);
        // partial pruning is not supported, all fields remain
        assertThat(dissect.extractedFields(), hasSize(2));
        assertThat(Expressions.names(dissect.extractedFields()), is(List.of("date", "rest")));
        var limit = as(dissect.child(), Limit.class);
        as(limit.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[extracted_first{r}#163],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS count#168, extracted_first{r}#
     * 163]]
     *   \_Eval[[constant[KEYWORD] AS extracted_first#163]]
     *     \_EsRelation[test][_meta_field{f}#175, emp_no{f}#169, first_name{f}#17..]
     * }
     */
    public void testPruneDissectFieldRedefinedBeforeStats() {
        var plan = plan("""
            FROM test
            | EVAL full_name = CONCAT(first_name, " ", last_name)
            | DISSECT full_name "%{extracted_first} %{extracted_last}"
            | EVAL extracted_first = "constant", extracted_last = "constant"
            | STATS count = COUNT(*) BY extracted_first
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("count", "extracted_first"));
        // Dissect is fully pruned since extracted_first/extracted_last are overwritten by EVAL
        var eval = as(agg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("extracted_first"));
        // Dissect is fully pruned — next node is EsRelation, no Dissect in the tree
        as(eval.child(), EsRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[message{r}#128, ip{r}#136]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_Filter[ip{r}#136 == 192.168.1.1[KEYWORD]]
     *     \_Grok[message{r}#128,Parser[pattern=%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}, grok=
     * org.elasticsearch.grok.Grok@7c72a4af],[date{r}#134, email{r}#135, ip{r}#136, num{r}#137]]
     *       \_LocalRelation[[message{r}#128],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[32 30 32 33
     *  2d 30 31 2d 32 33 54 31 32 3a 31 35 3a 30 30 5a 20 31 39 32 2e 31 36 38 2e 31 2e 31 20 75 73 65 72 40 65 78 61 6d 70 6c 65 2e 63 6f
     *  6d 20 34 32]]]]}]
     * }
     */
    public void testCannotPartiallyPruneGrokFieldsUsedInFilter() {
        var plan = plan("""
            ROW message = "2023-01-23T12:15:00Z 192.168.1.1 user@example.com 42"
            | GROK message "%{TIMESTAMP_ISO8601:date} %{IP:ip} %{EMAILADDRESS:email} %{NUMBER:num:int}"
            | WHERE ip == "192.168.1.1"
            | KEEP message, ip
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(2));
        assertThat(Expressions.names(project.projections()), is(List.of("message", "ip")));
        var limit = as(project.child(), Limit.class);
        var filter = as(limit.child(), Filter.class);
        var grok = as(filter.child(), Grok.class);
        // partial pruning is not supported, all fields remain even though only 'ip' is used
        assertThat(grok.extractedFields(), hasSize(4));
        assertThat(Expressions.names(grok.extractedFields()), is(List.of("date", "email", "ip", "num")));
        as(grok.child(), LocalRelation.class);
    }

    /**
     * Expects
     *
     * {@snippet lang="text":
     * Project[[combined{r}#11]]
     * \_Eval[[CONCAT(a{r}#5,-[KEYWORD],b{r}#6) AS combined#11]]
     *   \_Dissect[message{r}#4,Parser[pattern=%{a} %{b}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@63bbe42
     * c],[a{r}#5, b{r}#6]]
     *     \_Limit[1000[INTEGER],false,false]
     *       \_LocalRelation[[message{r}#4],Page{blocks=[BytesRefVectorBlock[vector=ConstantBytesRefVector[positions=1, value=[68 65 6c 6c 6
     * f 20 77 6f 72 6c 64]]]]}]
     * }
     */
    public void testNoPruneDissectFieldsUsedInEvalExpression() {
        var plan = plan("""
            ROW message = "hello world"
            | DISSECT message "%{a} %{b}"
            | EVAL combined = CONCAT(a, "-", b)
            | KEEP combined
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(1));
        assertThat(Expressions.names(project.projections()), contains("combined"));
        var eval = as(project.child(), Eval.class);
        var dissect = as(eval.child(), Dissect.class);
        // Both a and b survive because they're referenced in the EVAL expression
        assertThat(dissect.extractedFields(), hasSize(2));
        assertThat(
            dissect.extractedFields(),
            containsIgnoringIds(new ReferenceAttribute(EMPTY, "a", KEYWORD), new ReferenceAttribute(EMPTY, "b", KEYWORD))
        );
        var limit = as(dissect.child(), Limit.class);
        as(limit.child(), LocalRelation.class);
    }

    // === ExternalRelation pruning tests ===

    public void testPruneColumnsInExternalRelation() {
        Attribute colA = extAttr("col_a", KEYWORD);
        Attribute colB = extAttr("col_b", LONG);
        Attribute colC = extAttr("col_c", INTEGER);
        ExternalRelation ext = externalRelation(List.of(colA, colB, colC));

        // Project only col_a — col_b and col_c should be pruned
        LogicalPlan plan = new Project(EMPTY, ext, List.of(colA));
        LogicalPlan result = new PruneColumns().apply(plan);

        var project = as(result, Project.class);
        var prunedExt = as(project.child(), ExternalRelation.class);
        assertThat(prunedExt.output(), hasSize(1));
        assertThat(Expressions.names(prunedExt.output()), contains("col_a"));
    }

    public void testNoPruningWhenAllColumnsUsedInExternalRelation() {
        Attribute colA = extAttr("col_a", KEYWORD);
        Attribute colB = extAttr("col_b", LONG);
        ExternalRelation ext = externalRelation(List.of(colA, colB));

        // Project all columns — nothing should be pruned
        LogicalPlan plan = new Project(EMPTY, ext, List.of(colA, colB));
        LogicalPlan result = new PruneColumns().apply(plan);

        var project = as(result, Project.class);
        var prunedExt = as(project.child(), ExternalRelation.class);
        assertThat(prunedExt.output(), hasSize(2));
        assertThat(Expressions.names(prunedExt.output()), contains("col_a", "col_b"));
    }

    public void testPruneColumnsInExternalRelationThroughEval() {
        Attribute colA = extAttr("col_a", LONG);
        Attribute colB = extAttr("col_b", LONG);
        ExternalRelation ext = externalRelation(List.of(colA, colB));

        // Eval references col_a, Project keeps only the computed field — col_b should be pruned
        Alias computed = new Alias(EMPTY, "computed", colA);
        Eval eval = new Eval(EMPTY, ext, List.of(computed));
        LogicalPlan plan = new Project(EMPTY, eval, List.of(computed.toAttribute()));
        LogicalPlan result = new PruneColumns().apply(plan);

        var project = as(result, Project.class);
        var resultEval = as(project.child(), Eval.class);
        var prunedExt = as(resultEval.child(), ExternalRelation.class);
        assertThat(prunedExt.output(), hasSize(1));
        assertThat(Expressions.names(prunedExt.output()), contains("col_a"));
    }

    public void testPruneColumnsInExternalRelationWithFilter() {
        Attribute colA = extAttr("col_a", KEYWORD);
        Attribute colB = extAttr("col_b", LONG);
        Attribute colC = extAttr("col_c", INTEGER);
        ExternalRelation ext = externalRelation(List.of(colA, colB, colC));

        // Filter references col_b, Project keeps col_a — both col_a and col_b should be retained, col_c pruned
        var condition = new GreaterThan(EMPTY, colB, new Literal(EMPTY, 10L, LONG), null);
        Filter filter = new Filter(EMPTY, ext, condition);
        LogicalPlan plan = new Project(EMPTY, filter, List.of(colA));
        LogicalPlan result = new PruneColumns().apply(plan);

        var project = as(result, Project.class);
        var resultFilter = as(project.child(), Filter.class);
        var prunedExt = as(resultFilter.child(), ExternalRelation.class);
        assertThat(prunedExt.output(), hasSize(2));
        assertThat(Expressions.names(prunedExt.output()), contains("col_a", "col_b"));
    }

    /**
     * Ensures that PruneColumns does not drop a column used by LIMIT BY even when a subsequent DROP removes it from the output.
     * <pre>{@code
     * Project[[emp_no, first_name, ...excluding x]]
     * \_LimitBy[1,[x]]
     *   \_Eval[[salary + 4 AS x]]
     *     \_Limit[1000]
     *       \_EsRelation[test]
     * }</pre>
     */
    public void testPruneColumnsKeepsLimitByGrouping() {
        var plan = plan("""
            from test
            | eval x = salary + 4
            | limit 1 by x
            | drop x
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), org.hamcrest.Matchers.not(hasItems("x")));
        var defaultLimit = as(project.child(), Limit.class);
        var limitBy = as(defaultLimit.child(), LimitBy.class);
        assertThat(Expressions.names(limitBy.groupings()), contains("x"));
        var eval = as(limitBy.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), contains("x"));
    }

    private static Attribute extAttr(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalRelation externalRelation(List<Attribute> attributes) {
        SourceMetadata metadata = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return attributes;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/data.parquet";
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof SourceMetadata;
            }

            @Override
            public int hashCode() {
                return 1;
            }
        };
        return new ExternalRelation(EMPTY, "s3://bucket/data.parquet", metadata, attributes);
    }

    /**
     * PruneColumns must not descend into MetricsInfo children. MetricsInfo.computeReferences()
     * returns EMPTY, so without the skip the rule would prune every projection below it to
     * Project[[]], breaking the data pipeline on data nodes.
     */
    public void testPruneColumnsSkipsMetricsInfo() {
        FieldAttribute cpuField = new FieldAttribute(
            EMPTY,
            "cpu",
            new EsField("cpu", LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        EsRelation esRelation = new EsRelation(
            EMPTY,
            "k8s",
            IndexMode.TIME_SERIES,
            Map.of(),
            Map.of(),
            Map.of("k8s", IndexMode.TIME_SERIES),
            List.of(cpuField)
        );
        Project project = new Project(EMPTY, esRelation, List.of(cpuField));
        MetricsInfo metricsInfo = new MetricsInfo(EMPTY, project);

        LogicalPlan result = new PruneColumns().apply(metricsInfo);

        MetricsInfo resultMetricsInfo = as(result, MetricsInfo.class);
        Project resultProject = as(resultMetricsInfo.child(), Project.class);
        assertThat(resultProject.projections(), hasSize(1));
        assertThat(Expressions.names(resultProject.projections()), contains("cpu"));
    }

    /**
     * Same as {@link #testPruneColumnsSkipsMetricsInfo()} but for TsInfo.
     */
    public void testPruneColumnsSkipsTsInfo() {
        FieldAttribute cpuField = new FieldAttribute(
            EMPTY,
            "cpu",
            new EsField("cpu", LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        EsRelation esRelation = new EsRelation(
            EMPTY,
            "k8s",
            IndexMode.TIME_SERIES,
            Map.of(),
            Map.of(),
            Map.of("k8s", IndexMode.TIME_SERIES),
            List.of(cpuField)
        );
        Project project = new Project(EMPTY, esRelation, List.of(cpuField));
        TsInfo tsInfo = new TsInfo(EMPTY, project);

        LogicalPlan result = new PruneColumns().apply(tsInfo);

        TsInfo resultTsInfo = as(result, TsInfo.class);
        Project resultProject = as(resultTsInfo.child(), Project.class);
        assertThat(resultProject.projections(), hasSize(1));
        assertThat(Expressions.names(resultProject.projections()), contains("cpu"));
    }

}
