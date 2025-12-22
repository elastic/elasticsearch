/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests.releaseBuildForInlineStats;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class HoistOrderByBeforeInlineJoinOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    /*
     * EsqlProject[[emp_no{f}#12, avg{r}#6, languages{f}#15, gender{f}#14]]
     * \_TopN[[Order[emp_no{f}#12,ASC,LAST]],5[INTEGER],false]
     *   \_InlineJoin[LEFT,[languages{f}#15],[languages{r}#15]]
     *     |_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *     \_Project[[avg{r}#6, languages{f}#15]]
     *       \_Eval[[$$SUM$avg$0{r$}#23 / $$COUNT$avg$1{r$}#24 AS avg#6]]
     *         \_Aggregate[[languages{f}#15],[SUM(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#23,
     *              COUNT(salary{f}#17,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#24, languages{f}#15]]
     *           \_StubRelation[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20,
     *                  job.raw{f}#21, languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17]]
     */
    public void testInlineStatsAfterSortAndBeforeLimit() {
        var query = """
            FROM employees
            | SORT emp_no
            | INLINE STATS avg = AVG(salary) BY languages
            | LIMIT 5
            | KEEP emp_no, avg, languages, gender
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var esqlProject = as(plan, EsqlProject.class);

        var topN = as(esqlProject.child(), TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), equalTo("emp_no"));
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("languages")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));
        // Right
        var project = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "languages")));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "languages")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * TopN[[Order[emp_no{f}#9,DESC,FIRST]],1000[INTEGER],false]
     * \_InlineJoin[LEFT,[emp_no{f}#9],[emp_no{r}#9]]
     *   |_Filter[emp_no{f}#9 > 1000[INTEGER]]
     *   | \_EsRelation[employees][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *   \_Project[[avg{r}#6, emp_no{f}#9]]
     *     \_Eval[[$$SUM$avg$0{r$}#20 / $$COUNT$avg$1{r$}#21 AS avg#6]]
     *       \_Aggregate[[emp_no{f}#9],[SUM(salary{f}#14,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#20,
     *              COUNT(salary{f}#14,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#21, emp_no{f}#9]]
     *         \_StubRelation[[_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, gender{f}#11, hire_date{f}#16, job{f}#17, job.raw{f}#18,
     *              languages{f}#12, last_name{f}#13, long_noidx{f}#19, salary{f}#14]]
     */
    public void testInlineStatsAfterSort() {
        var query = """
            FROM employees
            | SORT emp_no DESC
            | INLINE STATS avg = AVG(salary) BY emp_no
            | WHERE emp_no > 1000 // to avoid an existing issue with LIMIT injection past INLINESTATS (which masks the issue the test tests)
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var topN = as(plan, TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), equalTo("emp_no"));
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));

        // Left side of the join
        var filter = as(inlineJoin.left(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));
        var relation = as(filter.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var project = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "emp_no")));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * TopN[[Order[emp_no{f}#17,DESC,FIRST]],1000[INTEGER],false]
     * \_InlineJoin[LEFT,[emp_no{f}#17],[emp_no{r}#17]]
     *   |_EsqlProject[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *          languages{f}#20, last_name{f}#21 AS lName#11, long_noidx{f}#27, salary{f}#22, msg{r}#4, salaryK{r}#8]]
     *   | \_Eval[[salary{f}#22 / 1000[INTEGER] AS salaryK#8]]
     *   |   \_Dissect[first_name{f}#18,Parser[pattern=%{msg}, appendSeparator=, parser=org.elasticsearch.dissect.DissectParser@3f4941c9],
     *              [msg{r}#4]]
     *   |     \_Filter[emp_no{f}#17 > 1000[INTEGER]]
     *   |       \_Sample[0.1[DOUBLE]]
     *   |         \_EsRelation[employees][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_Project[[avg{r}#14, emp_no{f}#17]]
     *     \_Eval[[$$SUM$avg$0{r$}#28 / $$COUNT$avg$1{r$}#29 AS avg#14]]
     *       \_Aggregate[[emp_no{f}#17],[SUM(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#28,
     *              COUNT(salary{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#29, emp_no{f}#17]]
     *         \_StubRelation[[_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, gender{f}#19, hire_date{f}#24, job{f}#25, job.raw{f}#26,
     *              languages{f}#20, lName{r}#11, long_noidx{f}#27, salary{f}#22, msg{r}#4, salaryK{r}#8]]
     */
    public void testInlineStatsAfterSortAndSortAgnostic() {
        var query = """
            FROM employees
            | SORT emp_no DESC
            | SAMPLE .1
            | DISSECT first_name "%{msg}"
            | EVAL salaryK = salary / 1000
            | RENAME last_name AS lName
            | INLINE STATS avg = AVG(salary) BY emp_no
            | WHERE emp_no > 1000 // to avoid an existing issue with LIMIT injection past INLINESTATS (which masks the issue the test tests)
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var topN = as(plan, TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        var field = as(order.child(), FieldAttribute.class);
        assertThat(field.name(), equalTo("emp_no"));
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));

        // Left side of the join
        var esqlProject = as(inlineJoin.left(), EsqlProject.class);
        var eval = as(esqlProject.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("salaryK")));
        var dissect = as(eval.child(), Dissect.class);
        assertThat(dissect.parser().pattern(), is("%{msg}"));
        assertThat(Expressions.name(dissect.input()), is("first_name"));
        var filter = as(dissect.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));
        var sample = as(filter.child(), Sample.class);
        assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.1));
        var esRelation = as(sample.child(), EsRelation.class);
        assertThat(esRelation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var project = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "emp_no")));
        var rightEval = as(project.child(), Eval.class);
        assertThat(Expressions.names(rightEval.fields()), is(List.of("avg")));
        var agg = as(rightEval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * TopN[[Order[salary{f}#19,ASC,LAST]],1000[INTEGER],false]
     * \_InlineJoin[LEFT,[emp_no{f}#14],[emp_no{r}#14]]
     *   |_Filter[emp_no{f}#14 > 1000[INTEGER]]
     *   | \_InlineJoin[LEFT,[languages{f}#17],[languages{r}#17]]
     *   |   |_EsRelation[employees][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     *   |   \_Aggregate[[languages{f}#17],[MIN(salary{f}#19,true[BOOLEAN],PT0S[TIME_DURATION]) AS min#6, languages{f}#17]]
     *   |     \_StubRelation[[_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, gender{f}#16, hire_date{f}#21, job{f}#22, job.raw{f}#23,
     *              languages{f}#17, last_name{f}#18, long_noidx{f}#24, salary{f}#19]]
     *   \_Project[[avg{r}#11, emp_no{f}#14]]
     *     \_Eval[[$$SUM$avg$0{r$}#25 / $$COUNT$avg$1{r$}#26 AS avg#11]]
     *       \_Aggregate[[emp_no{f}#14],[SUM(salary{f}#19,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg$0#25,
     *              COUNT(salary{f}#19,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg$1#26, emp_no{f}#14]]
     *         \_StubRelation[[_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, gender{f}#16, hire_date{f}#21, job{f}#22, job.raw{f}#23,
     *              last_name{f}#18, long_noidx{f}#24, salary{f}#19, min{r}#6, languages{f}#17]]
     */
    public void testInlineStatsAfterSortDoubled() {
        var query = """
            FROM employees
            | SORT emp_no DESC // going to be dropped
            | INLINE STATS min = MIN(salary) BY languages
            | SORT salary ASC
            | INLINE STATS avg = AVG(salary) BY emp_no
            | WHERE emp_no > 1000
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var topN = as(plan, TopN.class);
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        var field = as(order.child(), FieldAttribute.class);
        assertThat(field.name(), equalTo("salary"));
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));

        var outerJoin = as(topN.child(), InlineJoin.class);
        assertThat(Expressions.names(outerJoin.config().rightFields()), is(List.of("emp_no")));

        // Outer join's left side
        var filter = as(outerJoin.left(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));

        var innerJoin = as(filter.child(), InlineJoin.class);
        assertThat(Expressions.names(innerJoin.config().rightFields()), is(List.of("languages")));

        // Inner join's left side
        var relation = as(innerJoin.left(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Inner join's right side
        var innerAgg = as(innerJoin.right(), Aggregate.class);
        assertThat(Expressions.names(innerAgg.groupings()), is(List.of("languages")));
        assertThat(innerAgg.aggregates(), hasSize(2));
        var innerAggFunction = as(innerAgg.aggregates().get(0), Alias.class);
        assertThat(innerAggFunction.name(), is("min"));
        assertThat(innerAggFunction.child().nodeName(), is("Min"));
        assertThat(innerAgg.child(), instanceOf(StubRelation.class));

        // Outer join's right side
        var project = as(outerJoin.right(), Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("avg", "emp_no")));
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var outerAgg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(outerAgg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        assertThat(outerAgg.child(), instanceOf(StubRelation.class));
    }

    /*
     * Project[[emp_no{f}#22, ls{r}#10, cd{r}#16, s1{r}#13]]
     * \_TopN[[Order[$$s1$temp_name$33{r}#34,ASC,LAST]],1000[INTEGER],false]
     *   \_Filter[emp_no{f}#22 < 10006[INTEGER]]
     *     \_InlineJoin[LEFT,[],[]]
     *       |_Project[[_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, gender{f}#24, hire_date{f}#29, job{f}#30, job.raw{f}#31,
     *          languages{f}#25, last_name{f}#26, long_noidx{f}#32, salary{f}#27, ls{r}#10, salary{f}#27 AS s1#13, $$s1$temp_name$33{r}#34]]
     *       | \_Eval[[TOSTRING(languages{f}#25) AS ls#10, s1{r}#5 AS $$s1$temp_name$33#34]]
     *       |   \_Filter[s1{r}#5 > 50000[INTEGER]]
     *       |     \_Eval[[salary{f}#27 + 1[INTEGER] AS s1#5]]
     *       |       \_EsRelation[employees][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     *       \_Aggregate[[],[COUNTDISTINCT(ls{r}#10,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#16]]
     *         \_StubRelation[[_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, gender{f}#24, hire_date{f}#29, job{f}#30, job.raw{f}#31,
     *              languages{f}#25, last_name{f}#26, long_noidx{f}#32, salary{f}#27, ls{r}#10, s1{r}#13]]
     */
    public void testInlineStatsAfterSortShaddowed() {
        var query = """
            FROM employees
            | EVAL s1 = salary + 1
            | SORT s1
            | WHERE s1 > 50000
            | EVAL ls = languages::string
            | EVAL s1 = salary // s1 is shadowed, not dropped
            | INLINE STATS cd = COUNT_DISTINCT(ls)
            | WHERE emp_no < 10006
            | KEEP emp_no, ls, cd, s1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "ls", "cd", "s1")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order().size(), is(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), startsWith("$$s1$temp_name$"));

        var filter = as(topN.child(), Filter.class);
        var filterCondition = as(filter.condition(), LessThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(10006));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(inlineJoin.config().leftFields(), empty());
        assertThat(inlineJoin.config().rightFields(), empty());

        // Left side of the join
        var leftProject = as(inlineJoin.left(), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(is("ls"), startsWith("$$s1$temp_name$")));
        var leftFilter = as(leftEval.child(), Filter.class);
        var leftFilterCondition = as(leftFilter.condition(), GreaterThan.class);
        assertThat(Expressions.name(leftFilterCondition.left()), equalTo("s1"));
        assertThat(leftFilterCondition.right().fold(FoldContext.small()), equalTo(50000));
        var innerEval = as(leftFilter.child(), Eval.class);
        assertThat(Expressions.names(innerEval.fields()), is(List.of("s1")));
        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(agg.groupings(), empty());
        assertThat(agg.aggregates().size(), is(1));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("cd"));
        assertThat(aggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#30, ls{r}#15, cd{r}#24, s1{r}#18]]
     * \_TopN[[Order[$$s1$temp_name$41{r}#42,ASC,LAST], Order[emp_no{f}#30,ASC,LAST], Order[$$s2$temp_name$43{r}#44,ASC,LAST]],
     *      1000[INTEGER],false]
     *   \_Filter[emp_no{f}#30 < 10006[INTEGER]]
     *     \_InlineJoin[LEFT,[],[]]
     *       |_Project[[_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, gender{f}#32, hire_date{f}#37, job{f}#38, job.raw{f}#39,
     *          languages{f}#33, last_name{f}#34, long_noidx{f}#40, salary{f}#35, ls{r}#15, salary{f}#35 AS s1#18, salary{f}#35 AS s2#21,
     *          $$s1$temp_name$41{r}#42, $$s2$temp_name$43{r}#44]]
     *       | \_Eval[[TOSTRING(languages{f}#33) AS ls#15, s1{r}#5 AS $$s1$temp_name$41#42, s2{r}#8 AS $$s2$temp_name$43#44]]
     *       |   \_Filter[s1{r}#5 > 50000[INTEGER]]
     *       |     \_Eval[[salary{f}#35 + 1[INTEGER] AS s1#5, salary{f}#35 + 2[INTEGER] AS s2#8]]
     *       |       \_EsRelation[employees][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
     *       \_Aggregate[[],[COUNTDISTINCT(ls{r}#15,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#24]]
     *         \_StubRelation[[_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, gender{f}#32, hire_date{f}#37, job{f}#38, job.raw{f}#39, l
     * anguages{f}#33, last_name{f}#34, long_noidx{f}#40, salary{f}#35, ls{r}#15, s1{r}#18, s2{r}#21]]
     */
    public void testInlineStatsAfterTriSortPartlyShaddowed() {
        var query = """
            FROM employees
            | EVAL s1 = salary + 1, s2 = salary + 2
            | SORT s1, emp_no, s2
            | WHERE s1 > 50000
            | EVAL ls = languages::string
            | EVAL s1 = salary, s2 = s1 // s1 is shadowed, s2 renamed
            | INLINE STATS cd = COUNT_DISTINCT(ls)
            | WHERE emp_no < 10006
            | KEEP emp_no, ls, cd, s1
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_no", "ls", "cd", "s1")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(3));

        var order1 = as(topN.order().get(0), Order.class);
        assertThat(order1.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order1.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order1.child()), startsWith("$$s1$temp_name$"));

        var order2 = as(topN.order().get(1), Order.class);
        assertThat(order2.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order2.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order2.child()), is("emp_no"));

        var order3 = as(topN.order().get(2), Order.class);
        assertThat(order3.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order3.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order3.child()), startsWith("$$s2$temp_name$"));

        var filter = as(topN.child(), Filter.class);
        var filterCondition = as(filter.condition(), LessThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(10006));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(inlineJoin.config().leftFields(), empty());
        assertThat(inlineJoin.config().rightFields(), empty());

        // Left side of the join
        var leftProject = as(inlineJoin.left(), Project.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(is("ls"), startsWith("$$s1$temp_name$"), startsWith("$$s2$temp_name$")));
        var leftFilter = as(leftEval.child(), Filter.class);
        var leftFilterCondition = as(leftFilter.condition(), GreaterThan.class);
        assertThat(Expressions.name(leftFilterCondition.left()), equalTo("s1"));
        assertThat(leftFilterCondition.right().fold(FoldContext.small()), equalTo(50000));
        var innerEval = as(leftFilter.child(), Eval.class);
        assertThat(Expressions.names(innerEval.fields()), is(List.of("s1", "s2")));
        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(agg.groupings(), empty());
        assertThat(agg.aggregates(), hasSize(1));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("cd"));
        assertThat(aggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     *      languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17, cd{r}#11]]
     * \_TopN[[Order[$$s1$temp_name$23{r}#24,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[],[]]
     *     |_EsqlProject[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     *          languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17, $$s1$temp_name$23{r}#24]]
     *     | \_Eval[[s1{r}#5 AS $$s1$temp_name$23#24]]
     *     |   \_Filter[s1{r}#5 > 50000[INTEGER]]
     *     |     \_Eval[[salary{f}#17 + 1[INTEGER] AS s1#5]]
     *     |       \_EsRelation[employees][_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, ..]
     *     \_Aggregate[[],[COUNTDISTINCT(languages{f}#15,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#11]]
     *       \_StubRelation[[_meta_field{f}#18, emp_no{f}#12, first_name{f}#13, gender{f}#14, hire_date{f}#19, job{f}#20, job.raw{f}#21,
     *          languages{f}#15, last_name{f}#16, long_noidx{f}#22, salary{f}#17]]
     */
    public void testInlineStatsAfterSortDropped() {
        var query = """
            FROM employees
            | EVAL s1 = salary + 1
            | SORT s1
            | WHERE s1 > 50000
            | DROP s1
            | INLINE STATS cd = COUNT_DISTINCT(languages)
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), hasItem("cd"));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), startsWith("$$s1$temp_name$"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(inlineJoin.config().leftFields(), empty());
        assertThat(inlineJoin.config().rightFields(), empty());

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(startsWith("$$s1$temp_name$")));
        var leftFilter = as(leftEval.child(), Filter.class);
        var leftFilterCondition = as(leftFilter.condition(), GreaterThan.class);
        assertThat(Expressions.name(leftFilterCondition.left()), equalTo("s1"));
        assertThat(leftFilterCondition.right().fold(FoldContext.small()), equalTo(50000));
        var innerEval = as(leftFilter.child(), Eval.class);
        assertThat(Expressions.names(innerEval.fields()), is(List.of("s1")));
        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(agg.groupings(), empty());
        assertThat(agg.aggregates(), hasSize(1));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("cd"));
        assertThat(aggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[salary{r}#7, emp_no{f}#9]]
     * \_TopN[[Order[$$salary$temp_name$20{r}#21,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#9],[emp_no{r}#9]]
     *     |_EsqlProject[[salary{f}#14, emp_no{f}#9, $$salary$temp_name$20{r}#21]]
     *     | \_Eval[[salary{f}#14 AS $$salary$temp_name$20#21]]
     *     |   \_EsRelation[employees][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *     \_Aggregate[[emp_no{f}#9],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS salary#7, emp_no{f}#9]]
     *       \_StubRelation[[salary{f}#14, emp_no{f}#9]]
     */
    public void testShadowingInlineStatsAfterSort() {
        var query = """
            FROM employees
            | KEEP salary, emp_no
            | SORT salary
            | INLINE STATS salary = COUNT(*) BY emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("salary", "emp_no")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), startsWith("$$salary$temp_name$"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(startsWith("$$salary$temp_name$")));
        var relation = as(leftEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("emp_no")));
        assertThat(agg.aggregates(), hasSize(2));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("salary"));
        assertThat(aggFunction.child().nodeName(), is("Count"));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[salary{r}#8, emp_no{f}#10]]
     * \_TopN[[Order[$$salary$temp_name$21{r}#22,ASC,LAST], Order[emp_no{f}#10,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#10],[emp_no{r}#10]]
     *     |_EsqlProject[[salary{f}#15, emp_no{f}#10, $$salary$temp_name$21{r}#22]]
     *     | \_Eval[[salary{f}#15 AS $$salary$temp_name$21#22]]
     *     |   \_EsRelation[employees][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *     \_Aggregate[[emp_no{f}#10],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS salary#8, emp_no{f}#10]]
     *       \_StubRelation[[salary{f}#15, emp_no{f}#10]]
     */
    public void testMixedShadowingInlineStatsAfterSort() {
        var query = """
            FROM employees
            | KEEP salary, emp_no
            | SORT salary, emp_no
            | INLINE STATS salary = COUNT(*) BY emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("salary", "emp_no")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(2));

        var order1 = as(topN.order().get(0), Order.class);
        assertThat(order1.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order1.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order1.child()), startsWith("$$salary$temp_name$"));

        var order2 = as(topN.order().get(1), Order.class);
        assertThat(order2.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order2.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order2.child()), is("emp_no"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(startsWith("$$salary$temp_name$")));
        var relation = as(leftEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("emp_no")));
        assertThat(agg.aggregates(), hasSize(2));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("salary"));
        assertThat(aggFunction.child().nodeName(), is("Count"));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[salary{r}#12, emp_no{f}#14]]
     * \_TopN[[Order[$$salary$temp_name$25{r}#26,ASC,LAST], Order[$$s1$temp_name$27{r}#28,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[emp_no{f}#14],[emp_no{r}#14]]
     *     |_EsqlProject[[salary{f}#19, emp_no{f}#14, $$salary$temp_name$25{r}#26, $$s1$temp_name$27{r}#28]]
     *     | \_Eval[[salary{f}#19 + 1[INTEGER] AS s1#7, salary{f}#19 AS $$salary$temp_name$25#26, s1{r}#7 AS $$s1$temp_name$27#28]]
     *     |   \_EsRelation[employees][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
     *     \_Aggregate[[emp_no{f}#14],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS salary#12, emp_no{f}#14]]
     *       \_StubRelation[[salary{f}#19, emp_no{f}#14]]
     */
    public void testShadowingInlineStatsAfterSortAndDrop() {
        var query = """
            FROM employees
            | KEEP salary, emp_no
            | EVAL s1 = salary + 1
            | SORT salary, s1
            | DROP s1
            | INLINE STATS salary = COUNT(*) BY emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("salary", "emp_no")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(2));

        var order1 = as(topN.order().get(0), Order.class);
        assertThat(order1.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order1.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order1.child()), startsWith("$$salary$temp_name$"));

        var order2 = as(topN.order().get(1), Order.class);
        assertThat(order2.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order2.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order2.child()), startsWith("$$s1$temp_name$"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("emp_no")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(
            Expressions.names(leftEval.fields()),
            contains(is("s1"), startsWith("$$salary$temp_name$"), startsWith("$$s1$temp_name$"))
        );
        var relation = as(leftEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("emp_no")));
        assertThat(agg.aggregates(), hasSize(2));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("salary"));
        assertThat(aggFunction.child().nodeName(), is("Count"));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_idx{r}#9, salary{f}#20, sum{r}#13, languages{f}#18]]
     * \_TopN[[Order[$$emp_no$temp_name$27{r}#28,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[languages{f}#18],[languages{r}#18]]
     *     |_EsqlProject[[emp_no{f}#15 AS emp_idx#9, salary{f}#20, languages{f}#18, $$emp_no$temp_name$27{r}#28]]
     *     | \_Eval[[emp_no{f}#15 AS $$emp_no$temp_name$27#28]]
     *     |   \_EsRelation[employees][_meta_field{f}#21, emp_no{f}#15, first_name{f}#16, ..]
     *     \_Project[[sum{r}#13, languages{f}#18]]
     *       \_Eval[[$$COUNT$COUNT(salary)_+>$0{r$}#26 + $$COUNT$COUNT(salary)_+>$0{r$}#26 AS sum#13]]
     *         \_Aggregate[[languages{f}#18],[COUNT(salary{f}#20,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$COUNT(salary)_+>$0#26,
     *              languages{f}#18]]
     *           \_StubRelation[[emp_idx{r}#9, salary{f}#20, languages{f}#18]]
     */
    public void testInlineStatsWithAggExpressionAfterSortAndRename() {
        var query = """
            FROM employees
            | KEEP emp_no, salary, languages
            | SORT emp_no
            | RENAME emp_no AS emp_idx
            | INLINE STATS sum = COUNT(salary) + COUNT(salary) BY languages
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("emp_idx", "salary", "sum", "languages")));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), startsWith("$$emp_no$temp_name$"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("languages")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("languages")));

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(startsWith("$$emp_no$temp_name$")));
        var relation = as(leftEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var rightProject = as(inlineJoin.right(), Project.class);
        assertThat(Expressions.names(rightProject.projections()), is(List.of("sum", "languages")));
        var rightEval = as(rightProject.child(), Eval.class);
        assertThat(Expressions.names(rightEval.fields()), is(List.of("sum")));
        var agg = as(rightEval.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("languages")));
        assertThat(agg.aggregates(), hasSize(2));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), startsWith("$$COUNT$COUNT(salary)_+>$"));
        assertThat(aggFunction.child().nodeName(), is("Count"));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[emp_no{f}#16 > 50000[INTEGER]]
     *   \_InlineJoin[LEFT,[],[]]
     *     |_Aggregate[[emp_no{f}#16, languages{f}#19],[SUM(s1{r}#5,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS s#11,
     *          emp_no{f}#16, languages{f}#19]]
     *     | \_Eval[[salary{f}#21 + 1[INTEGER] AS s1#5]]
     *     |   \_EsRelation[employees][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_Aggregate[[],[COUNTDISTINCT(languages{f}#19,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#14]]
     *       \_StubRelation[[s{r}#11, emp_no{f}#16, languages{f}#19]]
     */
    public void testInlineStatsAfterSortAndStats() {
        var query = """
            FROM employees
            | EVAL s1 = salary + 1
            | SORT s1
            | STATS s = SUM(s1) BY emp_no, languages
            | INLINE STATS cd = COUNT_DISTINCT(languages)
            | WHERE emp_no > 50000
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        var filter = as(limit.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(50000));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(inlineJoin.config().leftFields(), empty());
        assertThat(inlineJoin.config().rightFields(), empty());

        // Left side of the join
        var leftAgg = as(inlineJoin.left(), Aggregate.class);
        assertThat(Expressions.names(leftAgg.groupings()), is(List.of("emp_no", "languages")));
        assertThat(leftAgg.aggregates(), hasSize(3));
        var leftAggFunction = as(leftAgg.aggregates().get(0), Alias.class);
        assertThat(leftAggFunction.name(), is("s"));
        assertThat(leftAggFunction.child().nodeName(), is("Sum"));
        var leftEval = as(leftAgg.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), is(List.of("s1")));
        var relation = as(leftEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var rightAgg = as(inlineJoin.right(), Aggregate.class);
        assertThat(rightAgg.groupings(), empty());
        assertThat(rightAgg.aggregates(), hasSize(1));
        var rightAggFunction = as(rightAgg.aggregates().get(0), Alias.class);
        assertThat(rightAggFunction.name(), is("cd"));
        assertThat(rightAggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(rightAgg.child(), StubRelation.class);
    }

    /**
     * TopN[[Order[salary{r}#11,DESC,FIRST]],5[INTEGER],false]
     * \_InlineJoin[LEFT,[gender{f}#18],[gender{r}#18]]
     *   |_Aggregate[[gender{f}#18],[MAX(salary{f}#21,true[BOOLEAN],PT0S[TIME_DURATION]) AS salary#11, gender{f}#18]]
     *   | \_EsRelation[employees][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *   \_Aggregate[[gender{f}#18],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS s#14, gender{f}#18]]
     *     \_StubRelation[[salary{r}#11, gender{f}#18]]
     */
    public void testInlineStatsAfterSortAndStatsAndSort() {
        var query = """
            FROM employees
            | KEEP salary, emp_no, first_name, gender
            | SORT salary
            | STATS salary = MAX(salary) BY gender
            | SORT salary DESC
            | INLINE STATS s = COUNT(*) BY gender
            | LIMIT 5
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var topN = as(plan, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(5));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), is("salary"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("gender")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("gender")));

        // Left side of the join
        var leftAgg = as(inlineJoin.left(), Aggregate.class);
        assertThat(Expressions.names(leftAgg.groupings()), is(List.of("gender")));
        assertThat(leftAgg.aggregates(), hasSize(2));
        var leftAggFunction = as(leftAgg.aggregates().get(0), Alias.class);
        assertThat(leftAggFunction.name(), is("salary"));
        assertThat(leftAggFunction.child().nodeName(), is("Max"));
        var relation = as(leftAgg.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var rightAgg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(rightAgg.groupings()), is(List.of("gender")));
        assertThat(rightAgg.aggregates(), hasSize(2));
        var rightAggFunction = as(rightAgg.aggregates().get(0), Alias.class);
        assertThat(rightAggFunction.name(), is("s"));
        assertThat(rightAggFunction.child().nodeName(), is("Count"));
        var stub = as(rightAgg.child(), StubRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false,false]
     * \_Filter[emp_no{f}#16 > 50000[INTEGER]]
     *   \_InlineJoin[LEFT,[],[]]
     *     |_Aggregate[[emp_no{f}#16, languages{f}#19],[SUM(s1{r}#5,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS s#11,
     *          emp_no{f}#16, languages{f}#19]]
     *     | \_Eval[[salary{f}#21 + 1[INTEGER] AS s1#5]]
     *     |   \_EsRelation[employees][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_Aggregate[[],[COUNTDISTINCT(languages{f}#19,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#14]]
     *       \_StubRelation[[s{r}#11, emp_no{f}#16, languages{f}#19]]
     */
    public void testInlineStatsAfterEvalAndSortAndStats() {
        var query = """
            FROM employees
            | EVAL s1 = salary + 1
            | SORT s1
            | STATS s = SUM(s1) BY emp_no, languages
            | INLINE STATS cd = COUNT_DISTINCT(languages)
            | WHERE emp_no > 50000
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var limit = as(plan, Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        var filter = as(limit.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(50000));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(inlineJoin.config().leftFields(), empty());
        assertThat(inlineJoin.config().rightFields(), empty());

        // Left side of the join
        var leftAgg = as(inlineJoin.left(), Aggregate.class);
        assertThat(Expressions.names(leftAgg.groupings()), is(List.of("emp_no", "languages")));
        assertThat(leftAgg.aggregates(), hasSize(3));
        var leftAggFunction = as(leftAgg.aggregates().get(0), Alias.class);
        assertThat(leftAggFunction.name(), is("s"));
        assertThat(leftAggFunction.child().nodeName(), is("Sum"));

        var eval = as(leftAgg.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("s1")));
        var relation = as(eval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var rightAgg = as(inlineJoin.right(), Aggregate.class);
        assertThat(rightAgg.groupings(), empty());
        assertThat(rightAgg.aggregates(), hasSize(1));
        var rightAggFunction = as(rightAgg.aggregates().get(0), Alias.class);
        assertThat(rightAggFunction.name(), is("cd"));
        assertThat(rightAggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(rightAgg.child(), StubRelation.class);
    }

    /*
     * Project[[emp_no{f}#22, first_name{f}#23, sal{r}#17, id{r}#13, language_code{r}#36, language_name{r}#37, cd{r}#20, languages{f}#25]]
     * \_TopN[[Order[$$language_name$temp_name$38$temp_name$40{r}#41,ASC,LAST]],1000[INTEGER],false]
     *   \_InlineJoin[LEFT,[languages{f}#25],[languages{r}#25]]
     *     |_EsqlProject[[emp_no{f}#22, first_name{f}#23, salary{f}#27 AS sal#17, languages{f}#25, id{r}#13, language_code{r}#36,
     *          language_name{r}#37, $$language_name$temp_name$38$temp_name$40{r}#41]]
     *     | \_Eval[[$$language_name$temp_name$38{r$}#39 AS $$language_name$temp_name$38$temp_name$40#41]]
     *     |   \_Enrich[ANY,languages_idx[KEYWORD],id{r}#13,{"match":{"indices":[],"match_field":"id",
     *              "enrich_fields":["language_code","language_name"]}},{=languages_idx},[language_code{r}#36, language_name{r}#37]]
     *     |     \_Eval[[TOSTRING(languages{f}#25) AS id#13, first_name{f}#23 AS $$language_name$temp_name$38#39]]
     *     |       \_EsRelation[employees][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     *     \_Aggregate[[languages{f}#25],[COUNT(emp_no{f}#22,true[BOOLEAN],PT0S[TIME_DURATION]) AS cd#20, languages{f}#25]]
     *       \_StubRelation[[emp_no{f}#22, first_name{f}#23, sal{r}#17, languages{f}#25, id{r}#13, language_code{r}#36,
     *              language_name{r}#37]]
     */
    public void testInlineStatsAfterEnrichAndSort() {
        var query = """
            FROM employees
            | KEEP emp_no, first_name, salary, languages
            | EVAL language_name = first_name // key in this test is to have a field overwritten by the enrich, and sorting by it before
            | SORT language_name
            | EVAL id = languages::KEYWORD
            | ENRICH languages_idx
            | RENAME salary AS sal
            | INLINE STATS cd = COUNT(emp_no) BY languages
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        assertThat(
            Expressions.names(project.projections()),
            is(List.of("emp_no", "first_name", "sal", "id", "language_code", "language_name", "cd", "languages"))
        );

        var topN = as(project.child(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.LAST));
        assertThat(Expressions.name(order.child()), startsWith("$$language_name$temp_name$"));

        var inlineJoin = as(topN.child(), InlineJoin.class);
        assertThat(inlineJoin.config().type(), is(JoinTypes.LEFT));
        assertThat(Expressions.names(inlineJoin.config().leftFields()), is(List.of("languages")));
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("languages")));

        // Left side of the join
        var leftProject = as(inlineJoin.left(), EsqlProject.class);
        var leftEval = as(leftProject.child(), Eval.class);
        assertThat(Expressions.names(leftEval.fields()), contains(startsWith("$$language_name$temp_name$")));
        var enrich = as(leftEval.child(), Enrich.class);
        assertThat(Expressions.name(enrich.policyName()), is("languages_idx"));
        var innerEval = as(enrich.child(), Eval.class);
        assertThat(Expressions.names(innerEval.fields()), contains(is("id"), startsWith("$$language_name$temp_name$")));
        var relation = as(innerEval.child(), EsRelation.class);
        assertThat(relation.concreteQualifiedIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), is(List.of("languages")));
        assertThat(agg.aggregates(), hasSize(2));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("cd"));
        assertThat(aggFunction.child().nodeName(), is("Count"));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * Project[[abbrev{f}#20, scalerank{f}#22 AS backup_scalerank#5, language_name{f}#29 AS scalerank#13]]
     * \_Limit[1000[INTEGER],true,false]
     *   \_Join[LEFT,[scalerank{f}#22],[language_code{f}#28],null]
     *     |_TopN[[Order[abbrev{f}#20,DESC,FIRST]],1000[INTEGER],false]
     *     | \_EsRelation[airports][abbrev{f}#20, city{f}#26, city_location{f}#27, coun..]
     *     \_EsRelation[languages_lookup][LOOKUP][language_code{f}#28, language_name{f}#29]
     */
    public void testInlineJoinPrunedAfterSortAndLookupJoin() {
        var query = """
            FROM airports
            | EVAL backup_scalerank = scalerank
            | RENAME scalerank AS language_code
            | SORT abbrev DESC
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name as scalerank
            | DROP language_code
            | INLINE STATS count=COUNT(*) BY scalerank
            | KEEP abbrev, *scalerank
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = planAirports(query);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), is(List.of("abbrev", "backup_scalerank", "scalerank")));

        var limit = as(project.child(), Limit.class);
        assertThat(limit.limit().fold(FoldContext.small()), equalTo(1000));

        var join = as(limit.child(), Join.class);
        assertThat(join.config().type(), is(JoinTypes.LEFT));

        var topN = as(join.left(), TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(topN.order(), hasSize(1));
        var order = as(topN.order().get(0), Order.class);
        assertThat(order.direction(), equalTo(Order.OrderDirection.DESC));
        assertThat(order.nullsPosition(), equalTo(Order.NullsPosition.FIRST));
        assertThat(Expressions.name(order.child()), is("abbrev"));

        var leftRelation = as(topN.child(), EsRelation.class);
        assertThat(leftRelation.concreteQualifiedIndices(), is(Set.of("airports")));

        var rightRelation = as(join.right(), EsRelation.class);
        assertThat(rightRelation.concreteQualifiedIndices(), is(Set.of("languages_lookup")));
    }

    public void testFailureWhenSortAndSortBreakerBeforeInlineStats() {
        assumeTrue("LIMIT before INLINE STATS limitation check", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        /*
         * Notes:
         * - SORT before STATS works (by dropping the SORT, see testInlineStatsAfterSortAndStats() above)
         * - FORK will inject a LIMIT and thus fail because of the "LIMIT before INLINE STATS" limitation
         */
        for (var cmd : List.of("MV_EXPAND languages", "LOOKUP JOIN languages_lookup ON language_code == languages")) {
            failPlan(
                """
                    FROM test
                    | KEEP emp_no, languages, gender
                    | SORT emp_no
                    |""" + " " + cmd + "\n" + """
                    | INLINE STATS max_lang = MAX(languages) BY gender
                    | WHERE emp_no > 10000 // prevents the default LIMIT being pushed past INLINE STATS
                    """,
                "line 5:3: INLINE STATS [INLINE STATS max_lang = MAX(languages) BY gender] cannot yet have an unbounded SORT"
                    + " [SORT emp_no] before it"
            );
        }
    }
}
