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
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
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

public class PullUpOrderByBeforeInlineJoinOptimizerTests extends AbstractLogicalPlanOptimizerTests {

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
        assertThat(relation.concreteIndices(), is(Set.of("employees")));
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
     * TopN[[Order[emp_no{f}#8,DESC,FIRST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#8 > 1000[INTEGER]]
     *   \_InlineJoin[LEFT,[emp_no{f}#8],[emp_no{f}#8],[emp_no{r}#8]]
     *     |_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *     \_Project[[avg{r}#5, emp_no{f}#8]]
     *       \_Eval[[$$SUM$avg$0{r$}#19 / $$COUNT$avg$1{r$}#20 AS avg#5]]
     *         \_Aggregate[[emp_no{f}#8],[SUM(salary{f}#13,true[BOOLEAN]) AS $$SUM$avg$0#19, COUNT(salary{f}#13,true[BOOLEAN]) AS $$COUNT$
     *              avg$1#20, emp_no{f}#8]]
     *           \_StubRelation[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17,
     *                  languages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13]]
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

        var filter = as(topN.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));
        // Left
        var relation = as(inlineJoin.left(), EsRelation.class);
        assertThat(relation.concreteIndices(), is(Set.of("employees")));
        // Right
        var project = as(inlineJoin.right(), Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * TopN[[Order[emp_no{f}#18,DESC,FIRST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#18 > 1000[INTEGER]]
     *   \_InlineJoin[LEFT,[emp_no{f}#18],[emp_no{f}#18],[emp_no{r}#18]]
     *     |_EsqlProject[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *          languages{r}#29, last_name{f}#22 AS lName#12, long_noidx{f}#28, salary{f}#23, msg{r}#5, salaryK{r}#9]]
     *     | \_Eval[[salary{f}#23 / 1000[INTEGER] AS salaryK#9]]
     *     |   \_Dissect[first_name{f}#19,Parser[pattern=%{msg}, appendSeparator=,
     *              parser=org.elasticsearch.dissect.DissectParser@2aa687d7],[msg{r}#5]]
     *     |     \_Sample[0.1[DOUBLE]]
     *     |       \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     *     \_Project[[avg{r}#15, emp_no{f}#18]]
     *       \_Eval[[$$SUM$avg$0{r$}#30 / $$COUNT$avg$1{r$}#31 AS avg#15]]
     *         \_Aggregate[[emp_no{f}#18],[SUM(salary{f}#23,true[BOOLEAN]) AS $$SUM$avg$0#30, COUNT(salary{f}#23,true[BOOLEAN]) AS
     *               $$COUNT$avg$1#31, emp_no{f}#18]]
     *           \_StubRelation[[_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, gender{f}#20, hire_date{f}#25, job{f}#26, job.raw{f}#27,
     *                  anguages{r}#29, lName{r}#12, long_noidx{f}#28, salary{f}#23, msg{r}#5, salaryK{r}#9]]
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

        var filter = as(topN.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));
        // Left
        var esqlProject = as(inlineJoin.left(), EsqlProject.class);

        var eval = as(esqlProject.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("salaryK")));

        var dissect = as(eval.child(), Dissect.class);
        assertThat(dissect.parser().pattern(), is("%{msg}"));
        assertThat(Expressions.name(dissect.input()), is("first_name"));

        var sample = as(dissect.child(), Sample.class);
        assertThat(sample.probability().fold(FoldContext.small()), equalTo(0.1));

        var esRelation = as(sample.child(), EsRelation.class);

        // Right
        var project = as(inlineJoin.right(), Project.class);
        eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        var agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        var stub = as(agg.child(), StubRelation.class);
    }

    /*
     * TopN[[Order[salary{f}#18,ASC,LAST]],1000[INTEGER]]
     * \_Filter[emp_no{f}#13 > 1000[INTEGER]]
     *   \_InlineJoin[LEFT,[emp_no{f}#13],[emp_no{f}#13],[emp_no{r}#13]]
     *     |_InlineJoin[LEFT,[languages{f}#16],[languages{f}#16],[languages{r}#16]]
     *     | |_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
     *     | \_Aggregate[[languages{f}#16],[MIN(salary{f}#18,true[BOOLEAN]) AS min#5, languages{f}#16]]
     *     |   \_StubRelation[[_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, gender{f}#15, hire_date{f}#20, job{f}#21, job.raw{f}#22,
     *              languages{f}#16, last_name{f}#17, long_noidx{f}#23, salary{f}#18]]
     *     \_Project[[avg{r}#10, emp_no{f}#13]]
     *       \_Eval[[$$SUM$avg$0{r$}#24 / $$COUNT$avg$1{r$}#25 AS avg#10]]
     *         \_Aggregate[[emp_no{f}#13],[SUM(salary{f}#18,true[BOOLEAN]) AS $$SUM$avg$0#24, COUNT(salary{f}#18,true[BOOLEAN]) AS
     *               $$COUNT$avg$1#25, emp_no{f}#13]]
     *           \_StubRelation[[_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, gender{f}#15, hire_date{f}#20, job{f}#21, job.raw{f}#22,
     *                  ast_name{f}#17, long_noidx{f}#23, salary{f}#18, min{r}#5, languages{f}#16]]
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

        var filter = as(topN.child(), Filter.class);
        var filterCondition = as(filter.condition(), GreaterThan.class);
        assertThat(Expressions.name(filterCondition.left()), equalTo("emp_no"));
        assertThat(filterCondition.right().fold(FoldContext.small()), equalTo(1000));

        var inlineJoin = as(filter.child(), InlineJoin.class);
        assertThat(Expressions.names(inlineJoin.config().rightFields()), is(List.of("emp_no")));
        // outer left
        var inlineJoinLeft = as(inlineJoin.left(), InlineJoin.class);
        // inner left
        var relation = as(inlineJoinLeft.left(), EsRelation.class);
        assertThat(relation.concreteIndices(), is(Set.of("employees")));
        // inner right
        var agg = as(inlineJoinLeft.right(), Aggregate.class);
        var groupings = agg.groupings();
        assertThat(groupings.size(), is(1));
        var fieldAttribute = as(groupings.get(0), FieldAttribute.class);
        assertThat(fieldAttribute.name(), is("languages"));
        var aggs = agg.aggregates();
        assertThat(aggs.get(0).toString(), startsWith("MIN(salary) AS min"));
        var stub = as(agg.child(), StubRelation.class);
        // outer right
        var project = as(inlineJoin.right(), Project.class);
        var eval = as(project.child(), Eval.class);
        assertThat(Expressions.names(eval.fields()), is(List.of("avg")));
        agg = as(eval.child(), Aggregate.class);
        assertMap(Expressions.names(agg.output()), is(List.of("$$SUM$avg$0", "$$COUNT$avg$1", "emp_no")));
        stub = as(agg.child(), StubRelation.class);
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
        assertThat(relation.concreteIndices(), is(Set.of("employees")));

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
        assertThat(relation.concreteIndices(), is(Set.of("employees")));

        // Right side of the join
        var agg = as(inlineJoin.right(), Aggregate.class);
        assertThat(agg.groupings(), empty());
        assertThat(agg.aggregates(), hasSize(1));
        var aggFunction = as(agg.aggregates().get(0), Alias.class);
        assertThat(aggFunction.name(), is("cd"));
        assertThat(aggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(agg.child(), StubRelation.class);
    }

    /**
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
        assertThat(relation.concreteIndices(), is(Set.of("employees")));

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

        var limit = as(plan, org.elasticsearch.xpack.esql.plan.logical.Limit.class);
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
        assertThat(relation.concreteIndices(), is(Set.of("employees")));

        // Right side of the join
        var rightAgg = as(inlineJoin.right(), Aggregate.class);
        assertThat(rightAgg.groupings(), empty());
        assertThat(rightAgg.aggregates(), hasSize(1));
        var rightAggFunction = as(rightAgg.aggregates().get(0), Alias.class);
        assertThat(rightAggFunction.name(), is("cd"));
        assertThat(rightAggFunction.child(), instanceOf(CountDistinct.class));
        var stub = as(rightAgg.child(), StubRelation.class);
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
