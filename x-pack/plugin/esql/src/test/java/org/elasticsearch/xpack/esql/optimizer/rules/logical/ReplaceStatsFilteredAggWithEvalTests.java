/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.LongVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests.releaseBuildForInlineStats;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ReplaceStatsFilteredAggWithEvalTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_LocalRelation[[sum(salary) where false{r}#26],[ConstantNullBlock[positions=1]]]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalSingleAgg() {
        var plan = plan("""
            from test
            | stats sum(salary) where false
            """);

        var project = as(plan, Limit.class);
        var source = as(project.child(), LocalRelation.class);
        assertThat(Expressions.names(source.output()), contains("sum(salary) where false"));
        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        assertThat(page.getBlock(0).getPositionCount(), is(1));
        assertTrue(page.getBlock(0).areAllValuesNull());
    }

    /**
     * <pre>{@code
     * Project[[sum(salary) + 1 where false{r}#68]]
     * \_Eval[[$$SUM$sum(salary)_+_1$0{r$}#79 + 1[INTEGER] AS sum(salary) + 1 where false]]
     *   \_Limit[1000[INTEGER]]
     *     \_LocalRelation[[$$SUM$sum(salary)_+_1$0{r$}#79],[ConstantNullBlock[positions=1]]]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalSingleAggWithExpression() {
        var plan = plan("""
            from test
            | stats sum(salary) + 1 where false
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("sum(salary) + 1 where false"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("sum(salary) + 1 where false"));
        var add = as(alias.child(), Add.class);
        var literal = as(add.right(), Literal.class);
        assertThat(literal.value(), is(1));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), LocalRelation.class);

        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        assertThat(page.getBlock(0).getPositionCount(), is(1));
        assertTrue(page.getBlock(0).areAllValuesNull());
    }

    /**
     * <pre>{@code
     * Project[[sum(salary) + 1 where false{r}#4, sum(salary) + 2{r}#6, emp_no{f}#7]]
     * \_Eval[[null[LONG] AS sum(salary) + 1 where false, $$SUM$sum(salary)_+_2$1{r$}#18 + 2[INTEGER] AS sum(salary) + 2]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[STANDARD,[emp_no{f}#7],[SUM(salary{f}#12,true[BOOLEAN]) AS $$SUM$sum(salary)_+_2$1, emp_no{f}#7]]
     *       \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalMixedFilterAndNoFilter() {
        var plan = plan("""
            from test
            | stats sum(salary) + 1 where false,
                    sum(salary) + 2
              by emp_no
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("sum(salary) + 1 where false", "sum(salary) + 2", "emp_no"));
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(2));

        var alias = as(eval.fields().getFirst(), Alias.class);
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));

        alias = as(eval.fields().getLast(), Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("sum(salary) + 2"));

        var limit = as(eval.child(), Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var source = as(aggregate.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Project[[sum(salary) + 1 where false{r}#3, sum(salary) + 3{r}#5, sum(salary) + 2 where null{r}#7,
     * sum(salary) + 4 where not true{r}#9]]
     * \_Eval[[null[LONG] AS sum(salary) + 1 where false#3, $$SUM$sum(salary)_+_3$1{r$}#22 + 3[INTEGER] AS sum(salary) + 3#5
     * , null[LONG] AS sum(salary) + 2 where null#7, null[LONG] AS sum(salary) + 4 where not true#9]]
     *   \_Limit[1000[INTEGER],false]
     *     \_Aggregate[[],[SUM(salary{f}#15,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$sum(salary)_+_3$1#22]]
     *       \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     * }</pre>
     *
     */
    public void testReplaceStatsFilteredAggWithEvalFilterFalseAndNull() {
        var plan = plan("""
            from test
            | stats sum(salary) + 1 where false,
                    sum(salary) + 3,
                    sum(salary) + 2 where null,
                    sum(salary) + 4 where not true
            """);

        var project = as(plan, Project.class);
        assertThat(
            Expressions.names(project.projections()),
            contains("sum(salary) + 1 where false", "sum(salary) + 3", "sum(salary) + 2 where null", "sum(salary) + 4 where not true")
        );
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(4));

        var alias = as(eval.fields().getFirst(), Alias.class);
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));

        alias = as(eval.fields().get(0), Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("sum(salary) + 1"));

        alias = as(eval.fields().get(1), Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("sum(salary) + 3"));

        alias = as(eval.fields().get(2), Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("sum(salary) + 2"));

        alias = as(eval.fields().get(3), Alias.class);
        assertThat(Expressions.name(alias.child()), containsString("sum(salary) + 4"));

        alias = as(eval.fields().getLast(), Alias.class);
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));

        var limit = as(eval.child(), Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        var source = as(aggregate.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER]]
     * \_LocalRelation[[count(salary) where false{r}#3],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     */
    public void testReplaceStatsFilteredAggWithEvalNotTrue() {
        var plan = plan("""
            from test
            | stats count(salary) where not true
            """);

        var limit = as(plan, Limit.class);
        var source = as(limit.child(), LocalRelation.class);
        assertThat(Expressions.names(source.output()), contains("count(salary) where not true"));
        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        var block = as(page.getBlock(0), LongVectorBlock.class);
        assertThat(block.getPositionCount(), is(1));
        assertThat(block.asVector().getLong(0), is(0L));
    }

    /*
     * Limit[1000[INTEGER],false]
     * \_Aggregate[[],[COUNT(salary{f}#10,true[BOOLEAN]) AS m1#4]]
     * \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testReplaceStatsFilteredAggWithEvalNotFalse() {
        var plan = plan("""
            from test
            | stats m1 = count(salary) where not false
            """);
        var limit = as(plan, Limit.class);
        var aggregate = as(limit.child(), Aggregate.class);
        assertEquals(1, aggregate.aggregates().size());
        assertEquals(1, aggregate.aggregates().get(0).children().size());
        assertTrue(aggregate.aggregates().get(0).children().get(0) instanceof Count);
        assertEquals("true", (((Count) aggregate.aggregates().get(0).children().get(0)).filter().toString()));
        assertThat(Expressions.names(aggregate.aggregates()), contains("m1"));
        var source = as(aggregate.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_LocalRelation[[count(salary) where false{r}#3],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalCount() {
        var plan = plan("""
            from test
            | stats count(salary) where false
            """);

        var limit = as(plan, Limit.class);
        var source = as(limit.child(), LocalRelation.class);
        assertThat(Expressions.names(source.output()), contains("count(salary) where false"));
        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        var block = as(page.getBlock(0), LongVectorBlock.class);
        assertThat(block.getPositionCount(), is(1));
        assertThat(block.asVector().getLong(0), is(0L));
    }

    /**
     * <pre>{@code
     * Project[[count_distinct(salary + 2) + 3 where false{r}#3]]
     * \_Eval[[$$COUNTDISTINCT$count_distinct(>$0{r$}#15 + 3[INTEGER] AS count_distinct(salary + 2) + 3 where false]]
     *   \_Limit[1000[INTEGER]]
     *     \_LocalRelation[[$$COUNTDISTINCT$count_distinct(>$0{r$}#15],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalCountDistinctInExpression() {
        var plan = plan("""
            from test
            | stats count_distinct(salary + 2) + 3 where false
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("count_distinct(salary + 2) + 3 where false"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("count_distinct(salary + 2) + 3 where false"));
        var add = as(alias.child(), Add.class);
        var literal = as(add.right(), Literal.class);
        assertThat(literal.value(), is(3));

        var limit = as(eval.child(), Limit.class);
        var source = as(limit.child(), LocalRelation.class);

        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        var block = as(page.getBlock(0), LongVectorBlock.class);
        assertThat(block.getPositionCount(), is(1));
        assertThat(block.asVector().getLong(0), is(0L));
    }

    /**
     * <pre>{@code
     * Project[[max{r}#91, max_a{r}#94, min{r}#97, min_a{r}#100, emp_no{f}#101]]
     * \_Eval[[null[INTEGER] AS max_a, null[INTEGER] AS min_a]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[STANDARD,[emp_no{f}#101],[MAX(salary{f}#106,true[BOOLEAN]) AS max, MIN(salary{f}#106,true[BOOLEAN]) AS min, emp_
     * no{f}#101]]
     *       \_EsRelation[test][_meta_field{f}#107, emp_no{f}#101, first_name{f}#10..]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalSameAggWithAndWithoutFilter() {
        var plan = plan("""
            from test
            | stats max = max(salary), max_a = max(salary) where null,
                    min = min(salary), min_a = min(salary) where to_string(null) == "abc"
              by emp_no
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("max", "max_a", "min", "min_a", "emp_no"));
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(2));

        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(Expressions.name(alias), containsString("max_a"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(INTEGER));

        alias = as(eval.fields().getLast(), Alias.class);
        assertThat(Expressions.name(alias), containsString("min_a"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(INTEGER));

        var limit = as(eval.child(), Limit.class);

        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("max", "min", "emp_no"));

        var source = as(aggregate.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_LocalRelation[[count{r}#7],[LongVectorBlock[vector=ConstantLongVector[positions=1, value=0]]]]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634") // i.e. PropagateEvalFoldables applicability to Aggs
    public void testReplaceStatsFilteredAggWithEvalFilterUsingEvaledValue() {
        var plan = plan("""
            from test
            | eval my_length = length(concat(first_name, null))
            | stats count = count(my_length) where my_length > 0
            """);

        var limit = as(plan, Limit.class);
        var source = as(limit.child(), LocalRelation.class);
        assertThat(Expressions.names(source.output()), contains("count"));
        Page page = source.supplier().get();
        assertThat(page.getBlockCount(), is(1));
        var block = as(page.getBlock(0), LongVectorBlock.class);
        assertThat(block.getPositionCount(), is(1));
        assertThat(block.asVector().getLong(0), is(0L));
    }

    /**
     *
     * <pre>{@code
     * Project[[c{r}#67, emp_no{f}#68]]
     * \_Eval[[0[LONG] AS c]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[STANDARD,[emp_no{f}#68],[emp_no{f}#68]]
     *       \_EsRelation[test][_meta_field{f}#74, emp_no{f}#68, first_name{f}#69, ..]
     * }</pre>
     */
    public void testReplaceStatsFilteredAggWithEvalSingleAggWithGroup() {
        var plan = plan("""
            from test
            | stats c = count(emp_no) where false
              by emp_no
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c", "emp_no"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(Expressions.name(alias), containsString("c"));

        var limit = as(eval.child(), Limit.class);

        var aggregate = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("emp_no"));

        var source = as(aggregate.child(), EsRelation.class);
    }

    /*
     * Project[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13, lang
     * uages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9, sum(salary) where false{r}#3]]
     * \_Eval[[null[LONG] AS sum(salary) where false#3]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalSingleAgg() {
        var query = """
            FROM test
            | INLINE STATS sum(salary) where false
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, Project.class);
        assertMap(
            Expressions.names(project.projections()).stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field"))
                .item(startsWith("emp_no"))
                .item(startsWith("first_name"))
                .item(startsWith("gender"))
                .item(startsWith("hire_date"))
                .item(startsWith("job"))
                .item(startsWith("job.raw"))
                .item(startsWith("languages"))
                .item(startsWith("last_name"))
                .item(startsWith("long_noidx"))
                .item(startsWith("salary"))
                .item(containsString("sum(salary) where false"))
        );
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(Expressions.name(alias), containsString("sum(salary) where false"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));
        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /*
     * Project[[_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, gender{f}#6, hire_date{f}#11, job{f}#12, job.raw{f}#13, lang
     * uages{f}#7, last_name{f}#8, long_noidx{f}#14, salary{f}#9, sum(salary) + 1 where false{r}#3]]
     * \_Eval[[null[LONG] AS sum(salary) + 1 where false#3]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#10, emp_no{f}#4, first_name{f}#5, ge..]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalSingleAggWithExpression() {
        var query = """
            FROM test
            | INLINE STATS sum(salary) + 1 where false
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, Project.class);
        assertMap(
            Expressions.names(project.projections()).stream().map(Object::toString).toList(),
            matchesList().item(startsWith("_meta_field"))
                .item(startsWith("emp_no"))
                .item(startsWith("first_name"))
                .item(startsWith("gender"))
                .item(startsWith("hire_date"))
                .item(startsWith("job"))
                .item(startsWith("job.raw"))
                .item(startsWith("languages"))
                .item(startsWith("last_name"))
                .item(startsWith("long_noidx"))
                .item(startsWith("salary"))
                .item(containsString("sum(salary) + 1 where false"))
        );
        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(Expressions.name(alias), containsString("sum(salary) + 1 where false"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));
        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],false]
     * \_InlineJoin[LEFT,[emp_no{f}#9],[emp_no{f}#9],[emp_no{r}#9]]
     *   |_EsqlProject[[salary{f}#14, emp_no{f}#9]]
     *   | \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     *   \_Project[[sum(salary)   1 where false{r}#5, sum(salary)   2{r}#7, emp_no{f}#9]]
     *     \_Eval[[null[LONG] AS sum(salary)   1 where false#5, $$SUM$sum(salary)_ _2$1{r$}#21   2[INTEGER] AS sum(salary)   2#7]]
     *       \_Aggregate[[emp_no{f}#9],[SUM(salary{f}#14,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$sum(salary)_ _2$1#21, emp_no{f}#9]]
     *         \_StubRelation[[salary{f}#14, emp_no{f}#9]]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalMixedFilterAndNoFilter() {
        var query = """
            FROM test
            | KEEP salary, emp_no
            | INLINE STATS sum(salary) + 1 where false,
                    sum(salary) + 2
              BY emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var limit = as(plan, Limit.class);
        var ij = as(limit.child(), InlineJoin.class);
        var left = as(ij.left(), EsqlProject.class);
        assertThat(Expressions.names(left.projections()), contains("salary", "emp_no"));
        var relation = as(left.child(), EsRelation.class);
        var right = as(ij.right(), Project.class);
        assertMap(
            Expressions.names(right.projections()).stream().map(Object::toString).toList(),
            matchesList().item(startsWith("sum(salary) + 1 where false")).item(startsWith("sum(salary) + 2")).item(startsWith("emp_no"))
        );
        var eval = as(right.child(), Eval.class);
        assertThat(eval.fields().size(), is(2));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), nullValue());
        assertThat(alias.child().dataType(), is(LONG));
        assertThat(Expressions.name(alias), containsString("sum(salary) + 1 where false"));
        alias = as(eval.fields().get(1), Alias.class);
        assertFalse(alias.child().foldable());
        assertThat(alias.child().dataType(), is(LONG));
        assertThat(Expressions.name(alias), containsString("sum(salary) + 2"));
        assertThat(alias.child() instanceof Add, is(true));
        var add = as(alias.child(), Add.class);
        assertThat(add.left().toString(), containsString("$$SUM$sum(salary)_+_2$1"));
        var agg = as(eval.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("$$SUM$sum(salary)_+_2$1", "emp_no"));
        var source = as(agg.child(), StubRelation.class);
        assertThat(Expressions.names(source.output()), contains("salary", "emp_no"));
    }

    /**
     * Limit[1000[INTEGER],true]
     * \_InlineJoin[LEFT,[],[],[]]
     *   |_EsqlProject[[salary{f}#16]]
     *   | \_Limit[1000[INTEGER],false]
     *   |   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *   \_Project[[sum(salary)   1 where false{r}#4, sum(salary)   3{r}#6, sum(salary)   2 where null{r}#8, sum(salary)   4 wher
     * e not true{r}#10]]
     *     \_Eval[[null[LONG] AS sum(salary)   1 where false#4, $$SUM$sum(salary)_ _3$1{r$}#23   3[INTEGER] AS sum(salary)   3#6
     * , null[LONG] AS sum(salary)   2 where null#8, null[LONG] AS sum(salary)   4 where not true#10]]
     *       \_Aggregate[[],[SUM(salary{f}#16,true[BOOLEAN],compensated[KEYWORD]) AS $$SUM$sum(salary)_ _3$1#23]]
     *         \_StubRelation[[salary{f}#16]]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalFilterFalseAndNull() {
        var query = """
            FROM test
            | KEEP salary
            | INLINE STATS sum(salary) + 1 where false,
                    sum(salary) + 3,
                    sum(salary) + 2 where null,
                    sum(salary) + 4 where not true
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var limit = as(plan, Limit.class);
        var ij = as(limit.child(), InlineJoin.class);

        // Check right side Project node
        var right = as(ij.right(), Project.class);
        assertThat(
            Expressions.names(right.projections()),
            contains("sum(salary) + 1 where false", "sum(salary) + 3", "sum(salary) + 2 where null", "sum(salary) + 4 where not true")
        );

        var eval = as(right.child(), Eval.class);
        assertThat(eval.fields().size(), is(4));

        var alias1 = as(eval.fields().get(0), Alias.class);
        assertTrue(alias1.child().foldable());
        assertThat(alias1.child().fold(FoldContext.small()), nullValue());

        var alias2 = as(eval.fields().get(1), Alias.class);
        assertFalse(alias2.child().foldable());

        var alias3 = as(eval.fields().get(2), Alias.class);
        assertTrue(alias3.child().foldable());
        assertThat(alias3.child().fold(FoldContext.small()), nullValue());

        var alias4 = as(eval.fields().get(3), Alias.class);
        assertTrue(alias4.child().foldable());
        assertThat(alias4.child().fold(FoldContext.small()), nullValue());

        var aggregate = as(eval.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("$$SUM$sum(salary)_+_3$1"));

        var source = as(aggregate.child(), StubRelation.class);
        assertThat(Expressions.names(source.output()), contains("salary"));
    }

    /**
     * EsqlProject[[emp_no{f}#6, salary{f}#11, count(salary) where not true{r}#5]]
     * \_Eval[[0[LONG] AS count(salary) where not true#5]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalNotTrue() {
        var query = """
            FROM test
            | KEEP emp_no, salary
            | INLINE STATS count(salary) where not true
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "salary", "count(salary) where not true"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("count(salary) where not true"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), is(0L));

        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],true]
     * \_InlineJoin[LEFT,[],[],[]]
     *   |_EsqlProject[[emp_no{f}#8, salary{f}#13, gender{f}#10]]
     *   | \_EsRelation[test][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *   \_Aggregate[[],[COUNT(salary{f}#13,true[BOOLEAN]) AS m1#7]]
     *     \_StubRelation[[emp_no{f}#8, salary{f}#13, gender{f}#10]]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalNotFalse() {
        var query = """
            FROM test
            | KEEP emp_no, salary, gender
            | INLINE STATS m1 = count(salary) WHERE not false
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var limit = as(plan, Limit.class);
        var ij = as(limit.child(), InlineJoin.class);

        var left = as(ij.left(), EsqlProject.class);
        assertThat(Expressions.names(left.projections()), contains("emp_no", "salary", "gender"));
        var relation = as(left.child(), EsRelation.class);

        var right = as(ij.right(), Aggregate.class);
        assertThat(Expressions.names(right.aggregates()), contains("m1"));
        assertEquals(1, right.aggregates().size());
        assertTrue(right.aggregates().get(0).children().get(0) instanceof Count);
        assertEquals("true", ((Count) right.aggregates().get(0).children().get(0)).filter().toString());

        var source = as(right.child(), StubRelation.class);
        assertThat(Expressions.names(source.output()), contains("emp_no", "salary", "gender"));
    }

    /**
     * EsqlProject[[salary{f}#10, count(salary) where false{r}#4]]
     * \_Eval[[0[LONG] AS count(salary) where false#4]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalCount() {
        var query = """
            FROM test
            | KEEP salary
            | INLINE STATS count(salary) where false
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("salary", "count(salary) where false"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("count(salary) where false"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), is(0L));

        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /**
     * EsqlProject[[salary{f}#10, count_distinct(salary   2)   3 where false{r}#4]]
     * \_Eval[[3[LONG] AS count_distinct(salary   2)   3 where false#4]]
     *   \_Limit[1000[INTEGER],false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalCountDistinctInExpression() {
        var query = """
            FROM test
            | KEEP salary
            | INLINE STATS count_distinct(salary + 2) + 3 where false
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("salary", "count_distinct(salary + 2) + 3 where false"));

        var eval = as(project.child(), Eval.class);
        assertThat(eval.fields().size(), is(1));
        var alias = as(eval.fields().getFirst(), Alias.class);
        assertThat(alias.name(), is("count_distinct(salary + 2) + 3 where false"));
        assertTrue(alias.child().foldable());
        assertThat(alias.child().fold(FoldContext.small()), is(3L));

        var limit = as(eval.child(), Limit.class);
        as(limit.child(), EsRelation.class);
    }

    /*
     * Limit[1000[INTEGER],true]
     * \_InlineJoin[LEFT,[emp_no{f}#17],[emp_no{f}#17],[emp_no{r}#17]]
     *   |_EsqlProject[[emp_no{f}#17, salary{f}#22]]
     *   | \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *   \_Project[[max{r}#6, max_a{r}#9, min{r}#12, min_a{r}#15, emp_no{f}#17]]
     *     \_Eval[[null[INTEGER] AS max_a#9, null[INTEGER] AS min_a#15]]
     *       \_Aggregate[[emp_no{f}#17],[MAX(salary{f}#22,true[BOOLEAN]) AS max#6, MIN(salary{f}#22,true[BOOLEAN]) AS min#12, emp_no{f}#17]]
     *         \_StubRelation[[emp_no{f}#17, salary{f}#22]]
     */
    public void testReplaceInlineStatsFilteredAggWithEvalSameAggWithAndWithoutFilter() {
        var query = """
            FROM test
            | KEEP emp_no, salary
            | INLINE STATS max = max(salary), max_a = max(salary) WHERE null,
                    min = min(salary),
                    min_a = min(salary) WHERE to_string(null) == "abc"
              BY emp_no
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var limit = as(plan, Limit.class);
        var ij = as(limit.child(), InlineJoin.class);

        var left = as(ij.left(), EsqlProject.class);
        assertThat(Expressions.names(left.projections()), contains("emp_no", "salary"));
        var relation = as(left.child(), EsRelation.class);

        var right = as(ij.right(), Project.class);
        assertThat(Expressions.names(right.projections()), contains("max", "max_a", "min", "min_a", "emp_no"));

        var eval = as(right.child(), Eval.class);
        assertThat(eval.fields().size(), is(2));

        var aliasMaxA = as(eval.fields().get(0), Alias.class);
        assertThat(Expressions.name(aliasMaxA), containsString("max_a"));
        assertTrue(aliasMaxA.child().foldable());
        assertThat(aliasMaxA.child().fold(FoldContext.small()), nullValue());
        assertThat(aliasMaxA.child().dataType(), is(INTEGER));

        var aliasMinA = as(eval.fields().get(1), Alias.class);
        assertThat(Expressions.name(aliasMinA), containsString("min_a"));
        assertTrue(aliasMinA.child().foldable());
        assertThat(aliasMinA.child().fold(FoldContext.small()), nullValue());
        assertThat(aliasMinA.child().dataType(), is(INTEGER));

        var aggregate = as(eval.child(), Aggregate.class);
        assertThat(Expressions.names(aggregate.aggregates()), contains("max", "min", "emp_no"));

        var source = as(aggregate.child(), StubRelation.class);
        assertThat(Expressions.names(source.output()), contains("emp_no", "salary"));
    }

    /*
     * EsqlProject[[emp_no{f}#9, count{r}#5, cc{r}#8]]
     * \_TopN[[Order[emp_no{f}#9,ASC,LAST]],3[INTEGER]]
     *   \_Eval[[0[LONG] AS count#5, 0[LONG] AS cc#8]]
     *     \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     */
    public void testReplaceTwoConsecutiveInlineStats_WithFalseFilters() {
        var query = """
            FROM test
                | KEEP emp_no
                | INLINE STATS count = count(*) WHERE false
                | INLINE STATS cc = count_distinct(emp_no) WHERE false
                | SORT emp_no
                | LIMIT 3
            """;
        if (releaseBuildForInlineStats(query)) {
            return;
        }
        var plan = plan(query);
        var project = as(plan, EsqlProject.class);
        assertThat(Expressions.names(project.projections()), contains("emp_no", "count", "cc"));

        var topN = as(project.child(), TopN.class);
        var eval = as(topN.child(), Eval.class);
        assertThat(eval.fields().size(), is(2));

        var aliasCount = as(eval.fields().get(0), Alias.class);
        assertThat(Expressions.name(aliasCount), startsWith("count"));
        assertTrue(aliasCount.child().foldable());
        assertThat(aliasCount.child().fold(FoldContext.small()), is(0L));

        var aliasCc = as(eval.fields().get(1), Alias.class);
        assertThat(Expressions.name(aliasCc), startsWith("cc"));
        assertTrue(aliasCc.child().foldable());
        assertThat(aliasCc.child().fold(FoldContext.small()), is(0L));
        as(eval.child(), EsRelation.class);
    }
}
