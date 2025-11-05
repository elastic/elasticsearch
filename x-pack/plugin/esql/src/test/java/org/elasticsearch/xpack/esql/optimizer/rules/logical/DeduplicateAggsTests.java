/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class DeduplicateAggsTests extends AbstractLogicalPlanOptimizerTests {
    /**
     * <pre>{@code
     * Project[[s{r}#4 AS d, s{r}#4, last_name{f}#21, first_name{f}#18]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[last_name{f}#21, first_name{f}#18],[SUM(salary{f}#22) AS s, last_name{f}#21, first_name{f}#18]]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testCombineProjectionWithDuplicateAggregation() {
        var plan = plan("""
            from test
            | stats s = sum(salary), d = sum(salary), c = sum(salary) by last_name, first_name
            | keep d, s, last_name, first_name
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("d", "s", "last_name", "first_name"));
        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        assertThat(Expressions.names(agg.aggregates()), contains("s", "last_name", "first_name"));
        assertThat(Alias.unwrap(agg.aggregates().get(0)), instanceOf(Sum.class));
        assertThat(Expressions.names(agg.groupings()), contains("last_name", "first_name"));
    }

    /**
     * <pre>{@code
     * Project[[a{r}#8, b{r}#12, c{r}#15, c{r}#15 AS d#18]]
     * \_Eval[[a{r}#8 + a{r}#8 AS b#12]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[COUNT(scalerank{f}#21,true[BOOLEAN],PT0S[TIME_DURATION]) AS a#8, COUNTDISTINCT(scalerank{f}#21,true[BOOLEAN],PT0S[TIME_DURATION],10[INTEGER]) AS c#15]]
     *       \_EsRelation[airports][abbrev{f}#19, city{f}#25, city_location{f}#26, coun..]
     * }</pre>
     */
    public void testAggsDeduplication() {
        assumeTrue("requires FIX FOR CLASSCAST exception to be enabled", EsqlCapabilities.Cap.FIX_STATS_CLASSCAST_EXCEPTION.isEnabled());
        String query = """
                FROM airports
                | rename scalerank AS x
                | stats  a = count(x), b = count(x) + count(x), c = count_distinct(x, 10), d = count_distinct(x, 10 + 1 - 1)
            """;

        LogicalPlan plan = planAirports(query);
        Project project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(4));
        var a = as(project.projections().get(0), ReferenceAttribute.class);
        var b = as(project.projections().get(1), ReferenceAttribute.class);
        var c = as(project.projections().get(2), ReferenceAttribute.class);
        var d = as(project.projections().get(3), Alias.class);

        assertEquals("a", a.name());
        assertEquals("b", b.name());
        assertEquals("c", c.name());
        assertEquals("d", d.name());
        assertEquals("c", as(d.child(), ReferenceAttribute.class).name());

        Eval eval = as(project.child(), Eval.class);
        assertThat(eval.fields(), hasSize(1));
        var bEval = as(eval.fields().getFirst(), Alias.class);
        var bAdd = as(bEval.child(), Add.class);

        assertEquals("a", as(bAdd.left(), ReferenceAttribute.class).name());
        assertEquals("a", as(bAdd.right(), ReferenceAttribute.class).name());
        assertEquals("b", bEval.name());

        Limit limit = as(eval.child(), Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);

        assertThat(agg.aggregates(), hasSize(2));
        var countAlias = as(agg.aggregates().get(0), Alias.class);
        var countDistinctAliasFirst = as(agg.aggregates().get(1), Alias.class);

        var count = as(countAlias.child(), Count.class);
        var countDistinct = as(countDistinctAliasFirst.child(), CountDistinct.class);

        assertEquals("a", countAlias.name());
        assertEquals("c", countDistinctAliasFirst.name());
        assertEquals("scalerank", as(count.field(), FieldAttribute.class).name());
        assertEquals("scalerank", as(countDistinct.field(), FieldAttribute.class).name());
    }
}
