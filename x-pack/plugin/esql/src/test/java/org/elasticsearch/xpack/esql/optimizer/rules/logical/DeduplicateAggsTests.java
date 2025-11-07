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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
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
        var projections = project.projections();
        assertThat(projections, hasSize(4));
        var a = as(projections.get(0), ReferenceAttribute.class);
        var b = as(projections.get(1), ReferenceAttribute.class);
        var c = as(projections.get(2), ReferenceAttribute.class);
        var d = as(projections.get(3), Alias.class);

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

    /**
     * <pre>{@code
     * Project[[MAX(y){r}#16, 2* MAX(a){r}#18]]
     * \_Eval[[MAX(y){r}#16 * 2[INTEGER] AS 2* MAX(a)#18]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[MAX(scalerank{f}#21,true[BOOLEAN],PT0S[TIME_DURATION]) AS MAX(y)#16]]
     *       \_EsRelation[airports][abbrev{f}#19, city{f}#25, city_location{f}#26, coun..]
     * }</pre>
     */
    public void testAggsDeduplicationWithComplicatedAliasChains() {
        assumeTrue("requires FIX FOR CLASSCAST exception to be enabled", EsqlCapabilities.Cap.FIX_STATS_CLASSCAST_EXCEPTION.isEnabled());
        String query = """
                FROM airports
                | rename scalerank AS x
                | EVAL y = x, z = x
                | RENAME z AS a
                | STATS MAX(y), 2* MAX(a)
            """;

        LogicalPlan plan = planAirports(query);
        Project project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(2));

        var firstResult = as(projections.get(0), ReferenceAttribute.class);
        var secondResult = as(projections.get(1), ReferenceAttribute.class);
        assertEquals("MAX(y)", firstResult.name());
        assertEquals("2* MAX(a)", secondResult.name());

        var eval = as(project.child(), Eval.class);
        var outputs = eval.output();
        assertThat(outputs, hasSize(2));

        assertEquals("MAX(y)", as(outputs.get(0), ReferenceAttribute.class).name());
        assertEquals("2* MAX(a)", as(outputs.get(1), ReferenceAttribute.class).name());
        assertThat(eval.fields(), hasSize(1));
        var twoMax = as(eval.fields().getFirst(), Alias.class);
        var mul = as(twoMax.child(), Mul.class);
        assertEquals("MAX(y)", as(mul.left(), ReferenceAttribute.class).name());
        assertEquals(2, as(mul.right(), Literal.class).value());
        assertEquals("2* MAX(a)", twoMax.name());


        var aggregates = as(as(eval.child(), Limit.class).child(), Aggregate.class).aggregates();
        assertThat(aggregates, hasSize(1));
        var agg = as(aggregates.getFirst(), Alias.class);
        assertEquals("MAX(y)", agg.name());
        var maxY = as(agg.child(), Max.class);
        assertEquals("scalerank", as(maxY.field(), FieldAttribute.class).name());
    }

    /**
     * <pre>{@code
     * Project[[COUNT(y){r}#7, 2 * COUNT(scalerank){r}#9, y{r}#5]]
     * \_Eval[[COUNT(y){r}#7 * 2[INTEGER] AS 2 * COUNT(scalerank)#9]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[scalerank{f}#13],[COUNT(scalerank{f}#13,true[BOOLEAN],PT0S[TIME_DURATION]) AS COUNT(y)#7, scalerank{f}#13 AS y
     * #5]]
     *       \_EsRelation[airports][abbrev{f}#11, city{f}#17, city_location{f}#18, coun..]
     * }</pre>
     */
    public void testAggsDeduplicationInByClauses() {
        assumeTrue("requires FIX FOR CLASSCAST exception to be enabled", EsqlCapabilities.Cap.FIX_STATS_CLASSCAST_EXCEPTION.isEnabled());
        String query = """
                FROM airports
                | STATS COUNT(y), 2 * COUNT(scalerank) BY y = scalerank
            """;

        LogicalPlan plan = planAirports(query);
        Project project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(3));

        assertEquals("COUNT(y)", as(projections.get(0), ReferenceAttribute.class).name());
        assertEquals("2 * COUNT(scalerank)", as(projections.get(1), ReferenceAttribute.class).name());
        assertEquals("y", as(projections.get(2), ReferenceAttribute.class).name());

        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var evalAlias = as(eval.fields().getFirst(), Alias.class);
        var mul = as(evalAlias.child(), Mul.class);
        assertEquals("COUNT(y)", as(mul.left(), ReferenceAttribute.class).name());
        assertEquals(2, as(mul.right(), Literal.class).value());
        assertEquals("2 * COUNT(scalerank)", evalAlias.name());

        var aggregates = as(as(eval.child(), Limit.class).child(), Aggregate.class).aggregates();
        assertThat(aggregates, hasSize(2));

        var countAlias = as(aggregates.getFirst(), Alias.class);
        assertEquals("COUNT(y)", countAlias.name());
        var countField = as(countAlias.child(), Count.class).field();
        assertEquals("scalerank", as(countField, FieldAttribute.class).name());
        assertEquals("scalerank", as(as(aggregates.get(1), Alias.class).child(), FieldAttribute.class).name());
    }

    /**
     * <pre>{@code
     * Project[[a{r}#5, b{r}#9, c{r}#13]]
     * \_Eval[[a{r}#5 * 2[INTEGER] AS b#9]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[COUNT(scalerank{f}#16,scalerank{f}#16 > 7[INTEGER],PT0S[TIME_DURATION]) AS a#5, COUNTDISTINCT(scalerank{f}#
     * 16,true[BOOLEAN],PT0S[TIME_DURATION]) AS c#13]]
     *       \_EsRelation[airports][abbrev{f}#14, city{f}#20, city_location{f}#21, coun..]
     * }</pre>
     */
    public void testDuplicatedAggsWithSameCannonicalizationInWhereCondition() {
        assumeTrue("requires FIX FOR CLASSCAST exception to be enabled", EsqlCapabilities.Cap.FIX_STATS_CLASSCAST_EXCEPTION.isEnabled());
        String query = """
                FROM airports
                | STATS a = COUNT(scalerank) WHERE scalerank  > 7,
                        b = 2*COUNT(scalerank) WHERE 7 < scalerank,
                        c = COUNT_DISTINCT(scalerank)
            """;

        LogicalPlan plan = planAirports(query);
        Project project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(3));

        assertEquals("a", as(projections.get(0), ReferenceAttribute.class).name());
        assertEquals("b", as(projections.get(1), ReferenceAttribute.class).name());
        assertEquals("c", as(projections.get(2), ReferenceAttribute.class).name());

        var eval = as(project.child(), Eval.class);

        assertThat(eval.fields(), hasSize(1));
        var evalAlias = as(eval.fields().getFirst(), Alias.class);
        var mul = as(evalAlias.child(), Mul.class);
        assertEquals("a", as(mul.left(), ReferenceAttribute.class).name());
        assertEquals(2, as(mul.right(), Literal.class).value());
        assertEquals("b", evalAlias.name());

        var aggregates = as(as(eval.child(), Limit.class).child(), Aggregate.class).aggregates();
        assertThat(aggregates, hasSize(2));

        var a = as(aggregates.get(0), Alias.class);
        var c = as(aggregates.get(1), Alias.class);

        assertEquals("a", a.name());
        var aField = as(a.child(), Count.class).field();
        assertEquals("scalerank", as(aField, FieldAttribute.class).name());

        assertEquals("c", c.name());
        var cField = as(c.child(), CountDistinct.class).field();
        assertEquals("scalerank", as(cField, FieldAttribute.class).name());
    }

    /**
     * Project[[a{r}#5, b{r}#8, c{r}#11]]
     * \_Eval[[$$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[INTEGER] AS a#5, $$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[
     * INTEGER] AS b#8, $$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[INTEGER] AS c#11]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[COUNTDISTINCT(scalerank{f}#14,true[BOOLEAN],PT0S[TIME_DURATION],100[INTEGER]) AS $$COUNTDISTINCT$2*COUNT_DI
     * STINC>$0#20]]
     *       \_EsRelation[airports][abbrev{f}#12, city{f}#18, city_location{f}#19, coun..]
     */
    public void testDuplicatedAggWithFoldableIdenticalExpressions() {
        assumeTrue("requires FIX FOR CLASSCAST exception to be enabled", EsqlCapabilities.Cap.FIX_STATS_CLASSCAST_EXCEPTION.isEnabled());
        String query = """
                FROM airports
                | STATS a = 2*COUNT_DISTINCT(scalerank, 100),
                b = 2*COUNT_DISTINCT(scalerank, 220 - 150 + 30),
                c = 2*COUNT_DISTINCT(scalerank, 1 + 200 - 80 - 20 - 1)
            """;

        LogicalPlan plan = planAirports(query);
        Project project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(3));

        assertEquals("a", as(projections.get(0), ReferenceAttribute.class).name());
        assertEquals("b", as(projections.get(1), ReferenceAttribute.class).name());
        assertEquals("c", as(projections.get(2), ReferenceAttribute.class).name());

        var eval = as(project.child(), Eval.class);
        var aggregates = as(as(eval.child(), Limit.class).child(), Aggregate.class).aggregates();

        assertThat(eval.fields(), hasSize(3));
        var firstEvalField = as(as(eval.fields().get(0), Alias.class).child(), Mul.class).left();
        var secondEvalField = as(as(eval.fields().get(1), Alias.class).child(), Mul.class).left();
        var thirdEvalField = as(as(eval.fields().get(2), Alias.class).child(), Mul.class).left();
        assertEquals(firstEvalField, secondEvalField);
        assertEquals(secondEvalField, thirdEvalField);

        assertThat(aggregates, hasSize(1));
        var countDistinct = as(as(aggregates.get(0), Alias.class).child(), CountDistinct.class);
        assertEquals("scalerank", as(countDistinct.field(), FieldAttribute.class).name());
    }
}
