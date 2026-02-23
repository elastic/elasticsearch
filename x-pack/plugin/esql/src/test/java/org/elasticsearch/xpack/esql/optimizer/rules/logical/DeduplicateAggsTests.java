/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class DeduplicateAggsTests extends AbstractLogicalPlanOptimizerTests {
    public static <T extends AggregateFunction> void aggFieldName(Expression exp, Class<T> aggType, String fieldName) {
        var alias = as(exp, Alias.class);
        var af = as(alias.child(), aggType);
        var field = af.field();
        var name = field.foldable() ? BytesRefs.toString(field.fold(FoldContext.small())) : Expressions.name(field);
        assertThat(name, is(fieldName));
    }

    public static <T> T aliased(Expression exp, Class<T> clazz) {
        var alias = as(exp, Alias.class);
        return as(alias.child(), clazz);
    }

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
     * Expects
     * <pre>{@code
     * Project[[c1{r}#2, c2{r}#4, cs{r}#6, cm{r}#8, cexp{r}#10]]
     * \_Eval[[c1{r}#2 AS c2, c1{r}#2 AS cs, c1{r}#2 AS cm, c1{r}#2 AS cexp]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsCountAll() {
        var plan = plan("""
              from test
            | stats c1 = count(1), c2 = count(2), cs = count(*), cm = count(), cexp = count("123")
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "c2", "cs", "cm", "cexp"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("c2", "cs", "cm", "cexp"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[a{r}#5, b{r}#9, $$max(salary)_+_3>$COUNT$2{r}#46 AS d, $$count(salary)_->$MIN$3{r}#47 AS e, $$avg(salary)_+_m
     * >$MAX$1{r}#45 AS g]]
     * \_Eval[[$$$$avg(salary)_+_m>$AVG$0$SUM$0{r}#48 / $$max(salary)_+_3>$COUNT$2{r}#46 AS $$avg(salary)_+_m>$AVG$0, $$avg(
     * salary)_+_m>$AVG$0{r}#44 + $$avg(salary)_+_m>$MAX$1{r}#45 AS a, $$avg(salary)_+_m>$MAX$1{r}#45 + 3[INTEGER] +
     * 3.141592653589793[DOUBLE] + $$max(salary)_+_3>$COUNT$2{r}#46 AS b]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[w{r}#28],[SUM(salary{f}#39) AS $$$$avg(salary)_+_m>$AVG$0$SUM$0, MAX(salary{f}#39) AS $$avg(salary)_+_m>$MAX$1
     * , COUNT(salary{f}#39) AS $$max(salary)_+_3>$COUNT$2, MIN(salary{f}#39) AS $$count(salary)_->$MIN$3]]
     *       \_Eval[[languages{f}#37 % 2[INTEGER] AS w]]
     *         \_EsRelation[test][_meta_field{f}#40, emp_no{f}#34, first_name{f}#35, ..]
     * }</pre>
     */
    public void testStatsExpOverAggsWithScalarAndDuplicateAggs() {
        var plan = optimizedPlan("""
            from test
            | stats a = avg(salary) + max(salary),
                    b = max(salary) + 3 + PI() + count(salary),
                    c = count(salary) - min(salary),
                    d = count(salary),
                    e = min(salary),
                    f = max(salary),
                    g = max(salary)
                    by w = languages % 2
            | keep a, b, d, e, g
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("a", "b", "d", "e", "g"));
        var refA = Alias.unwrap(projections.get(0));
        var refB = Alias.unwrap(projections.get(1));
        var refD = Alias.unwrap(projections.get(2));
        var refE = Alias.unwrap(projections.get(3));
        var refG = Alias.unwrap(projections.get(4));

        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        // avg = Sum/Count
        assertThat(Expressions.name(fields.get(0)), containsString("AVG"));
        assertThat(Alias.unwrap(fields.get(0)), instanceOf(Div.class));
        // avg + max
        assertThat(Expressions.name(fields.get(1)), is("a"));
        var add = as(Alias.unwrap(fields.get(1)), Add.class);
        var max_salary = add.right();
        assertThat(Expressions.attribute(fields.get(1)), is(Expressions.attribute(refA)));

        assertThat(Expressions.name(fields.get(2)), is("b"));
        assertThat(Expressions.attribute(fields.get(2)), is(Expressions.attribute(refB)));

        add = as(Alias.unwrap(fields.get(2)), Add.class);
        add = as(add.left(), Add.class);
        add = as(add.left(), Add.class);
        assertThat(Expressions.attribute(max_salary), is(Expressions.attribute(add.left())));

        var limit = as(eval.child(), Limit.class);

        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        var sum = as(Alias.unwrap(aggs.get(0)), Sum.class);

        assertThat(Expressions.attribute(aggs.get(1)), is(Expressions.attribute(max_salary)));
        var max = as(Alias.unwrap(aggs.get(1)), Max.class);
        var count = as(Alias.unwrap(aggs.get(2)), Count.class);
        var min = as(Alias.unwrap(aggs.get(3)), Min.class);

        eval = as(agg.child(), Eval.class);
        fields = eval.fields();
        assertThat(Expressions.name(fields.get(0)), is("w"));
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[c1{r}#7, cx{r}#10, cs{r}#12, cy{r}#15]]
     * \_Eval[[c1{r}#7 AS cx, c1{r}#7 AS cs, c1{r}#7 AS cy]]
     *   \_Limit[1000[INTEGER]]
     *     \_Aggregate[[],[COUNT([2a][KEYWORD]) AS c1]]
     *       \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggsWithAliasedFields() {
        var plan = plan("""
              from test
            | eval x = 1
            | eval y = x
            | stats c1 = count(1), cx = count(x), cs = count(*), cy = count(y)
            """);

        var project = as(plan, Project.class);
        assertThat(Expressions.names(project.projections()), contains("c1", "cx", "cs", "cy"));
        var eval = as(project.child(), Eval.class);
        var fields = eval.fields();
        assertThat(Expressions.names(fields), contains("cx", "cs", "cy"));
        for (Alias field : fields) {
            assertThat(Expressions.name(field.child()), is("c1"));
        }
        var limit = as(eval.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("c1"));
        aggFieldName(aggs.get(0), Count.class, "*");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[min{r}#1385, max{r}#1388, min{r}#1385 AS min2, max{r}#1388 AS max2, gender{f}#1398]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[gender{f}#1398],[MIN(salary{f}#1401) AS min, MAX(salary{f}#1401) AS max, gender{f}#1398]]
     *     \_EsRelation[test][_meta_field{f}#1402, emp_no{f}#1396, first_name{f}#..]
     * }</pre>
     */
    public void testEliminateDuplicateAggsMixed() {
        var plan = plan("""
              from test
            | stats min = min(salary), max = max(salary), min2 = min(salary), max2 = max(salary) by gender
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("min", "max", "min2", "max2", "gender"));
        as(projections.get(0), ReferenceAttribute.class);
        as(projections.get(1), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("min"));
        assertThat(Expressions.name(aliased(projections.get(3), ReferenceAttribute.class)), is("max"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("min", "max", "gender"));
        aggFieldName(aggs.get(0), Min.class, "salary");
        aggFieldName(aggs.get(1), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Project[[max(x){r}#11, max(x){r}#11 AS max(y), max(x){r}#11 AS max(z)]]
     * \_Limit[1000[INTEGER]]
     *   \_Aggregate[[],[MAX(salary{f}#21) AS max(x)]]
     *     \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     * }</pre>
     */
    public void testEliminateDuplicateAggsNonCount() {
        var plan = plan("""
            from test
            | eval x = salary
            | eval y = x
            | eval z = y
            | stats max(x), max(y), max(z)
            """);

        var project = as(plan, Project.class);
        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("max(x)", "max(y)", "max(z)"));
        as(projections.get(0), ReferenceAttribute.class);
        assertThat(Expressions.name(aliased(projections.get(1), ReferenceAttribute.class)), is("max(x)"));
        assertThat(Expressions.name(aliased(projections.get(2), ReferenceAttribute.class)), is("max(x)"));

        var limit = as(project.child(), Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var aggs = agg.aggregates();
        assertThat(Expressions.names(aggs), contains("max(x)"));
        aggFieldName(aggs.get(0), Max.class, "salary");
        var source = as(agg.child(), EsRelation.class);
    }

    /**
     * Expects
     * <pre>{@code
     * Limit[1000[INTEGER]]
     * \_Aggregate[[salary{f}#12],[salary{f}#12, salary{f}#12 AS x]]
     *   \_EsRelation[test][_meta_field{f}#13, emp_no{f}#7, first_name{f}#8, ge..]
     * }</pre>
     */
    public void testEliminateDuplicateRenamedGroupings() {
        var plan = plan("""
            from test
            | eval x = salary
            | stats by salary, x
            """);

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var relation = as(agg.child(), EsRelation.class);

        assertThat(Expressions.names(agg.groupings()), contains("salary"));
        assertThat(Expressions.names(agg.aggregates()), contains("salary", "x"));
    }

    /**
     * Expects
     * <pre>{@code
     * EsqlProject[[a{r}#5, c{r}#8]]
     * \_Eval[[null[INTEGER] AS x]]
     *   \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/100634")
    public void testEliminateDuplicateAggWithNull() {
        var plan = plan("""
              from test
            | eval x = null + 1
            | stats a = avg(x), c = count(x)
            """);
        fail("Awaits fix");
    }

    /**
     * <pre>{@code
     * Project[[a{r}#8, b{r}#12, c{r}#15, c{r}#15 AS d#18]]
     * \_Eval[[a{r}#8 + a{r}#8 AS b#12]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[],[COUNT(scalerank{f}#21,true[BOOLEAN],PT0S[TIME_DURATION]) AS a#8,
     *     COUNTDISTINCT(scalerank{f}#21,true[BOOLEAN],PT0S[TIME_DURATION],10[INTEGER]) AS c#15]]
     *       \_EsRelation[airports][abbrev{f}#19, city{f}#25, city_location{f}#26, coun..]
     * }</pre>
     */
    public void testAggsDeduplication() {
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
        String query = """
                FROM airports
                | RENAME scalerank AS x
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
        var firstEvalField = as(as(eval.fields().get(0), Alias.class).child(), Mul.class);
        var secondEvalField = as(as(eval.fields().get(1), Alias.class).child(), Mul.class);
        var thirdEvalField = as(as(eval.fields().get(2), Alias.class).child(), Mul.class);
        assertEquals(firstEvalField, secondEvalField);
        assertEquals(secondEvalField, thirdEvalField);

        assertThat(aggregates, hasSize(1));
        var countDistinct = as(as(aggregates.get(0), Alias.class).child(), CountDistinct.class);
        assertEquals("scalerank", as(countDistinct.field(), FieldAttribute.class).name());
    }

    /**
     * Limit[1000[INTEGER],false,false]
     * \_InlineJoin[LEFT,[],[]]
     *   |_EsRelation[airports][abbrev{f}#12, city{f}#18, city_location{f}#19, coun..]
     *   \_Project[[a{r}#5, b{r}#8, c{r}#11]]
     *     \_Eval[[$$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[INTEGER] AS a#5, $$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[
     * INTEGER] AS b#8, $$COUNTDISTINCT$2*COUNT_DISTINC>$0{r$}#20 * 2[INTEGER] AS c#11]]
     *       \_Aggregate[[],[COUNTDISTINCT(scalerank{f}#14,true[BOOLEAN],PT0S[TIME_DURATION],100[INTEGER]) AS $$COUNTDISTINCT$2*COUNT_DI
     * STINC>$0#20]]
     *         \_StubRelation[[abbrev{f}#12, city{f}#18, city_location{f}#19, country{f}#17, location{f}#16, name{f}#13, scalerank{f}#14, ty
     * pe{f}#15]]
     */
    public void testDuplicatedInlineAggWithFoldableIdenticalExpressions() {
        String query = """
                FROM airports
                | INLINE STATS a = 2*COUNT_DISTINCT(scalerank, 100),
                b = 2*COUNT_DISTINCT(scalerank, 220 - 150 + 30),
                c = 2*COUNT_DISTINCT(scalerank, 1 + 200 - 80 - 20 - 1)
            """;

        LogicalPlan plan = planAirports(query);
        var limit = as(plan, Limit.class);
        var inlineJoin = as(limit.child(), InlineJoin.class);

        var project = as(inlineJoin.right(), Project.class);
        var projections = project.projections();
        assertThat(projections, hasSize(3));

        assertEquals("a", as(projections.get(0), ReferenceAttribute.class).name());
        assertEquals("b", as(projections.get(1), ReferenceAttribute.class).name());
        assertEquals("c", as(projections.get(2), ReferenceAttribute.class).name());

        var eval = as(project.child(), Eval.class);
        var aggregates = as(eval.child(), Aggregate.class).aggregates();
        assertThat(eval.fields(), hasSize(3));
        var firstEvalField = as(as(eval.fields().get(0), Alias.class).child(), Mul.class);
        var secondEvalField = as(as(eval.fields().get(1), Alias.class).child(), Mul.class);
        var thirdEvalField = as(as(eval.fields().get(2), Alias.class).child(), Mul.class);
        assertEquals(firstEvalField, secondEvalField);
        assertEquals(secondEvalField, thirdEvalField);

        assertThat(aggregates, hasSize(1));
        var countDistinct = as(as(aggregates.get(0), Alias.class).child(), CountDistinct.class);
        assertEquals("scalerank", as(countDistinct.field(), FieldAttribute.class).name());
    }
}
