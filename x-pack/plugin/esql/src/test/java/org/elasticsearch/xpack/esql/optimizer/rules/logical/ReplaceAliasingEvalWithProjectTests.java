/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.findFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.mul;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.hamcrest.Matchers.startsWith;

public class ReplaceAliasingEvalWithProjectTests extends AbstractLogicalPlanOptimizerTests {
    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#18, salary{f}#23, emp_no{f}#18 AS emp_no2#7, salary2{r}#10, emp_no{f}#18 AS emp_no3#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS salary2#10, salary2{r}#10 * 3[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     * }</pre>
     */
    public void testSimple() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan(randomFrom("""
            FROM test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = 2*salary, emp_no3 = emp_no2, salary3 = 3*salary2
            | KEEP *
            """, """
            FROM test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = 2*salary
            | EVAL emp_no3 = emp_no2, salary3 = 3*salary2
            | KEEP *
            """));

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        var salary2 = alias("salary2", mul(salary, of(2)));
        var salary3 = alias("salary3", mul(salary2.toAttribute(), of(3)));

        List<Expression> expectedEvalFields = new ArrayList<>();
        expectedEvalFields.add(salary2);
        expectedEvalFields.add(salary3);
        assertEqualsIgnoringIds(expectedEvalFields, eval.fields());

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(empNo);
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(salary2.toAttribute());
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#19, salary{f}#35, emp_no{f}#19 AS emp_no2#8, salary2{r}#11, emp_no{f}#19 AS emp_no3#14, salary3{r}#17]]
     * \_Eval[[salary{f}#35 * 2[INTEGER] AS salary2#11, salary2{r}#11 * 3[INTEGER] AS salary3#17]]
     *   \_Limit[1000[INTEGER],true,false]
     *     \_Join[LEFT,[emp_no{f}#19],[emp_no{f}#30],null]
     *       |_Limit[1000[INTEGER],false,false]
     *       | \_EsRelation[test][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     *       \_EsRelation[test_lookup][LOOKUP][emp_no{f}#30, salary{f}#35]
     * }</pre>
     */
    public void testSimpleFieldFromLookup() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            FROM test
            | LOOKUP JOIN test_lookup ON emp_no
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = 2*salary, emp_no3 = emp_no2, salary3 = 3*salary2
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        // the final emp_no is from the main index, the final salary from the lookup index
        var empNo = findFieldAttribute(plan, "emp_no", relation -> relation.indexMode() == IndexMode.STANDARD);
        var salary = findFieldAttribute(plan, "salary", relation -> relation.indexMode() == IndexMode.LOOKUP);

        var salary2 = alias("salary2", mul(salary, of(2)));
        var salary3 = alias("salary3", mul(salary2.toAttribute(), of(3)));

        List<Expression> expectedEvalFields = new ArrayList<>();
        expectedEvalFields.add(salary2);
        expectedEvalFields.add(salary3);
        assertEqualsIgnoringIds(expectedEvalFields, eval.fields());
        // assert using id to ensure we're pointing to the right salary
        assertEquals(eval.fields().get(0).child(), mul(salary, of(2)));

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(empNo);
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(salary2.toAttribute());
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
        // assert using id to ensure we're pointing to the right emp_no and salary
        assertEquals(project.projections().get(0), empNo);
        assertEquals(project.projections().get(1), salary);
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#24 AS emp_no2#7, salary{f}#29 AS salary2#10, emp_no{f}#24 AS emp_no3#13, emp_no{f}#24 AS salary#16,
     *  salary{f}#29 AS salary3#19, salary{f}#29 AS emp_no#22]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     * }</pre>
     */
    public void testOnlyAliases() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = salary, emp_no3 = emp_no2, salary = emp_no3, salary3 = salary2, emp_no = salary3
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(alias("salary2", salary));
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(alias("salary", empNo));
        expectedProjections.add(alias("salary3", salary));
        expectedProjections.add(alias("emp_no", salary));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#26 AS b#21, emp_no{f}#26 AS a#24]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#32, emp_no{f}#26, first_name{f}#27, ..]
     * }</pre>
     */
    public void testAliasLoopTwoVars() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no
            | RENAME emp_no AS a
            | EVAL b = a, a = b, b = a, a = b, b = a, a = b
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);

        var empNo = findFieldAttribute(plan, "emp_no");

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(alias("b", empNo));
        expectedProjections.add(alias("a", empNo));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#17 AS b#9, emp_no{f}#17 AS c#12, emp_no{f}#17 AS a#15]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testAliasLoopThreeVars() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no
            | RENAME emp_no AS a
            | EVAL b = a, c = b, a = c
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var limit = as(project.child(), Limit.class);

        var empNo = findFieldAttribute(plan, "emp_no");

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(alias("b", empNo));
        expectedProjections.add(alias("c", empNo));
        expectedProjections.add(alias("a", empNo));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[salary{f}#23, emp_no{f}#18 AS emp_no2#7, $$emp_no$temp_name$29{r}#30 AS emp_no#10,
     *  emp_no{f}#18 AS emp_no3#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS $$emp_no$temp_name$29#30, $$emp_no$temp_name$29{r$}#30 * 2[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     * }</pre>
     */
    public void testNonAliasShadowingAliasedAttribute() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan(randomFrom("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, emp_no = 2*salary, emp_no3 = emp_no2, salary3 = 2*emp_no
            | KEEP *
            """));

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        assertEquals(2, eval.fields().size());

        var empNoTempName = eval.fields().get(0);
        assertThat(empNoTempName.name(), startsWith("$$emp_no$temp_name$"));
        assertEquals(mul(salary, of(2)), empNoTempName.child());

        var salary3 = alias("salary3", mul(empNoTempName.toAttribute(), of(2)));
        assertEqualsIgnoringIds(salary3, eval.fields().get(1));

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(alias("emp_no", empNoTempName.toAttribute()));
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * Limit[1000[INTEGER],false,false]
     * \_Aggregate[[$$emp_no$temp_name$33{r$}#34, emp_no{f}#22, salary{f}#27, salary3{r}#16],
     *      [$$emp_no$temp_name$33{r$}#34 AS emp_no#10, emp_no{f}#22 AS emp_no2#7, emp_no{f}#22 AS emp_no3#13, salary{f}#27, salary3{r}#16]]
     *   \_Eval[[salary{f}#27 * 2[INTEGER] AS $$emp_no$temp_name$33#34, $$emp_no$temp_name$33{r$}#34 * 2[INTEGER] AS salary3#1
     * 6]]
     *     \_EsRelation[test][_meta_field{f}#28, emp_no{f}#22, first_name{f}#23, ..]
     * }</pre>
     */
    public void testNonAliasShadowingAliasedAttributeWithAgg() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan(randomFrom("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, emp_no = 2*salary, emp_no3 = emp_no2, salary3 = 2*emp_no
            | STATS BY emp_no, emp_no2, emp_no3, salary, salary3
            """));

        var limit = as(plan, Limit.class);
        var agg = as(limit.child(), Aggregate.class);
        var eval = as(agg.child(), Eval.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        assertEquals(2, eval.fields().size());

        var empNoTempName = eval.fields().get(0);
        assertThat(empNoTempName.name(), startsWith("$$emp_no$temp_name$"));
        assertEquals(mul(salary, of(2)), empNoTempName.child());

        var salary3 = alias("salary3", mul(empNoTempName.toAttribute(), of(2)));
        assertEqualsIgnoringIds(salary3, eval.fields().get(1));

        List<Expression> expectedAggregates = new ArrayList<>();
        expectedAggregates.add(alias("emp_no", empNoTempName.toAttribute()));
        expectedAggregates.add(alias("emp_no2", empNo));
        expectedAggregates.add(alias("emp_no3", empNo));
        expectedAggregates.add(salary);
        expectedAggregates.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedAggregates, agg.aggregates());
    }

    /**
     * <pre>{@code
     * EsqlProject[[salary{f}#35, emp_no{f}#30 AS emp_no2#22, $$id$temp_name$41{r$}#42 AS emp_no#25, emp_no{f}#30 AS emp_no3#28,
     *  salary3{r}#19]]
     * \_Eval[[salary{f}#35 * 2[INTEGER] AS $$id$temp_name$41#42, $$id$temp_name$41{r$}#42 * 2[INTEGER] AS salary3#19]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#36, emp_no{f}#30, first_name{f}#31, ..]
     * }</pre>
     */
    public void testNonAliasShadowingAliasedAttributeWithRename() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan(randomFrom("""
            from test
            | KEEP emp_no, salary
            | RENAME emp_no AS id
            | EVAL id2 = id, id = 2*salary, id3 = id2, salary3 = 2*id
            | RENAME id2 AS emp_no2, id AS emp_no, id3 AS emp_no3
            | KEEP *
            """));

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        assertEquals(2, eval.fields().size());

        var idTempName = eval.fields().get(0);
        // The renaming to $$id$temp_name$ is not strictly necessary. If the eval pushdown past the first rename happened before the
        // alias extraction, the eval wouldn't shadow any input attributes anymore.
        assertThat(idTempName.name(), startsWith("$$id$temp_name$"));
        assertEquals(mul(salary, of(2)), idTempName.child());

        var salary3 = alias("salary3", mul(idTempName.toAttribute(), of(2)));
        assertEqualsIgnoringIds(salary3, eval.fields().get(1));

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(alias("emp_no", idTempName.toAttribute()));
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#18, salary{f}#23, emp_no{f}#18 AS emp_no3#10, emp_no2{r}#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS emp_no2#13, emp_no2{r}#13 * 3[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     * }</pre>
     */
    public void testNonAliasShadowingAliasOfAliasedAttribute() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, emp_no3 = emp_no2, emp_no2 = 2*salary, salary3 = 3*emp_no2
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        var empNo2 = alias("emp_no2", mul(salary, of(2)));
        var salary3 = alias("salary3", mul(empNo2.toAttribute(), of(3)));
        assertEqualsIgnoringIds(List.of(empNo2, salary3), eval.fields());

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(empNo);
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(empNo2.toAttribute());
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[emp_no{f}#24, salary{f}#29, emp_no{f}#24 AS emp_no3#13, salary{f}#29 AS salary3#16,
     *  salary{f}#29 AS emp_no2#19, emp_no{f}#24 AS salary2#22]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
     * }</pre>
     */
    public void testAliasShadowingOtherAlias() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = salary, emp_no3 = emp_no2, salary3 = salary2, emp_no2 = salary, salary2 = emp_no
            | KEEP *
            """);

        var project = as(plan, Project.class);

        var empNo = findFieldAttribute(plan, "emp_no");
        var salary = findFieldAttribute(plan, "salary");

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(empNo);
        expectedProjections.add(salary);
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(alias("salary3", salary));
        expectedProjections.add(alias("emp_no2", salary));
        expectedProjections.add(alias("salary2", empNo));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[salary{f}#22, salary2{r}#6, salary2{r}#6 AS aliased_salary2#9, salary3{r}#12, salary2{r}#6 AS twice_aliased_salary2#15]]
     * \_Eval[[salary{f}#22 * 2[INTEGER] AS salary2#6, salary2{r}#6 * 3[INTEGER] AS salary3#12]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     * }</pre>
     */
    public void testAliasForNonAlias() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP salary
            | EVAL salary2 = 2*salary, aliased_salary2 = salary2, salary3 = 3*salary2, twice_aliased_salary2 = aliased_salary2
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var salary = findFieldAttribute(plan, "salary");

        var salary2 = alias("salary2", mul(salary, of(2)));
        var salary3 = alias("salary3", mul(salary2.toAttribute(), of(3)));
        assertEqualsIgnoringIds(List.of(salary2, salary3), eval.fields());

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(salary);
        expectedProjections.add(salary2.toAttribute());
        expectedProjections.add(alias("aliased_salary2", salary2.toAttribute()));
        expectedProjections.add(salary3.toAttribute());
        expectedProjections.add(alias("twice_aliased_salary2", salary2.toAttribute()));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * <pre>{@code
     * EsqlProject[[salary{f}#25, salary2{r}#6 AS aliased_salary2#9, $$salary2$temp_name$31{r$}#32 AS salary2#12, salary3{r}#15,
     *  salary3{r}#15 AS salary4#18]]
     * \_Eval[[salary{f}#25 * 2[INTEGER] AS salary2#6, salary2{r}#6 * 3[INTEGER] AS $$salary2$temp_name$31#32,
     *  $$salary2$temp_name$31{r$}#32 * 4[INTEGER] AS salary3#15]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
     * }</pre>
     */
    public void testAliasForShadowedNonAlias() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP salary
            | EVAL salary2 = 2*salary, aliased_salary2 = salary2, salary2 = 3*salary2, salary3 = 4*salary2, salary4 = salary3
            | KEEP *
            """);

        var project = as(plan, Project.class);
        var eval = as(project.child(), Eval.class);

        var salary = findFieldAttribute(plan, "salary");

        assertEquals(3, eval.fields().size());
        var salary2 = alias("salary2", mul(salary, of(2)));
        assertEqualsIgnoringIds(salary2, eval.fields().get(0));
        var salary2TempName = eval.fields().get(1);
        assertThat(salary2TempName.name(), startsWith("$$salary2$temp_name$"));
        assertEqualsIgnoringIds(mul(salary2.toAttribute(), of(3)), salary2TempName.child());
        var salary3 = alias("salary3", mul(salary2TempName.toAttribute(), of(4)));
        assertEqualsIgnoringIds(salary3, eval.fields().get(2));

        List<Expression> expectedProjections = new ArrayList<>();
        expectedProjections.add(salary);
        expectedProjections.add(alias("aliased_salary2", salary2.toAttribute()));
        expectedProjections.add(alias("salary2", salary2TempName.toAttribute()));
        expectedProjections.add(salary3.toAttribute());
        expectedProjections.add(alias("salary4", salary3.toAttribute()));
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }
}
