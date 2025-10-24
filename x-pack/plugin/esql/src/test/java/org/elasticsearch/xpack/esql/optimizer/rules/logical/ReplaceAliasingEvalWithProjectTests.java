/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
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
     * EsqlProject[[emp_no{f}#18, salary{f}#23, emp_no{f}#18 AS emp_no2#7, salary2{r}#10, emp_no{f}#18 AS emp_no3#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS salary2#10, salary2{r}#10 * 3[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
     */
    public void testSimple() {
        // Rule only kicks in if there's a Project or Aggregate above, so we add a KEEP *
        var plan = plan("""
            from test
            | KEEP emp_no, salary
            | EVAL emp_no2 = emp_no, salary2 = 2*salary, emp_no3 = emp_no2, salary3 = 3*salary2
            | KEEP *
            """);

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
        expectedProjections.add(salary.toAttribute());
        expectedProjections.add(alias("emp_no2", empNo));
        expectedProjections.add(salary2.toAttribute());
        expectedProjections.add(alias("emp_no3", empNo));
        expectedProjections.add(salary3.toAttribute());
        assertEqualsIgnoringIds(expectedProjections, project.projections());
    }

    /**
     * EsqlProject[[salary{f}#23, emp_no{f}#18 AS emp_no2#7, $$emp_no$temp_name$29{r}#30 AS emp_no#10,
     *  emp_no{f}#18 AS emp_no3#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS $$emp_no$temp_name$29#30, $$emp_no$temp_name$29{r$}#30 * 2[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
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
        assertEqualsIgnoringIds(mul(salary, of(2)), empNoTempName.child());

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
     * EsqlProject[[emp_no{f}#18, salary{f}#23, emp_no{f}#18 AS emp_no3#10, emp_no2{r}#13, salary3{r}#16]]
     * \_Eval[[salary{f}#23 * 2[INTEGER] AS emp_no2#13, emp_no2{r}#13 * 3[INTEGER] AS salary3#16]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#24, emp_no{f}#18, first_name{f}#19, ..]
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
     * EsqlProject[[emp_no{f}#24, salary{f}#29, emp_no{f}#24 AS emp_no3#13, salary{f}#29 AS salary3#16,
     *  salary{f}#29 AS emp_no2#19, emp_no{f}#24 AS salary2#22]]
     * \_Limit[1000[INTEGER],false,false]
     *   \_EsRelation[test][_meta_field{f}#30, emp_no{f}#24, first_name{f}#25, ..]
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
     * EsqlProject[[salary{f}#22, salary2{r}#6, salary2{r}#6 AS aliased_salary2#9, salary3{r}#12, salary2{r}#6 AS twice_aliased_salary2#15]]
     * \_Eval[[salary{f}#22 * 2[INTEGER] AS salary2#6, salary2{r}#6 * 3[INTEGER] AS salary3#12]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
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
     * EsqlProject[[salary{f}#25, salary2{r}#6 AS aliased_salary2#9, $$salary2$temp_name$31{r$}#32 AS salary2#12, salary3{r}#15,
     *  salary3{r}#15 AS salary4#18]]
     * \_Eval[[salary{f}#25 * 2[INTEGER] AS salary2#6, salary2{r}#6 * 3[INTEGER] AS $$salary2$temp_name$31#32,
     *  $$salary2$temp_name$31{r$}#32 * 4[INTEGER] AS salary3#15]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_EsRelation[test][_meta_field{f}#26, emp_no{f}#20, first_name{f}#21, ..]
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
