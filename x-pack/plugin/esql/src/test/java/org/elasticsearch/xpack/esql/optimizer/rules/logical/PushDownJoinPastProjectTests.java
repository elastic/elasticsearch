/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.asLimit;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;

public class PushDownJoinPastProjectTests extends AbstractLogicalPlanOptimizerTests {

    // Expects
    //
    // Project[[languages{f}#16, emp_no{f}#13, languages{f}#16 AS language_code#6, language_name{f}#27]]
    // \_Limit[1000[INTEGER],true]
    // \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#26]]
    // |_Limit[1000[INTEGER],true]
    // | \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#24]]
    // | |_Limit[1000[INTEGER],false]
    // | | \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
    // | \_EsRelation[languages_lookup][LOOKUP][language_code{f}#24]
    // \_EsRelation[languages_lookup][LOOKUP][language_code{f}#26, language_name{f}#27]
    public void testMultipleLookups() {
        String query = """
            FROM test
            | KEEP languages, emp_no
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | RENAME language_name AS foo
            | LOOKUP JOIN languages_lookup ON language_code
            | DROP foo
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var upperLimit = asLimit(project.child(), 1000, true);

        var join1 = as(upperLimit.child(), Join.class);
        var lookupRel1 = as(join1.right(), EsRelation.class);
        var limit1 = asLimit(join1.left(), 1000, true);

        var join2 = as(limit1.child(), Join.class);
        var lookupRel2 = as(join2.right(), EsRelation.class);
        var limit2 = asLimit(join2.left(), 1000, false);

        var mainRel = as(limit2.child(), EsRelation.class);

        // Left join key should be updated to use an attribute from the main index directly
        assertLeftJoinConfig(join1.config(), "languages", mainRel.outputSet(), "language_code", lookupRel1.outputSet());
        assertLeftJoinConfig(join2.config(), "languages", mainRel.outputSet(), "language_code", lookupRel2.outputSet());
    }

    // Expects
    //
    // Project[[languages{f}#14 AS language_code#4, language_name{f}#23]]
    // \_Limit[1000[INTEGER],true]
    // \_Join[LEFT,[languages{f}#14],[languages{f}#14],[language_code{f}#22]]
    // |_Limit[1000[INTEGER],false]
    // | \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
    // \_EsRelation[languages_lookup][LOOKUP][language_code{f}#22, language_name{f}#23]
    public void testShadowingBeforePushdown() {
        String query = """
            FROM test
            | RENAME languages AS language_code, last_name AS language_name
            | KEEP language_code, language_name
            | LOOKUP JOIN languages_lookup ON language_code
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);
        var limit2 = asLimit(join.left(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        // Left join key should be updated to use an attribute from the main index directly
        assertLeftJoinConfig(join.config(), "languages", mainRel.outputSet(), "language_code", lookupRel.outputSet());
    }

    // Expects
    //
    // Project[[languages{f}#17 AS language_code#9, $$language_name$temp_name$27{r$}#28 AS foo#12, language_name{f}#26]]
    // \_Limit[1000[INTEGER],true]
    // \_Join[LEFT,[languages{f}#17],[languages{f}#17],[language_code{f}#25]]
    // |_Eval[[salary{f}#19 * 2[INTEGER] AS language_name#4, language_name{r}#4 AS $$language_name$temp_name$27#28]]
    // | \_Limit[1000[INTEGER],false]
    // | \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
    // \_EsRelation[languages_lookup][LOOKUP][language_code{f}#25, language_name{f}#26]
    public void testShadowingAfterPushdown() {
        String query = """
            FROM test
            | EVAL language_name = 2*salary
            | KEEP languages, language_name
            | RENAME languages AS language_code, language_name AS foo
            | LOOKUP JOIN languages_lookup ON language_code
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);

        var eval = as(join.left(), Eval.class);
        var limit2 = asLimit(eval.child(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("language_code", "foo", "language_name"));

        var languages = unwrapAlias(projections.get(0), FieldAttribute.class);
        assertEquals("languages", languages.fieldName().string());
        assertTrue(mainRel.outputSet().contains(languages));

        var tempName = unwrapAlias(projections.get(1), ReferenceAttribute.class);
        assertThat(tempName.name(), startsWith("$$language_name$temp_name$"));
        assertTrue(eval.outputSet().contains(tempName));

        var languageName = as(projections.get(2), FieldAttribute.class);
        assertTrue(lookupRel.outputSet().contains(languageName));

        var evalExprs = eval.fields();
        assertThat(Expressions.names(evalExprs), contains("language_name", tempName.name()));
        var originalLanguageName = unwrapAlias(evalExprs.get(1), ReferenceAttribute.class);
        assertEquals("language_name", originalLanguageName.name());
        assertTrue(originalLanguageName.semanticEquals(as(evalExprs.get(0), Alias.class).toAttribute()));

        var mul = unwrapAlias(evalExprs.get(0), Mul.class);
        assertEquals(new Literal(Source.EMPTY, 2, DataType.INTEGER), mul.right());
        var salary = as(mul.left(), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(mainRel.outputSet().contains(salary));

        assertLeftJoinConfig(join.config(), "languages", mainRel.outputSet(), "language_code", lookupRel.outputSet());
    }

    /*
     * Expects
     * Project[[$$emp_no$temp_name$51{r$}#52 AS languages#17, emp_no{f}#38, salary{f}#43, $$salary$temp_name$49{r$}#50 AS salary2#8,
     * last_name{f}#31 AS ln#20]]
     * \_Limit[1000[INTEGER],true,false]
     *   \_Join[LEFT,[emp_no{f}#27],[languages{f}#41],null]
     *     |_Eval[[salary{f}#32 AS $$salary$temp_name$49#50, emp_no{f}#27 AS $$emp_no$temp_name$51#52]]
     *     | \_Limit[1000[INTEGER],false,false]
     *     |   \_EsRelation[test][_meta_field{f}#33, emp_no{f}#27, first_name{f}#28, ..]
     *     \_EsRelation[test_lookup][LOOKUP][emp_no{f}#38, languages{f}#41, salary{f}#43]
     */
    public void testShadowingAfterPushdown2() {
        String query = """
            FROM test
            | RENAME emp_no AS x, salary AS salary2
            | EVAL y = x, gender = last_name
            | RENAME y AS languages, gender AS ln
            | LOOKUP JOIN test_lookup ON languages
            | KEEP languages, emp_no, salary, salary2, ln
            """;

        var plan = optimizedPlan(query);

        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);

        var eval = as(join.left(), Eval.class);
        var limit2 = asLimit(eval.child(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("languages", "emp_no", "salary", "salary2", "ln"));

        var empNoTempName = unwrapAlias(projections.get(0), ReferenceAttribute.class);
        assertThat(empNoTempName.name(), startsWith("$$emp_no$temp_name$"));
        assertTrue(eval.outputSet().contains(empNoTempName));

        var empNo = as(projections.get(1), FieldAttribute.class);
        assertEquals("emp_no", empNo.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(empNo));

        var salary = as(projections.get(2), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(salary));

        var salaryTempName = unwrapAlias(projections.get(3), ReferenceAttribute.class);
        assertThat(salaryTempName.name(), startsWith("$$salary$temp_name$"));
        assertTrue(eval.outputSet().contains(salaryTempName));

        var lastName = unwrapAlias(projections.get(4), FieldAttribute.class);
        assertEquals("last_name", lastName.fieldName().string());
        assertTrue(mainRel.outputSet().contains(lastName));

        var evalExprs = eval.fields();
        assertThat(Expressions.names(evalExprs), contains(salaryTempName.name(), empNoTempName.name()));

        var originalSalary = unwrapAlias(evalExprs.get(0), FieldAttribute.class);
        assertTrue(evalExprs.get(0).toAttribute().semanticEquals(salaryTempName));
        assertEquals("salary", originalSalary.fieldName().string());
        assertTrue(mainRel.outputSet().contains(originalSalary));

        var originalEmpNo = unwrapAlias(evalExprs.get(1), FieldAttribute.class);
        assertTrue(evalExprs.get(1).toAttribute().semanticEquals(empNoTempName));
        assertEquals("emp_no", originalEmpNo.fieldName().string());
        assertTrue(mainRel.outputSet().contains(originalEmpNo));

        assertLeftJoinConfig(join.config(), "emp_no", mainRel.outputSet(), "languages", lookupRel.outputSet());
    }

    /**
     * Project[[languages{f}#30, emp_no{f}#27, salary{f}#32]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[emp_no{f}#16, languages{f}#30],[emp_no{f}#16],[languages{f}#30],emp_no{f}#16 == languages{f}#30]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#22, emp_no{f}#16, first_name{f}#17, ..]
     *     \_EsRelation[test_lookup][LOOKUP][emp_no{f}#27, languages{f}#30, salary{f}#32]
     */
    public void testShadowingAfterPushdownExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );

        String query = """
            FROM test
            | RENAME languages as lang2
            | EVAL y = emp_no
            | RENAME y AS lang
            | LOOKUP JOIN test_lookup ON lang == languages
            | KEEP languages, emp_no, salary
            """;

        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);
        var limit2 = asLimit(join.left(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("languages", "emp_no", "salary"));

        var languages = as(projections.get(0), FieldAttribute.class);
        assertEquals("languages", languages.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(languages));

        var empNo = as(projections.get(1), FieldAttribute.class);
        assertEquals("emp_no", empNo.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(empNo));

        var salary = as(projections.get(2), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(salary));
    }

    /**
     * Project[[languages{f}#31, emp_no{f}#28, salary{f}#33, $$languages$temp_name$39{r$}#40 AS lang2#4]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[emp_no{f}#17],[languages{f}#31],emp_no{f}#17 == languages{f}#31]
     *     |_Eval[[languages{f}#20 AS $$languages$temp_name$39#40]]
     *     | \_Limit[1000[INTEGER],false]
     *     |   \_EsRelation[test][_meta_field{f}#23, emp_no{f}#17, first_name{f}#18, ..]
     *     \_EsRelation[test_lookup][LOOKUP][emp_no{f}#28, languages{f}#31, salary{f}#33]
     */
    public void testShadowingAfterPushdownExpressionJoinKeepOrig() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );

        String query = """
            FROM test
            | RENAME languages as lang2
            | EVAL y = emp_no
            | RENAME y AS lang
            | LOOKUP JOIN test_lookup ON lang == languages
            | KEEP languages, emp_no, salary, lang2
            """;

        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);
        var eval = as(join.left(), Eval.class);
        var limit2 = asLimit(eval.child(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("languages", "emp_no", "salary", "lang2"));

        var languages = as(projections.get(0), FieldAttribute.class);
        assertEquals("languages", languages.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(languages));

        var empNo = as(projections.get(1), FieldAttribute.class);
        assertEquals("emp_no", empNo.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(empNo));

        var salary = as(projections.get(2), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(salary));

        var lang2TempName = unwrapAlias(projections.get(3), ReferenceAttribute.class);
        assertThat(lang2TempName.name(), startsWith("$$languages$temp_name$"));
        assertTrue(eval.outputSet().contains(lang2TempName));
    }

    /**
     * Project[[languages{f}#24, emp_no{f}#21, salary{f}#26]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[languages{f}#24, languages{f}#13],[languages{f}#13],[languages{f}#24],languages{f}#13 == languages{f}#24]
     *     |_Limit[1000[INTEGER],false]
     *     | \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
     *     \_EsRelation[test_lookup][LOOKUP][emp_no{f}#21, languages{f}#24, salary{f}#26]
     */
    public void testShadowingAfterPushdownRenameExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );

        String query = """
            FROM test
            | RENAME languages AS lang
            | LOOKUP JOIN test_lookup ON lang == languages
            | KEEP languages, emp_no, salary
            """;

        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);
        var limit2 = asLimit(join.left(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("languages", "emp_no", "salary"));

        var languages = as(projections.get(0), FieldAttribute.class);
        assertEquals("languages", languages.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(languages));

        var empNo = as(projections.get(1), FieldAttribute.class);
        assertEquals("emp_no", empNo.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(empNo));

        var salary = as(projections.get(2), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(salary));
    }

    /**
     * Project[[languages{f}#25, emp_no{f}#22, salary{f}#27]]
     * \_Limit[1000[INTEGER],true]
     *   \_Join[LEFT,[lang{r}#4, languages{f}#25],[lang{r}#4],[languages{f}#25],lang{r}#4 == languages{f}#25]
     *     |_Eval[[languages{f}#14 + 0[INTEGER] AS lang#4]]
     *     | \_Limit[1000[INTEGER],false]
     *     |   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *     \_EsRelation[test_lookup][LOOKUP][emp_no{f}#22, languages{f}#25, salary{f}#27]
     */
    public void testShadowingAfterPushdownEvalExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );

        String query = """
            FROM test
            | EVAL lang = languages + 0
            | DROP languages
            | LOOKUP JOIN test_lookup ON lang == languages
            | KEEP languages, emp_no, salary
            """;

        var plan = optimizedPlan(query);
        var project = as(plan, Project.class);
        var limit1 = asLimit(project.child(), 1000, true);
        var join = as(limit1.child(), Join.class);
        var lookupRel = as(join.right(), EsRelation.class);
        var eval = as(join.left(), Eval.class);
        var limit2 = asLimit(eval.child(), 1000, false);
        var mainRel = as(limit2.child(), EsRelation.class);

        var projections = project.projections();
        assertThat(Expressions.names(projections), contains("languages", "emp_no", "salary"));

        var languages = as(projections.get(0), FieldAttribute.class);
        assertEquals("languages", languages.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(languages));

        var empNo = as(projections.get(1), FieldAttribute.class);
        assertEquals("emp_no", empNo.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(empNo));

        var salary = as(projections.get(2), FieldAttribute.class);
        assertEquals("salary", salary.fieldName().string());
        assertTrue(lookupRel.outputSet().contains(salary));
    }

    private static void assertLeftJoinConfig(
        JoinConfig config,
        String expectedLeftFieldName,
        AttributeSet leftSourceAttributes,
        String expectedRightFieldName,
        AttributeSet rightSourceAttributes
    ) {
        assertSame(config.type(), JoinTypes.LEFT);

        var leftKeys = config.leftFields();
        var rightKeys = config.rightFields();

        assertEquals(1, leftKeys.size());
        var leftKey = leftKeys.get(0);
        assertEquals(expectedLeftFieldName, leftKey.name());
        assertTrue(leftSourceAttributes.contains(leftKey));

        assertEquals(1, rightKeys.size());
        var rightKey = rightKeys.get(0);
        assertEquals(expectedRightFieldName, rightKey.name());
        assertTrue(rightSourceAttributes.contains(rightKey));
    }

    private static <T> T unwrapAlias(Expression alias, Class<T> type) {
        var child = as(alias, Alias.class).child();

        return as(child, type);
    }
}
