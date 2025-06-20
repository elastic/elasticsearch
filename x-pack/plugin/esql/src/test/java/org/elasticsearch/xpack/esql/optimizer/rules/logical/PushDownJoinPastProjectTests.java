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
    //   \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#26]]
    //   |_Limit[1000[INTEGER],true]
    //   | \_Join[LEFT,[languages{f}#16],[languages{f}#16],[language_code{f}#24]]
    //   |   |_Limit[1000[INTEGER],false]
    //   |   | \_EsRelation[test][_meta_field{f}#19, emp_no{f}#13, first_name{f}#14, ..]
    //   |   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#24]
    //   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#26, language_name{f}#27]
    public void testMultipleLookups() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

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
    //   \_Join[LEFT,[languages{f}#14],[languages{f}#14],[language_code{f}#22]]
    //   |_Limit[1000[INTEGER],false]
    //   | \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
    //   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#22, language_name{f}#23]
    public void testShadowingBeforePushdown() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

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
    //   \_Join[LEFT,[languages{f}#17],[languages{f}#17],[language_code{f}#25]]
    //   |_Eval[[salary{f}#19 * 2[INTEGER] AS language_name#4, language_name{r}#4 AS $$language_name$temp_name$27#28]]
    //   | \_Limit[1000[INTEGER],false]
    //   |   \_EsRelation[test][_meta_field{f}#20, emp_no{f}#14, first_name{f}#15, ..]
    //   \_EsRelation[languages_lookup][LOOKUP][language_code{f}#25, language_name{f}#26]
    public void testShadowingAfterPushdown() {
        assumeTrue("Requires LOOKUP JOIN", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());

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
