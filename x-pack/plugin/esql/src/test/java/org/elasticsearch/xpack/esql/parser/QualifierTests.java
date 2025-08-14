/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

public class QualifierTests extends AbstractStatementParserTests {

    /**
     * Test that qualifiers are parsed correctly in various expressions.
     */
    public void testQualifiersReferencedInExpressions() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        // We do not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified `field`", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified `field`> 0", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field== qualified `field`", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field/qualified field == 1", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field - qualified `field` ==qualified field",
            "qualified",
            "field",
            3
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE -qualified field", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field: \"foo\"", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT (qualified field)", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT (qualified `field`)", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT qualified field AND qualified `field`", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field OR qualified field OR qualified `field` AND qualified field",
            "qualified",
            "field",
            4
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified `field` IS NULL", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field IS NOT NULL", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE function(qualified `field`) <= other_function(qualified field)",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field::boolean", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified `field`::boolean", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field :: boolean", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE (qualified field)::boolean", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field IN (qualified field)", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field IN (qualified `field`, qualified field, qualified `field`)",
            "qualified",
            "field",
            4
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field LIKE (\"foo\", \"bar?\")", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified `field` RLIKE \"foo\"", "qualified", "field", 1);
    }

    /**
     * Test that qualifiers are parsed correctly in various commands.
     */
    public void testQualifiersReferencedInCommands() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        // We do not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx AS qualified ON x | ";

        assertQualifiedAttributeInExpressions(
            sourceQuery + "CHANGE_POINT qualified field ON qualified field AS type_name, pvalue_name",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "DISSECT qualified field \"%{foo}\"", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "DISSECT qualified `field` \"%{foo}\"", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "DISSECT qualified field \"\"\"%{foo}\"\"\"", "qualified", "field", 1);

        String keepDrop = randomBoolean() ? "KEEP" : "DROP";
        assertQualifiedAttributeInExpressions(sourceQuery + keepDrop + " qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + keepDrop + " qualified `field`", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " qualified field, field, qualified `field`, otherfield",
            "qualified",
            "field",
            2
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + keepDrop + " pat*ern, qualified field, other_pat*ern, qualified `field`, yet*other*pattern",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "ENRICH policy ON qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "ENRICH policy ON qualified field WITH x = y", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL x = qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL x = qualified field, y = qualified field", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL x = (qualified field)", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL qualified field/qualified field", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(sourceQuery + "EVAL x=qualified field/qualified field, y = foo", "qualified", "field", 2);

        assertQualifiedAttributeInExpressions(
            sourceQuery + "FORK (WHERE qualified field) (EVAL qualified field/2)",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "GROK qualified field \"%{WORD:foo}\"", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "GROK qualified `field` \"%{WORD:foo}\"", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "GROK qualified field \"\"\"%{WORD:foo}\"\"\"", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "MV_EXPAND qualified field", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "RENAME qualified field AS foo", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "RENAME qualified field AS foo, other_field AS bar", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "RENAME other_field AS bar, qualified field AS foo", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "RENAME bar = other_field, foo = qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME qualified field AS foo, bar = qualified field",
            "qualified",
            "field",
            2
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RENAME bar = qualified field, qualified field AS foo",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "RERANK score = \"query\" ON qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON qualified field, qualified field",
            "qualified",
            "field",
            2
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON field, qualified field, other_field",
            "qualified",
            "field",
            1
        );
        assertQualifiedAttributeInExpressions(
            sourceQuery + "RERANK score = \"query\" ON qualified field WITH {\"inference_id\": \"foo\"}",
            "qualified",
            "field",
            1
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field", "qualified", "field", 1);
    }

    public void testUnsupportedQualifiers() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        expectError("ROW qualified field = 1", "Qualified names are not supported in field definitions, found [qualified field]");

        expectError(
            sourceQuery + "CHANGE_POINT value_field ON key_field AS qualified type_name, pvalue_name",
            "Qualified names are not supported in field definitions, found [qualified type_name]"
        );
        expectError(
            sourceQuery + "CHANGE_POINT value_field ON key_field AS type_name, qualified pvalue_name",
            "Qualified names are not supported in field definitions, found [qualified pvalue_name]"
        );

        expectError(
            sourceQuery + "COMPLETION qualified field = \"prompt\" WITH {\"inference_id\" : \"foo\"}",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );

        String keepDrop = randomBoolean() ? "KEEP" : "DROP";
        expectError(
            sourceQuery + keepDrop + " qualified pat*ern",
            "Qualified names are not supported in patterns, found [qualified pat*ern]"
        );
        expectError(sourceQuery + keepDrop + " qual*fied field", "Qualified names are not supported in patterns, found [qual*fied field]");
        expectError(sourceQuery + keepDrop + " qualified *", "Qualified names are not supported in patterns, found [qualified *]");
        expectError(sourceQuery + keepDrop + " qual*fied *", "Qualified names are not supported in patterns, found [qual*fied *]");

        expectError(
            sourceQuery + "ENRICH policy ON field WITH qualified field = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [qualified field]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH new_field = qualified field",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [qualified field]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH qualified f*eld = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [qualified f*eld]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON field WITH qualified * = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [qualified *]"
        );
        expectError(
            sourceQuery + "ENRICH policy WITH quali*ied field = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [quali*ied field]"
        );
        expectError(
            sourceQuery + "ENRICH policy WITH * field = y",
            "Using qualifiers in ENRICH WITH projections is not allowed, found [* field]"
        );
        expectError(
            sourceQuery + "ENRICH policy ON quali*ied field",
            "Qualified names are not supported in patterns, found [quali*ied field]"
        );
        expectError(sourceQuery + "ENRICH policy ON * field", "Qualified names are not supported in patterns, found [* field]");

        expectError(
            sourceQuery + "EVAL qualified field = \"foo\"",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );
        expectError(
            sourceQuery + "EVAL field = x, qualified field = \"foo\"",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );
        expectError(sourceQuery + "EVAL field = x, quali*ied field = \"foo\"", "mismatched input '='");

        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON qualified field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON another_field, qualified field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx ON qualified field, another_field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx qualified ON qualified field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx another_qualifier ON qualified field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );
        expectError(
            sourceQuery + "LOOKUP JOIN another_idx AS another_qualifier ON qualified field",
            "JOIN ON clause only supports unqualified fields, found [qualified field]"
        );

        expectError(
            sourceQuery + "RENAME foo AS qualified field",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );
        expectError(
            sourceQuery + "RENAME qualified field = foo",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );

        expectError(
            sourceQuery + "RERANK qualified field = \"query\" ON foo",
            "Qualified names are not supported in field definitions, found [qualified field]"
        );
    }

    public void testIllegalQualifiers() {
        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx qualified ON x | ";

        expectError(sourceQuery + "EVAL qualified function(x)", "mismatched input '('");

        expectError(sourceQuery + "EVAL y = qualified \"foo\"", "extraneous input '\"foo\"'");
        expectError(sourceQuery + "EVAL y = qualified TRUE", "extraneous input 'TRUE'");
        expectError(sourceQuery + "EVAL y = qualified 1", "extraneous input '1'");
        expectError(sourceQuery + "EVAL y = qualified 1.2", "extraneous input '1.2'");

        expectError(sourceQuery + "LIMIT qualified 1.2", "extraneous input 'qualified'");
        expectError(sourceQuery + "LIMIT qualified field", "mismatched input 'qualified'");

        expectError(sourceQuery + "SAMPLE qualified field", "mismatched input 'qualified'");
    }

    private void assertQualifiedAttributeInExpressions(String query, String qualifier, String name, int expectedCount) {
        LogicalPlan plan = statement(query);
        Holder<Integer> count = new Holder<>(0);

        plan.forEachExpressionDown(UnresolvedAttribute.class, expr -> {
            int numOccurrences = countAttribute(expr, qualifier, name);
            if (numOccurrences > 0) {
                count.set(count.get() + numOccurrences);
            }
        });

        assertEquals(expectedCount, (int) count.get());
    }

    private static int countAttribute(Expression expr, String qualifier, String name) {
        Holder<Integer> numOccurrences = new Holder<>(0);

        expr.forEachDown(UnresolvedAttribute.class, a -> {
            if (a.qualifier() != null && a.qualifier().equals(qualifier) && a.name().equals(name)) {
                numOccurrences.set(numOccurrences.get() + 1);
            }
        });

        return numOccurrences.get();
    }
}
