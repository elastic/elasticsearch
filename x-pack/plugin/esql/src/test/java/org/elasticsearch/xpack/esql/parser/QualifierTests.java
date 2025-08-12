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
     * Test that qualifiers are parsed correctly in expressions.
     * Does not check the lookup join specifically; it only serves as an example source of qualifiers for the WHERE.
     */
    public void testQualifiersReferencedInExpressions() {
        assumeTrue("Requires qualifier support", EsqlCapabilities.Cap.NAME_QUALIFIERS.isEnabled());

        String sourceQuery = "ROW x = 1 | LOOKUP JOIN lu_idx lu ON x | ";

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field > 0", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field== qualified field", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field/qualified field == 1", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field - qualified field ==qualified field",
            "qualified",
            "field",
            3
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE -qualified field", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field: \"foo\"", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT qualified field", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT (qualified field)", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE NOT qualified field AND qualified field", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field OR qualified field OR qualified field AND qualified field",
            "qualified",
            "field",
            4
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field IS NULL", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field IS NOT NULL", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE function(qualified field) <= other_function(qualified field)",
            "qualified",
            "field",
            2
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field::boolean", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field :: boolean", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE (qualified field)::boolean", "qualified", "field", 1);

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field IN (qualified field)", "qualified", "field", 2);
        assertQualifiedAttributeInExpressions(
            sourceQuery + "WHERE qualified field IN (qualified field, qualified field)",
            "qualified",
            "field",
            3
        );

        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field LIKE (\"foo\", \"bar?\")", "qualified", "field", 1);
        assertQualifiedAttributeInExpressions(sourceQuery + "WHERE qualified field RLIKE \"foo\"", "qualified", "field", 1);

    }

    private void assertQualifiedAttributeInExpressions(String query, String qualifier, String name, int expectedCount) {
        LogicalPlan plan = statement(query);
        int count = 0;
        for (Expression expr : plan.expressions()) {
            count += countAttribute(expr, qualifier, name);
        }
        assertEquals(expectedCount, count);
    }

    private static int countAttribute(Expression expr, String qualifier, String name) {
        Holder<Integer> numOccurrences = new Holder<>(0);

        expr.forEachDown(UnresolvedAttribute.class, a -> {
            if (a.qualifier().equals(qualifier) && a.name().equals(name)) {
                numOccurrences.set(numOccurrences.get() + 1);
            }
        });

        return numOccurrences.get();
    }
}
