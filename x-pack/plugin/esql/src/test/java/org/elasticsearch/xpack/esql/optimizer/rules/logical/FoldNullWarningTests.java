/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class FoldNullWarningTests extends AbstractLogicalPlanOptimizerTests {
    public void testEqualsNullSuggestsIsNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no == NULL
            """);
        assertWarnings("Line 2:12: Expression [emp_no == NULL] always evaluates to NULL, did you mean [emp_no IS NULL]?");
    }

    public void testNotEqualsNullSuggestsIsNotNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no != NULL
            """);
        assertWarnings("Line 2:12: Expression [emp_no != NULL] always evaluates to NULL, did you mean [emp_no IS NOT NULL]?");
    }

    public void testNotWrappedEqualsNullSuggestsIsNotNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = NOT (emp_no == NULL)
            """);
        assertWarnings("Line 2:12: Expression [NOT (emp_no == NULL)] always evaluates to NULL, did you mean [emp_no IS NOT NULL]?");
    }

    public void testAddNullHasNoAlternative() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no + NULL
            """);
        assertWarnings("Line 2:12: Expression [emp_no + NULL] always evaluates to NULL.");
    }

    public void testConcatNullHasNoAlternative() {
        plan("""
            ROW name = "a"
            | EVAL x = CONCAT(name, NULL)
            """);
        assertWarnings("Line 2:12: Expression [CONCAT(name, NULL)] always evaluates to NULL.");
    }

    /**
     * The null is nested two levels deep. FoldNull transforms DOWN, so the outermost viral-null
     * expression folds first and emits a single warning covering the whole expression; there is
     * no IS NULL suggestion because neither direct operand of == is a guaranteed null.
     */
    public void testNestedConcatNullWarnsOnceOnWholeExpression() {
        plan("""
            FROM test
            | EVAL x = first_name == CONCAT(first_name, CONCAT(NULL, last_name))
            """);
        assertWarnings("Line 2:12: Expression [first_name == CONCAT(first_name, CONCAT(NULL, last_name))] always evaluates to NULL.");
    }

    public void testNestedConcatNullOnLiteralRowWarnsOnceOnWholeExpression() {
        plan("""
            ROW a = "x", b = "y", f = "z"
            | EVAL c = f == CONCAT(a, CONCAT(NULL, b))
            """);
        assertWarnings("Line 2:12: Expression [f == CONCAT(a, CONCAT(NULL, b))] always evaluates to NULL.");
    }

    public void testNullOnLeftSuggestsIsNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = NULL == emp_no
            """);
        assertWarnings("Line 2:12: Expression [NULL == emp_no] always evaluates to NULL, did you mean [emp_no IS NULL]?");
    }

    /**
     * When the null comes from a null-typed reference rather than a literal, the kept operand is
     * the literal side, producing an odd suggestion. Documents current behavior.
     */
    public void testNullTypedReferenceComparisonSuggestsLiteralIsNull() {
        plan("""
            ROW x = null
            | WHERE x == 5
            """);
        assertWarnings("Line 2:9: Expression [x == 5] always evaluates to NULL, did you mean [5 IS NULL]?");
    }

    public void testIndentedAddNullWarningLocation() {
        plan("""
            FROM test
              | KEEP emp_no
              | EVAL values = emp_no + NULL
            """);
        assertWarnings("Line 3:19: Expression [emp_no + NULL] always evaluates to NULL.");
    }

    public void testToIntegerNullWarningLocation() {
        plan("""
            FROM test
            | RENAME languages AS language_code
            | SORT emp_no, language_code
            | LIMIT 4
            | EVAL language_code = TO_INTEGER(NULL)
            """);
        assertWarnings("Line 5:24: Expression [TO_INTEGER(NULL)] always evaluates to NULL.");
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
