/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer.WarnNullMisuse;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

public class FoldNullWarningTests extends AbstractLogicalPlanOptimizerTests {
    @Override
    protected LogicalPlan plan(String query) {
        LogicalPlan analyzed = defaultAnalyzer().query(query);
        new WarnNullMisuse().apply(analyzed, ActionListener.noop());
        return optimize(analyzed);
    }

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
     * The explicit NULL is a direct child of the inner CONCAT, so the warning points there
     * rather than at the enclosing comparison the null later propagates to.
     */
    public void testNestedConcatNullWarnsOnInnerConcat() {
        plan("""
            FROM test
            | EVAL x = first_name == CONCAT(first_name, CONCAT(NULL, last_name))
            """);
        assertWarnings("Line 2:45: Expression [CONCAT(NULL, last_name)] always evaluates to NULL.");
    }

    public void testNestedConcatNullOnLiteralRowWarnsOnInnerConcat() {
        plan("""
            ROW a = "x", b = "y", f = "z"
            | EVAL c = f == CONCAT(a, CONCAT(NULL, b))
            """);
        assertWarnings("Line 2:27: Expression [CONCAT(NULL, b)] always evaluates to NULL.");
    }

    public void testNullOnLeftSuggestsIsNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = NULL == emp_no
            """);
        assertWarnings("Line 2:12: Expression [NULL == emp_no] always evaluates to NULL, did you mean [emp_no IS NULL]?");
    }

    /**
     * A NULL-typed reference is not an explicit NULL literal, so no warning is emitted
     * even though the comparison will fold to NULL later.
     */
    public void testNullTypedReferenceComparisonDoesNotWarn() {
        plan("""
            ROW x = null
            | WHERE x == 5
            """);
        ensureNoWarnings();
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
