/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.preoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;

public class WarnNullMisuseTests extends AbstractLogicalPlanOptimizerTests {
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
     * even though the comparison will fold later.
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

    /**
     * A one-element {@code IN} is parsed as {@code Equals}, so this already warns via the
     * comparison path — and {@code emp_no IS NULL} is the right suggestion.
     */
    public void testNullInFieldAlwaysNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = NULL IN (emp_no)
            """);
        assertWarnings("Line 2:12: Expression [NULL IN (emp_no)] always evaluates to NULL, did you mean [emp_no IS NULL]?");
    }

    /**
     * Same {@code Equals} rewrite: {@code emp_no IN (NULL)} is {@code emp_no == NULL}.
     */
    public void testFieldInOnlyNullSuggestsIsNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no IN (NULL)
            """);
        assertWarnings("Line 2:12: Expression [emp_no IN (NULL)] always evaluates to NULL, did you mean [emp_no IS NULL]?");
    }

    /**
     * {@code NULL IN (subquery)} is rewritten to a SemiJoin whose left key is a synthetic NULL
     * constant. Analysis then rejects the join as a type mismatch, so {@link WarnNullMisuse}
     * never sees the expression.
     */
    public void testNullInSubqueryIsRejectedAsTypeMismatch() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        VerificationException e = expectThrows(VerificationException.class, () -> plan("""
            FROM test
            | WHERE NULL IN (FROM test | KEEP emp_no)
            """));
        assertThat(e.getMessage(), containsString("of type [NULL] is incompatible"));
    }

    /**
     * Real {@code In}: a null probe against a multi-element list. Always NULL, but there is
     * no {@code IS NULL} rewrite — the list items are not being tested for nullness.
     */
    public void testNullInTwoValuesAlwaysNull() {
        plan("""
            ROW emp_no = 1, salary = 2
            | EVAL x = NULL IN (emp_no, salary)
            """);
        assertWarnings("Line 2:12: Expression [NULL IN (emp_no, salary)] always evaluates to NULL.");
    }

    /**
     * Real {@code In}: every list element is an explicit NULL. The list {@code NULL}s are ignored;
     * the remaining suggestion is {@code IS NULL}.
     */
    public void testFieldInAllNullListSuggestsIsNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no IN (NULL, NULL)
            """);
        assertWarnings("Line 2:12: NULL in the IN list of [emp_no IN (NULL, NULL)] is ignored, did you mean [emp_no IS NULL]?");
    }

    public void testFieldNotInAllNullListSuggestsIsNotNull() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no NOT IN (NULL, NULL)
            """);
        assertWarnings("Line 2:12: NULL in the IN list of [emp_no NOT IN (NULL, NULL)] is ignored, did you mean [emp_no IS NOT NULL]?");
    }

    /**
     * A match still yields true; the list {@code NULL} never matches. Suggest moving the null
     * check to {@code OR … IS NULL} rather than rewriting the {@code IN} source.
     */
    public void testFieldInValueAndNullIsIgnored() {
        plan("""
            ROW emp_no = 1
            | EVAL x = emp_no IN (1, NULL)
            """);
        assertWarnings("Line 2:12: NULL in the IN list of [emp_no IN (1, NULL)] is ignored, you can move it to [OR emp_no IS NULL].");
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
