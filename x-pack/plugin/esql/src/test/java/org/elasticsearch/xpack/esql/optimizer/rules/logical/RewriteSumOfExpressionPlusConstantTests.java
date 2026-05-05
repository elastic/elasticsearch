/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSingleValueOrNull;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;

/**
 * All other unit tests for {@link RewriteSumOfExpressionPlusConstant} live in
 * {@link RewriteSumOfExpressionPlusConstantSubstitutionOnlyGoldenTests}. The tests in this class could not be
 * moved there because {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAggregateNestedExpressionWithEval}
 * stores the canonical form of extracted arithmetic expressions, whose operand ordering depends on
 * randomly-assigned attribute IDs, making the golden output non-deterministic across runs.
 */
public class RewriteSumOfExpressionPlusConstantTests extends AbstractLogicalPlanOptimizerTests {

    public void testDuplicateAliasNotRewritten() {
        var plan = plan("""
            from test
            | stats s = sum(emp_no + 1), s = sum(emp_no + 2)
            | limit 10
            """, new TestSubstitutionOnlyOptimizer(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION));

        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("Duplicate alias should not be rewritten by RewriteSumOfExpressionPlusConstant", hasMvSingleValueOrNull);
        assertWarnings("Line 2:9: Field 's' shadowed by field at line 2:30");
    }

    public void testAvgOfFieldPlusConstantNotRewrittenByRule() {
        var plan = plan("""
            from test
            | stats a = avg(emp_no + 1), b = avg(emp_no + 2)
            """, new TestSubstitutionOnlyOptimizer(MvSingleValueOrNull.MV_SINGLE_VALUE_OR_NULL_TRANSPORT_VERSION));

        boolean hasMvSingleValueOrNull = plan.anyMatch(
            node -> node instanceof Eval e
                && e.fields().stream().anyMatch(f -> f instanceof Alias a && a.child() instanceof MvSingleValueOrNull)
        );
        assertFalse("AVG should not be rewritten by RewriteSumOfExpressionPlusConstant", hasMvSingleValueOrNull);
    }
}
