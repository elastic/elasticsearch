/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.notEqualsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class BooleanFunctionEqualsEliminationTests extends ESTestCase {
    private Expression booleanFunctionEqualElimination(BinaryComparison e) {
        return new BooleanFunctionEqualsElimination().rule(e, unboundLogicalOptimizerContext());
    }

    public void testBoolEqualsSimplificationOnExpressions() {
        Expression exp = new GreaterThan(EMPTY, getFieldAttribute(), new Literal(EMPTY, 0, DataType.INTEGER), null);

        assertEquals(exp, booleanFunctionEqualElimination(new Equals(EMPTY, exp, TRUE)));
        // TODO: Replace use of QL Not with ESQL Not
        assertEquals(new Not(EMPTY, exp), booleanFunctionEqualElimination(new Equals(EMPTY, exp, FALSE)));
    }

    public void testBoolEqualsSimplificationOnFields() {
        FieldAttribute field = getFieldAttribute();

        List<? extends BinaryComparison> comparisons = asList(
            new Equals(EMPTY, field, TRUE),
            new Equals(EMPTY, field, FALSE),
            notEqualsOf(field, TRUE),
            notEqualsOf(field, FALSE),
            new Equals(EMPTY, NULL, TRUE),
            new Equals(EMPTY, NULL, FALSE),
            notEqualsOf(NULL, TRUE),
            notEqualsOf(NULL, FALSE)
        );

        for (BinaryComparison comparison : comparisons) {
            assertEquals(comparison, booleanFunctionEqualElimination(comparison));
        }
    }

}
