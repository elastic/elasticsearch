/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.greaterThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;

public class PropagateNullableTests extends ESTestCase {
    private Literal nullOf(DataType dataType) {
        return new Literal(Source.EMPTY, null, dataType);
    }

    // a IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNull() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        assertEquals(FALSE, new PropagateNullable().rule(and));
    }

    // a IS NULL AND b IS NOT NULL AND c IS NULL AND d IS NOT NULL AND e IS NULL AND a IS NOT NULL => false
    public void testIsNullAndNotNullMultiField() {
        FieldAttribute fa = getFieldAttribute();

        And andOne = new And(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, getFieldAttribute()));
        And andTwo = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, getFieldAttribute()));
        And andThree = new And(EMPTY, new IsNull(EMPTY, getFieldAttribute()), new IsNotNull(EMPTY, fa));

        And and = new And(EMPTY, andOne, new And(EMPTY, andThree, andTwo));

        assertEquals(FALSE, new PropagateNullable().rule(and));
    }

    // a IS NULL AND a > 1 => a IS NULL AND false
    public void testIsNullAndComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And and = new And(EMPTY, isNull, greaterThanOf(fa, ONE));
        assertEquals(new And(EMPTY, isNull, nullOf(BOOLEAN)), new PropagateNullable().rule(and));
    }

    // a IS NULL AND b < 1 AND c < 1 AND a < 1 => a IS NULL AND b < 1 AND c < 1 => a IS NULL AND b < 1 AND c < 1
    public void testIsNullAndMultipleComparison() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        And nestedAnd = new And(EMPTY, lessThanOf(getFieldAttribute("b"), ONE), lessThanOf(getFieldAttribute("c"), ONE));
        And and = new And(EMPTY, isNull, nestedAnd);
        And top = new And(EMPTY, and, lessThanOf(fa, ONE));

        Expression optimized = new PropagateNullable().rule(top);
        Expression expected = new And(EMPTY, and, nullOf(BOOLEAN));
        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // ((a+1)/2) > 1 AND a + 2 AND a IS NULL AND b < 3 => NULL AND NULL AND a IS NULL AND b < 3
    public void testIsNullAndDeeplyNestedExpression() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Expression nullified = new And(
            EMPTY,
            greaterThanOf(new Div(EMPTY, new Add(EMPTY, fa, ONE), TWO), ONE),
            greaterThanOf(new Add(EMPTY, fa, TWO), ONE)
        );
        Expression kept = new And(EMPTY, isNull, lessThanOf(getFieldAttribute("b"), THREE));
        And and = new And(EMPTY, nullified, kept);

        Expression optimized = new PropagateNullable().rule(and);
        Expression expected = new And(EMPTY, new And(EMPTY, nullOf(BOOLEAN), nullOf(BOOLEAN)), kept);

        assertEquals(Predicates.splitAnd(expected), Predicates.splitAnd(optimized));
    }

    // a IS NULL OR a IS NOT NULL => no change
    // a IS NULL OR a > 1 => no change
    public void testIsNullInDisjunction() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, new IsNull(EMPTY, fa), new IsNotNull(EMPTY, fa));
        Filter dummy = new Filter(EMPTY, relation(), or);
        LogicalPlan transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());

        or = new Or(EMPTY, new IsNull(EMPTY, fa), greaterThanOf(fa, ONE));
        dummy = new Filter(EMPTY, relation(), or);
        transformed = new PropagateNullable().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(or, ((Filter) transformed).condition());
    }

    // a + 1 AND (a IS NULL OR a > 3) => no change
    public void testIsNullDisjunction() {
        FieldAttribute fa = getFieldAttribute();
        IsNull isNull = new IsNull(EMPTY, fa);

        Or or = new Or(EMPTY, isNull, greaterThanOf(fa, THREE));
        And and = new And(EMPTY, new Add(EMPTY, fa, ONE), or);

        assertEquals(and, new PropagateNullable().rule(and));
    }
}
