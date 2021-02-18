/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ql.TestUtils.of;

public class CanonicalizeTests extends ESTestCase {

    interface BinaryOperatorFactory {
        BinaryOperator<?,?,?,?> create(Source source, Expression left, Expression right);
    }

    public void testBasicOperators() throws Exception {
        List<BinaryOperatorFactory> list = Arrays.asList(
            // arithmetic
            Add::new, Mul::new,
            // logical
            Or::new, And::new
        );

        for (BinaryOperatorFactory factory : list) {
            Literal left = of(randomInt());
            Literal right = of(randomInt());

            BinaryOperator<?,?,?,?> first = factory.create(Source.EMPTY, left, right);
            BinaryOperator<?,?,?,?> second = factory.create(Source.EMPTY, right, left);

            assertNotEquals(first, second);
            assertTrue(first.semanticEquals(second));
            assertEquals(first, second.swapLeftAndRight());
            assertEquals(second, first.swapLeftAndRight());
        }
    }

    interface BinaryOperatorWithTzFactory {
        BinaryOperator<?,?,?,?> create(Source source, Expression left, Expression right, ZoneId zoneId);
    }

    public void testTimeZoneOperators() throws Exception {
        List<BinaryOperatorWithTzFactory> list = Arrays.asList(
            LessThan::new, LessThanOrEqual::new,
            Equals::new, NotEquals::new,
            GreaterThan::new, GreaterThanOrEqual::new
        );

        for (BinaryOperatorWithTzFactory factory : list) {
            Literal left = of(randomInt());
            Literal right = of(randomInt());
            ZoneId zoneId = randomZone();


            BinaryOperator<?, ?, ?, ?> first = factory.create(Source.EMPTY, left, right, zoneId);
            BinaryOperator<?,?,?,?> swap = first.swapLeftAndRight();

            assertNotEquals(first, swap);
            assertTrue(first.semanticEquals(swap));
        }
    }
}
