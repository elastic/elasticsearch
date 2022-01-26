/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.ql.expression.predicate.Predicates;
import org.elasticsearch.xpack.ql.expression.predicate.logical.And;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.ql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.ql.TestUtils.equalsOf;
import static org.elasticsearch.xpack.ql.TestUtils.fieldAttribute;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOf;
import static org.elasticsearch.xpack.ql.TestUtils.greaterThanOrEqualOf;
import static org.elasticsearch.xpack.ql.TestUtils.lessThanOf;
import static org.elasticsearch.xpack.ql.TestUtils.notEqualsOf;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class CanonicalTests extends ESTestCase {

    Comparator<? super Expression> comparator = Comparator.comparingInt(Object::hashCode);

    public void testNonCommutativeBinary() throws Exception {
        Div div = new Div(EMPTY, of(2), of(1));
        Sub sub = new Sub(EMPTY, of(2), of(1));
        Mod mod = new Mod(EMPTY, div, sub);
        assertEquals(mod, mod.canonical());
    }

    public void testNonCommutativeMixedWithCommutative() throws Exception {
        // (0+1) / 1
        Div div = new Div(EMPTY, new Add(EMPTY, of(0), of(1)), of(1));
        // 1*2 - 1+2
        Sub sub = new Sub(EMPTY, new Mul(EMPTY, of(1), new Add(EMPTY, of(1), of(2))), new Add(EMPTY, of(1), of(2)));

        Div shuffledDiv = new Div(EMPTY, new Add(EMPTY, of(1), of(0)), of(1));
        Sub shuffledSub = new Sub(EMPTY, new Mul(EMPTY, new Add(EMPTY, of(2), of(1)), of(1)), new Add(EMPTY, of(2), of(1)));

        And and = new And(EMPTY, div, sub);
        And shuffledAnd = new And(EMPTY, shuffledDiv, shuffledSub);

        assertNotEquals(and, shuffledAnd);
        assertEquals(and.canonical(), shuffledAnd.canonical());
    }

    public void testAndManually() throws Exception {
        FieldAttribute a = fieldAttribute();
        FieldAttribute b = fieldAttribute();
        FieldAttribute c = fieldAttribute();
        FieldAttribute d = fieldAttribute();
        And one = new And(EMPTY, new And(EMPTY, a, b), new And(EMPTY, c, d));
        And two = new And(EMPTY, new And(EMPTY, c, a), new And(EMPTY, b, d));

        assertEquals(one.canonical(), two.canonical());
        assertEquals(one.semanticHash(), two.semanticHash());
    }

    public void testBasicSymmetricalAdd() throws Exception {
        Expression left = new Add(EMPTY, new Add(EMPTY, of(1), of(2)), new Add(EMPTY, of(3), of(4)));
        Expression right = new Add(EMPTY, new Add(EMPTY, of(4), of(2)), new Add(EMPTY, of(1), of(3)));

        assertEquals(left.canonical(), right.canonical());
        assertEquals(left.semanticHash(), right.semanticHash());
    }

    public void testBasicASymmetricalAdd() throws Exception {
        Expression left = new Add(EMPTY, new Add(EMPTY, of(1), of(2)), of(3));
        Expression right = new Add(EMPTY, of(1), new Add(EMPTY, of(2), of(3)));

        assertEquals(left.canonical(), right.canonical());
        assertEquals(left.semanticHash(), right.semanticHash());
    }

    public void testBasicAnd() throws Exception {
        testBinaryLogic(Predicates::combineAnd);
    }

    public void testBasicOr() throws Exception {
        testBinaryLogic(Predicates::combineAnd);
    }

    private void testBinaryLogic(Function<List<Expression>, Expression> combiner) {
        List<Expression> children = randomList(2, 128, TestUtils::fieldAttribute);
        Expression expression = combiner.apply(children);
        Collections.shuffle(children, random());
        Expression shuffledExpression = combiner.apply(children);
        assertTrue(expression.semanticEquals(shuffledExpression));
        assertEquals(expression.semanticHash(), shuffledExpression.semanticHash());
    }

    public void testBinaryOperatorCombinations() throws Exception {
        FieldAttribute a = fieldAttribute();
        FieldAttribute b = fieldAttribute();
        FieldAttribute c = fieldAttribute();
        FieldAttribute d = fieldAttribute();

        And ab = new And(EMPTY, greaterThanOf(a, of(1)), lessThanOf(b, of(2)));
        And cd = new And(EMPTY, equalsOf(new Add(EMPTY, c, of(20)), of(3)), greaterThanOrEqualOf(d, of(4)));

        And and = new And(EMPTY, ab, cd);

        // swap d comparison
        And db = new And(EMPTY, greaterThanOrEqualOf(d, of(4)).swapLeftAndRight(), lessThanOf(b, of(2)));
        // swap order for c and swap a comparison
        And ca = new And(EMPTY, equalsOf(new Add(EMPTY, of(20), c), of(3)), greaterThanOf(a, of(1)));

        And shuffleAnd = new And(EMPTY, db, ca);

        assertEquals(and.canonical(), shuffleAnd.canonical());
    }

    public void testNot() throws Exception {
        FieldAttribute a = fieldAttribute();
        FieldAttribute b = fieldAttribute();
        FieldAttribute c = fieldAttribute();
        FieldAttribute d = fieldAttribute();

        And ab = new And(EMPTY, greaterThanOf(a, of(1)), lessThanOf(b, of(2)));
        And cd = new And(EMPTY, equalsOf(new Add(EMPTY, c, of(20)), of(3)), greaterThanOrEqualOf(d, of(4)));
        And and = new And(EMPTY, ab, cd);

        // swap d comparison
        Or db = new Or(EMPTY, new Not(EMPTY, greaterThanOrEqualOf(d, of(4))), lessThanOf(b, of(2)).negate());
        // swap order for c and swap a comparison
        Or ca = new Or(EMPTY, notEqualsOf(new Add(EMPTY, of(20), c), of(3)), new Not(EMPTY, greaterThanOf(a, of(1))));

        Not not = new Not(EMPTY, new Or(EMPTY, db, ca));

        Expression expected = and.canonical();
        Expression actual = not.canonical();
        assertEquals(and.canonical(), not.canonical());
    }

    public void testLiteralHashSorting() throws Exception {
        DataType type = randomFrom(DataTypes.types());
        List<Expression> list = randomList(10, 1024, () -> new Literal(EMPTY, randomInt(), type));
        List<Expression> shuffle = new ArrayList<>(list);
        Collections.shuffle(shuffle, random());

        assertNotEquals(list, shuffle);

        list.sort(comparator);
        shuffle.sort(comparator);

        assertEquals(list, shuffle);
    }

    public void testInManual() throws Exception {
        FieldAttribute value = fieldAttribute();

        Literal a = new Literal(EMPTY, 1, DataTypes.INTEGER);
        Literal b = new Literal(EMPTY, 2, DataTypes.INTEGER);
        Literal c = new Literal(EMPTY, 3, DataTypes.INTEGER);

        In in = new In(EMPTY, value, asList(a, b, c));
        In anotherIn = new In(EMPTY, value, asList(b, a, c));

        assertTrue(in.semanticEquals(anotherIn));
        assertEquals(in.semanticHash(), anotherIn.semanticHash());
    }

    public void testIn() throws Exception {
        FieldAttribute value = fieldAttribute();
        List<Expression> list = randomList(randomInt(1024), () -> new Literal(EMPTY, randomInt(), DataTypes.INTEGER));
        In in = new In(EMPTY, value, list);
        List<Expression> shuffledList = new ArrayList<>(list);
        Collections.shuffle(shuffledList, random());
        In shuffledIn = new In(EMPTY, value, shuffledList);

        assertEquals(in.semanticHash(), shuffledIn.semanticHash());
        assertTrue(in.semanticEquals(shuffledIn));
    }

    interface BinaryOperatorFactory {
        BinaryOperator<?, ?, ?, ?> create(Source source, Expression left, Expression right);
    }

    public void testBasicOperators() throws Exception {
        List<BinaryOperatorFactory> list = Arrays.asList(
            // arithmetic
            Add::new,
            Mul::new,
            // logical
            Or::new,
            And::new
        );

        for (BinaryOperatorFactory factory : list) {
            Literal left = of(randomInt());
            Literal right = of(randomInt());

            BinaryOperator<?, ?, ?, ?> first = factory.create(Source.EMPTY, left, right);
            BinaryOperator<?, ?, ?, ?> second = factory.create(Source.EMPTY, right, left);

            assertNotEquals(first, second);
            assertTrue(first.semanticEquals(second));
            assertEquals(first, second.swapLeftAndRight());
            assertEquals(second, first.swapLeftAndRight());
        }
    }

    interface BinaryOperatorWithTzFactory {
        BinaryOperator<?, ?, ?, ?> create(Source source, Expression left, Expression right, ZoneId zoneId);
    }

    public void testTimeZoneOperators() throws Exception {
        List<BinaryOperatorWithTzFactory> list = Arrays.asList(
            LessThan::new,
            LessThanOrEqual::new,
            Equals::new,
            NotEquals::new,
            GreaterThan::new,
            GreaterThanOrEqual::new
        );

        for (BinaryOperatorWithTzFactory factory : list) {
            Literal left = of(randomInt());
            Literal right = of(randomInt());
            ZoneId zoneId = randomZone();

            BinaryOperator<?, ?, ?, ?> first = factory.create(Source.EMPTY, left, right, zoneId);
            BinaryOperator<?, ?, ?, ?> swap = first.swapLeftAndRight();

            assertNotEquals(first, swap);
            assertTrue(first.semanticEquals(swap));
        }
    }
}
