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
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.randomLiteral;
import static org.hamcrest.Matchers.contains;

public class CombineDisjunctionsToInTests extends ESTestCase {
    public void testTwoEqualsWithOr() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testTwoEqualsWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, ONE));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals(ONE, eq.right());
    }

    public void testOneEqualsOneIn() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, List.of(TWO)));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testOneEqualsOneInWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, asList(ONE, TWO)));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testSingleValueInToEquals() {
        FieldAttribute fa = getFieldAttribute();

        Equals equals = equalsOf(fa, ONE);
        Or or = new Or(EMPTY, equals, new In(EMPTY, fa, List.of(ONE)));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(equals, e);
    }

    public void testEqualsBehindAnd() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Filter dummy = new Filter(EMPTY, relation(), and);
        LogicalPlan transformed = new CombineDisjunctionsToIn().apply(dummy);
        assertSame(dummy, transformed);
        assertEquals(and, ((Filter) transformed).condition());
    }

    public void testTwoEqualsDifferentFields() {
        FieldAttribute fieldOne = getFieldAttribute("ONE");
        FieldAttribute fieldTwo = getFieldAttribute("TWO");

        Or or = new Or(EMPTY, equalsOf(fieldOne, ONE), equalsOf(fieldTwo, TWO));
        Expression e = new CombineDisjunctionsToIn().rule(or);
        assertEquals(or, e);
    }

    public void testMultipleIn() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), new In(EMPTY, fa, List.of(TWO)));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = new CombineDisjunctionsToIn().rule(secondOr);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO, THREE));
    }

    public void testOrWithNonCombinableExpressions() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), lessThanOf(fa, TWO));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = new CombineDisjunctionsToIn().rule(secondOr);
        assertEquals(Or.class, e.getClass());
        Or or = (Or) e;
        assertEquals(or.left(), firstOr.right());
        assertEquals(In.class, or.right().getClass());
        In in = (In) or.right();
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, THREE));
    }

    public void testCombineCIDRMatch() {
        FieldAttribute faa = getFieldAttribute("a");
        FieldAttribute fab = getFieldAttribute("b");

        List<Expression> ipa1 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipa2 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipb1 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipb2 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));

        List<Expression> ipa = new HashSet<>(CollectionUtils.combine(ipa1, ipa2)).stream().toList();
        List<Expression> ipb = new HashSet<>(CollectionUtils.combine(ipb1, ipb2)).stream().toList();

        Or or1 = new Or(EMPTY, new CIDRMatch(EMPTY, faa, ipa1), new CIDRMatch(EMPTY, faa, ipa2));
        Or or2 = new Or(EMPTY, new CIDRMatch(EMPTY, fab, ipb1), new CIDRMatch(EMPTY, fab, ipb2));
        Or oldOr = new Or(EMPTY, or1, or2);
        Expression e = new CombineDisjunctionsToIn().rule(oldOr);
        assertEquals(Or.class, e.getClass());
        Or newOr = (Or) e;
        assertEquals(CIDRMatch.class, newOr.left().getClass());
        CIDRMatch cm1 = (CIDRMatch) newOr.left();
        assertEquals(faa, cm1.ipField());
        assertTrue(cm1.matches().size() == ipa.size() && cm1.matches().containsAll(ipa) && ipa.containsAll(cm1.matches()));
        assertEquals(CIDRMatch.class, newOr.right().getClass());
        CIDRMatch cm2 = (CIDRMatch) newOr.right();
        assertEquals(fab, cm2.ipField());
        assertTrue(cm2.matches().size() == ipb.size() && cm2.matches().containsAll(ipb) && ipb.containsAll(cm2.matches()));
    }
}
