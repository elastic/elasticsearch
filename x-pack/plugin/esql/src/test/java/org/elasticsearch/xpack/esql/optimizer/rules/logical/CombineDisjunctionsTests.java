/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.FOUR;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ONE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.SIX;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.THREE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TWO;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalsOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.lessThanOf;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.relation;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;

public class CombineDisjunctionsTests extends ESTestCase {
    private Expression combineDisjunctions(Or e) {
        return new CombineDisjunctions().rule(e, unboundLogicalOptimizerContext());
    }

    private LogicalPlan combineDisjunctions(LogicalPlan l) {
        return new CombineDisjunctions().apply(l, unboundLogicalOptimizerContext());
    }

    public void testTwoEqualsWithOr() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Expression e = combineDisjunctions(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testTwoEqualsWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), equalsOf(fa, ONE));
        Expression e = combineDisjunctions(or);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals(ONE, eq.right());
    }

    public void testOneEqualsOneIn() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, List.of(TWO)));
        Expression e = combineDisjunctions(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testOneEqualsOneInWithSameValue() {
        FieldAttribute fa = getFieldAttribute();

        Or or = new Or(EMPTY, equalsOf(fa, ONE), new In(EMPTY, fa, asList(ONE, TWO)));
        Expression e = combineDisjunctions(or);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO));
    }

    public void testSingleValueInToEquals() {
        FieldAttribute fa = getFieldAttribute();

        Equals equals = equalsOf(fa, ONE);
        Or or = new Or(EMPTY, equals, new In(EMPTY, fa, List.of(ONE)));
        Expression e = combineDisjunctions(or);
        assertEquals(equals, e);
    }

    public void testEqualsBehindAnd() {
        FieldAttribute fa = getFieldAttribute();

        And and = new And(EMPTY, equalsOf(fa, ONE), equalsOf(fa, TWO));
        Filter dummy = new Filter(EMPTY, relation(), and);
        LogicalPlan transformed = combineDisjunctions(dummy);
        assertSame(dummy, transformed);
        assertEquals(and, ((Filter) transformed).condition());
    }

    public void testTwoEqualsDifferentFields() {
        FieldAttribute fieldOne = getFieldAttribute("ONE");
        FieldAttribute fieldTwo = getFieldAttribute("TWO");

        Or or = new Or(EMPTY, equalsOf(fieldOne, ONE), equalsOf(fieldTwo, TWO));
        Expression e = combineDisjunctions(or);
        assertEquals(or, e);
    }

    public void testMultipleIn() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), new In(EMPTY, fa, List.of(TWO)));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = combineDisjunctions(secondOr);
        assertEquals(In.class, e.getClass());
        In in = (In) e;
        assertEquals(fa, in.value());
        assertThat(in.list(), contains(ONE, TWO, THREE));
    }

    public void testOrWithNonCombinableExpressions() {
        FieldAttribute fa = getFieldAttribute();

        Or firstOr = new Or(EMPTY, new In(EMPTY, fa, List.of(ONE)), lessThanOf(fa, TWO));
        Or secondOr = new Or(EMPTY, firstOr, new In(EMPTY, fa, List.of(THREE)));
        Expression e = combineDisjunctions(secondOr);
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

        List<Expression> cidrs = new ArrayList<>(4);
        cidrs.add(new CIDRMatch(EMPTY, faa, ipa1));
        cidrs.add(new CIDRMatch(EMPTY, fab, ipb1));
        cidrs.add(new CIDRMatch(EMPTY, faa, ipa2));
        cidrs.add(new CIDRMatch(EMPTY, fab, ipb2));
        Or oldOr = (Or) Predicates.combineOr(cidrs);
        Expression e = combineDisjunctions(oldOr);
        assertEquals(Or.class, e.getClass());
        Or newOr = (Or) e;
        assertEquals(CIDRMatch.class, newOr.left().getClass());
        assertEquals(CIDRMatch.class, newOr.right().getClass());

        CIDRMatch cm1 = (CIDRMatch) newOr.left();
        CIDRMatch cm2 = (CIDRMatch) newOr.right();

        if (cm1.ipField() == faa) {
            assertEquals(fab, cm2.ipField());
            assertTrue(cm1.matches().size() == ipa.size() && cm1.matches().containsAll(ipa) && ipa.containsAll(cm1.matches()));
            assertTrue(cm2.matches().size() == ipb.size() && cm2.matches().containsAll(ipb) && ipb.containsAll(cm2.matches()));
        } else {
            assertEquals(fab, cm1.ipField());
            assertTrue(cm1.matches().size() == ipb.size() && cm1.matches().containsAll(ipb) && ipb.containsAll(cm1.matches()));
            assertTrue(cm2.matches().size() == ipa.size() && cm2.matches().containsAll(ipa) && ipa.containsAll(cm2.matches()));
        }
    }

    public void testCombineCIDRMatchEqualsIns() {
        FieldAttribute fa_cidr_a = getFieldAttribute("cidr_a");
        FieldAttribute fa_cidr_b = getFieldAttribute("cidr_b");
        FieldAttribute fa_in_a = getFieldAttribute("in_a");
        FieldAttribute fa_eqin_a = getFieldAttribute("eqin_a");
        FieldAttribute fa_eq_a = getFieldAttribute("eq_a");

        List<Expression> ipa1 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipa2 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipb1 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));
        List<Expression> ipb2 = randomList(1, 5, () -> new Literal(EMPTY, randomLiteral(DataType.IP).value(), DataType.IP));

        List<Expression> ipa = new HashSet<>(CollectionUtils.combine(ipa1, ipa2)).stream().toList();
        List<Expression> ipb = new HashSet<>(CollectionUtils.combine(ipb1, ipb2)).stream().toList();

        List<Expression> all = new ArrayList<>(10);
        all.add(new CIDRMatch(EMPTY, fa_cidr_a, ipa1));
        all.add(new CIDRMatch(EMPTY, fa_cidr_b, ipb1));
        all.add(new CIDRMatch(EMPTY, fa_cidr_a, ipa2));
        all.add(new CIDRMatch(EMPTY, fa_cidr_b, ipb2));

        all.add(new In(EMPTY, fa_in_a, List.of(ONE, TWO, THREE)));
        all.add(new In(EMPTY, fa_eqin_a, List.of(FOUR, FIVE, SIX)));
        all.add(new In(EMPTY, fa_in_a, List.of(THREE, FOUR, FIVE)));

        all.add(new Equals(EMPTY, fa_eq_a, ONE));
        all.add(new Equals(EMPTY, fa_eqin_a, ONE));
        all.add(new Equals(EMPTY, fa_eq_a, SIX));

        Or oldOr = (Or) Predicates.combineOr(all);

        Expression e = combineDisjunctions(oldOr);
        assertEquals(Or.class, e.getClass());
        Or newOr = (Or) e;
        assertEquals(Or.class, newOr.left().getClass());
        Or newOr1 = (Or) newOr.left();
        assertEquals(CIDRMatch.class, newOr.right().getClass());
        CIDRMatch cidr2 = (CIDRMatch) newOr.right();
        assertEquals(Or.class, e.getClass());

        assertEquals(Or.class, newOr1.left().getClass());
        Or newOr2 = (Or) newOr1.left();
        assertEquals(In.class, newOr2.left().getClass());
        In in1 = (In) newOr2.left();
        assertEquals(In.class, newOr2.right().getClass());
        In in2 = (In) newOr2.right();
        assertEquals(Or.class, newOr1.right().getClass());
        Or newOr3 = (Or) newOr1.right();
        assertEquals(In.class, newOr3.left().getClass());
        In in3 = (In) newOr3.left();
        assertEquals(CIDRMatch.class, newOr3.right().getClass());
        CIDRMatch cidr1 = (CIDRMatch) newOr3.right();

        if (cidr1.ipField() == fa_cidr_a) {
            assertEquals(fa_cidr_b, cidr2.ipField());
            assertTrue(cidr1.matches().size() == ipa.size() && cidr1.matches().containsAll(ipa) && ipa.containsAll(cidr1.matches()));
            assertTrue(cidr2.matches().size() == ipb.size() && cidr2.matches().containsAll(ipb) && ipb.containsAll(cidr2.matches()));
        } else {
            assertEquals(fa_cidr_b, cidr1.ipField());
            assertTrue(cidr1.matches().size() == ipb.size() && cidr1.matches().containsAll(ipb) && ipb.containsAll(cidr1.matches()));
            assertTrue(cidr2.matches().size() == ipa.size() && cidr2.matches().containsAll(ipa) && ipa.containsAll(cidr2.matches()));
        }

        if (in1.value() == fa_in_a) {
            assertTrue(in1.list().size() == 5 && in1.list().containsAll(List.of(ONE, TWO, THREE, FOUR, FIVE)));
            if (in2.value() == fa_eqin_a) {
                assertEquals(in3.value(), fa_eq_a);
                assertTrue(in3.list().size() == 2 && in3.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in2.list().size() == 4 && in2.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            } else {
                assertEquals(in2.value(), fa_eq_a);
                assertTrue(in2.list().size() == 2 && in2.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in3.list().size() == 4 && in3.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            }
        } else if (in2.value() == fa_in_a) {
            assertTrue(in2.list().size() == 5 && in2.list().containsAll(List.of(ONE, TWO, THREE, FOUR, FIVE)));
            if (in1.value() == fa_eqin_a) {
                assertEquals(in3.value(), fa_eq_a);
                assertTrue(in3.list().size() == 2 && in3.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in1.list().size() == 4 && in1.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            } else {
                assertEquals(in3.value(), fa_eq_a);
                assertTrue(in3.list().size() == 2 && in3.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in1.list().size() == 4 && in1.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            }
        } else {
            assertTrue(in3.list().size() == 5 && in3.list().containsAll(List.of(ONE, TWO, THREE, FOUR, FIVE)));
            if (in1.value() == fa_eqin_a) {
                assertEquals(in2.value(), fa_eq_a);
                assertTrue(in2.list().size() == 2 && in2.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in1.list().size() == 4 && in1.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            } else {
                assertEquals(in1.value(), fa_eq_a);
                assertTrue(in1.list().size() == 2 && in1.list().containsAll(List.of(ONE, SIX)));
                assertTrue(in2.list().size() == 4 && in2.list().containsAll(List.of(ONE, FOUR, FIVE, SIX)));
            }
        }
    }

    public void testCombineIpEqualityIntoCidrMatchUsesKeywordType() {
        FieldAttribute ipField = getFieldAttribute("ip_field", DataType.IP);

        BytesRef cidrPattern = new BytesRef("10.0.0.0/8");
        List<Expression> cidrPatterns = List.of(new Literal(EMPTY, cidrPattern, DataType.KEYWORD));

        BytesRef encodedIp = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")));
        Literal ipLiteral = new Literal(EMPTY, encodedIp, DataType.IP);
        Equals ipEquals = new Equals(EMPTY, ipField, ipLiteral, null);

        // CIDRMatch(ipField, "10.0.0.0/8") OR ipField == 127.0.0.1::ip
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ipField, cidrPatterns);
        Or or = new Or(EMPTY, cidrMatch, ipEquals);

        Expression result = combineDisjunctions(or);
        assertEquals(CIDRMatch.class, result.getClass());
        CIDRMatch combined = (CIDRMatch) result;
        // CIDRMatch(ipField, "10.0.0.0/8", "127.0.0.1")
        assertTrue(result.resolved());
        assertEquals(ipField, combined.ipField());
        assertEquals(2, combined.matches().size());

        for (Expression match : combined.matches()) {
            assertEquals(DataType.KEYWORD, match.dataType());
        }
    }

    public void testCombineIpInIntoCidrMatchUsesKeywordType() {
        FieldAttribute ipField = getFieldAttribute("ip_field", DataType.IP);

        BytesRef cidrPattern = new BytesRef("10.0.0.0/8");
        List<Expression> cidrPatterns = List.of(new Literal(EMPTY, cidrPattern, DataType.KEYWORD));

        BytesRef encodedIp1 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")));
        BytesRef encodedIp2 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.1")));
        Literal ipLiteral1 = new Literal(EMPTY, encodedIp1, DataType.IP);
        Literal ipLiteral2 = new Literal(EMPTY, encodedIp2, DataType.IP);
        In ipIn = new In(EMPTY, ipField, List.of(ipLiteral1, ipLiteral2));

        // CIDRMatch(ipField, "10.0.0.0/8") OR ipField IN (127.0.0.1::ip, 192.168.1.1::ip)
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ipField, cidrPatterns);
        Or or = new Or(EMPTY, cidrMatch, ipIn);

        Expression result = combineDisjunctions(or);
        assertEquals(CIDRMatch.class, result.getClass());
        CIDRMatch combined = (CIDRMatch) result;
        // CIDRMatch(ipField, "10.0.0.0/8", "127.0.0.1", "192.168.1.1")
        assertTrue(result.resolved());
        assertEquals(ipField, combined.ipField());
        assertEquals(3, combined.matches().size());

        for (Expression match : combined.matches()) {
            assertEquals(DataType.KEYWORD, match.dataType());
        }
    }

    /**
     * When there's no CIDRMatch at all, the IN should remain unchanged because
     * the {@code ips} map is never consumed (the {@code cidrs.isEmpty()} guard prevents it).
     */
    public void testIpInWithoutCidrMatchRemainsAsIn() {
        FieldAttribute ipField = getFieldAttribute("ip_field", DataType.IP);

        BytesRef encodedIp1 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")));
        BytesRef encodedIp2 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.1")));
        Literal ipLiteral1 = new Literal(EMPTY, encodedIp1, DataType.IP);
        Literal ipLiteral2 = new Literal(EMPTY, encodedIp2, DataType.IP);
        In ipIn = new In(EMPTY, ipField, List.of(ipLiteral1, ipLiteral2));

        // ipField IN (127.0.0.1::ip) OR ipField IN (192.168.1.1::ip) — no CIDRMatch present
        In ipIn2 = new In(EMPTY, ipField, List.of(ipLiteral2));
        Or or = new Or(EMPTY, ipIn, ipIn2);

        Expression result = combineDisjunctions(or);
        assertEquals(In.class, result.getClass());
        In combined = (In) result;
        assertEquals(ipField, combined.value());
        assertEquals(2, combined.list().size());
        for (Expression item : combined.list()) {
            assertEquals(DataType.IP, item.dataType());
        }
    }

    /**
     * When a CIDRMatch exists on a different field, the IN's IP values still get
     * merged into a new CIDRMatch for their own field (because {@code cidrs.isEmpty()} is false).
     * Verify the resulting CIDRMatch literals are KEYWORD-typed.
     */
    public void testIpInPromotedToCidrMatchWhenCidrExistsOnDifferentField() {
        FieldAttribute ipField1 = getFieldAttribute("ip_field_1", DataType.IP);
        FieldAttribute ipField2 = getFieldAttribute("ip_field_2", DataType.IP);

        BytesRef cidrPattern = new BytesRef("10.0.0.0/8");
        List<Expression> cidrPatterns = List.of(new Literal(EMPTY, cidrPattern, DataType.KEYWORD));
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ipField1, cidrPatterns);

        BytesRef encodedIp = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")));
        Literal ipLiteral = new Literal(EMPTY, encodedIp, DataType.IP);
        In ipIn = new In(EMPTY, ipField2, List.of(ipLiteral));

        // CIDRMatch(ip_field_1, "10.0.0.0/8") OR ip_field_2 IN (127.0.0.1::ip)
        Or or = new Or(EMPTY, cidrMatch, ipIn);

        Expression result = combineDisjunctions(or);
        // Should become: CIDRMatch(ip_field_1, "10.0.0.0/8") OR CIDRMatch(ip_field_2, "127.0.0.1")
        assertEquals(Or.class, result.getClass());
        Or resultOr = (Or) result;

        CIDRMatch left = (CIDRMatch) resultOr.left();
        assertEquals(ipField1, left.ipField());
        assertEquals(1, left.matches().size());
        assertEquals(DataType.KEYWORD, left.matches().get(0).dataType());

        CIDRMatch right = (CIDRMatch) resultOr.right();
        assertEquals(ipField2, right.ipField());
        assertEquals(1, right.matches().size());
        assertEquals(DataType.KEYWORD, right.matches().get(0).dataType());
    }

    /**
     * An IP equality combined with an IP IN on the same field, with a CIDRMatch also on that field.
     * All three should merge into a single CIDRMatch with KEYWORD-typed patterns.
     */
    public void testCombineIpEqualityAndInIntoCidrMatch() {
        FieldAttribute ipField = getFieldAttribute("ip_field", DataType.IP);

        BytesRef cidrPattern = new BytesRef("10.0.0.0/8");
        List<Expression> cidrPatterns = List.of(new Literal(EMPTY, cidrPattern, DataType.KEYWORD));
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ipField, cidrPatterns);

        BytesRef encodedIp1 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("127.0.0.1")));
        BytesRef encodedIp2 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.1")));
        BytesRef encodedIp3 = new BytesRef(InetAddressPoint.encode(InetAddresses.forString("172.16.0.1")));
        Literal ipLiteral1 = new Literal(EMPTY, encodedIp1, DataType.IP);
        Literal ipLiteral2 = new Literal(EMPTY, encodedIp2, DataType.IP);
        Literal ipLiteral3 = new Literal(EMPTY, encodedIp3, DataType.IP);

        Equals ipEquals = new Equals(EMPTY, ipField, ipLiteral1, null);
        In ipIn = new In(EMPTY, ipField, List.of(ipLiteral2, ipLiteral3));

        // CIDRMatch(ipField, "10.0.0.0/8") OR ipField == 127.0.0.1::ip OR ipField IN (192.168.1.1::ip, 172.16.0.1::ip)
        Or inner = new Or(EMPTY, cidrMatch, ipEquals);
        Or or = new Or(EMPTY, inner, ipIn);

        Expression result = combineDisjunctions(or);
        assertEquals(CIDRMatch.class, result.getClass());
        CIDRMatch combined = (CIDRMatch) result;
        assertEquals(ipField, combined.ipField());
        // "10.0.0.0/8" + "127.0.0.1" + "192.168.1.1" + "172.16.0.1"
        assertEquals(4, combined.matches().size());

        for (Expression match : combined.matches()) {
            assertEquals(DataType.KEYWORD, match.dataType());
        }
    }
}
