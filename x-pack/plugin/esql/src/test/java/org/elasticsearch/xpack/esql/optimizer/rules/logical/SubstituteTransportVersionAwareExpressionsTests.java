/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SummationMode;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SubstituteTransportVersionAwareExpressionsTests extends ESTestCase {
    private static final TransportVersion ESQL_SUM_LONG_OVERFLOW_FIX = TransportVersion.fromName("esql_sum_long_overflow_fix");

    public void testSumNotReplacedWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, sameInstance(sum));
    }

    public void testSumReplacedWithCurrentVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
        assertThat(result, not(sameInstance(sum)));
    }

    /**
     * Checks that if an overflowing sum receives an old transport version, it won't be changed.
     * <p>
     *     This tests idempotence.
     * </p>
     */
    public void testSumAlreadyOverflowingWithOldVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_THROW
        );
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, oldVersion);
        assertThat(result, sameInstance(sum));
    }

    /**
     * Checks that an overflowing sum with a new transport version gets upgraded to safe long mode.
     */
    public void testSumOverflowingWithNewVersionUpgraded() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_THROW
        );
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
        assertThat(result, not(sameInstance(sum)));
    }

    /**
     * Checks that a safe long sum with a new transport version is not changed (idempotent).
     */
    public void testSumAlreadySafeWithNewVersion() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        Sum sum = new Sum(
            EMPTY,
            field,
            Literal.TRUE,
            AggregateFunction.NO_WINDOW,
            SummationMode.COMPENSATED_LITERAL,
            Sum.LONG_OVERFLOW_WARN
        );
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, sameInstance(sum));
    }

    public void testSumDoubleFieldWithNewVersion() {
        Expression field = getFieldAttribute("f", DataType.DOUBLE);
        Sum sum = new Sum(EMPTY, field);
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(sum, newVersion);
        assertThat(result, instanceOf(Sum.class));
        assertThat(((Sum) result).longOverflowMode(), is(Sum.LONG_OVERFLOW_WARN));
    }

    public void testNonTransportVersionAwareUnchanged() {
        Expression field = getFieldAttribute("f", DataType.LONG);
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(ESQL_SUM_LONG_OVERFLOW_FIX);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(field, oldVersion);
        assertThat(result, sameInstance(field));
    }

    public void testCidrMatchNotLoweredWithOldVersion() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ip, List.of(Literal.keyword(EMPTY, "10.0.0.0/8")));
        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, oldVersion);
        assertThat(result, sameInstance(cidrMatch));
    }

    public void testCidrMatchLoweredToMvInRangeWithCurrentVersion() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ip, List.of(Literal.keyword(EMPTY, "10.0.0.0/8")));
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, newVersion);
        assertThat(result, instanceOf(MvInRange.class));
        MvInRange range = (MvInRange) result;
        assertThat(range.field(), sameInstance(ip));
        assertThat(range.lower(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("10.0.0.0"), DataType.IP)));
        assertThat(range.upper(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("10.255.255.255"), DataType.IP)));
        assertThat(range.options(), nullValue());
    }

    public void testCidrMatchMultiBlockLoweredToOrOfMvInRange() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        CIDRMatch cidrMatch = new CIDRMatch(
            EMPTY,
            ip,
            List.of(Literal.keyword(EMPTY, "10.0.0.0/8"), Literal.keyword(EMPTY, "192.168.0.0/16"))
        );
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, newVersion);
        assertThat(result, instanceOf(Or.class));
        Or or = (Or) result;
        assertThat(or.left(), instanceOf(MvInRange.class));
        assertThat(or.right(), instanceOf(MvInRange.class));
        MvInRange left = (MvInRange) or.left();
        MvInRange right = (MvInRange) or.right();
        assertThat(left.lower(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("10.0.0.0"), DataType.IP)));
        assertThat(left.upper(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("10.255.255.255"), DataType.IP)));
        assertThat(right.lower(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("192.168.0.0"), DataType.IP)));
        assertThat(right.upper(), equalTo(new Literal(EMPTY, EsqlDataTypeConverter.stringToIP("192.168.255.255"), DataType.IP)));
    }

    public void testCidrMatchBareAddressLoweredToPointRange() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ip, List.of(Literal.keyword(EMPTY, "10.1.2.3")));
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, newVersion);
        assertThat(result, instanceOf(MvInRange.class));
        MvInRange range = (MvInRange) result;
        BytesRef addr = EsqlDataTypeConverter.stringToIP("10.1.2.3");
        assertThat(range.lower(), equalTo(new Literal(EMPTY, addr, DataType.IP)));
        assertThat(range.upper(), equalTo(new Literal(EMPTY, addr, DataType.IP)));
    }

    public void testCidrMatchNonFoldableBlockNotLowered() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        Expression cidrField = getFieldAttribute("cidr", DataType.KEYWORD);
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ip, List.of(cidrField));
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, newVersion);
        assertThat(result, sameInstance(cidrMatch));
    }

    public void testCidrMatchMalformedBlockNotLowered() {
        Expression ip = getFieldAttribute("ip", DataType.IP);
        CIDRMatch cidrMatch = new CIDRMatch(EMPTY, ip, List.of(Literal.keyword(EMPTY, "not-a-cidr")));
        TransportVersion newVersion = TransportVersionUtils.randomVersionSupporting(CIDRMatch.ESQL_CIDR_MATCH_MV_IN_RANGE);
        Expression result = SubstituteTransportVersionAwareExpressions.rule(cidrMatch, newVersion);
        assertThat(result, sameInstance(cidrMatch));
    }
}
