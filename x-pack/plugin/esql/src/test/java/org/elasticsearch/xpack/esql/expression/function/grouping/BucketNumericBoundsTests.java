/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.math.BigInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * Direct tests for the numeric span of 4-arg {@link Bucket} with whole-number from/to.
 * Unsigned_long bounds used to fold through their sortable encoding; these combinations would
 * produce a NaN or wildly-wrong span if that happened again. Large long bounds used to be
 * subtracted as doubles, losing the range above 2^53.
 */
public class BucketNumericBoundsTests extends ESTestCase {

    /**
     * Numeric from with unsigned_long to: with the encoded fold this was {@code 1000 - (-2^63+1000)},
     * an inverted range yielding a NaN span.
     */
    public void testUnsignedLongToBound() {
        assertThat(numberRoundTo(intLiteral(0), ulLiteral(1000)), equalTo(100.0));
    }

    /**
     * Unsigned_long from with numeric to: with the encoded fold this was {@code 1000 - (-2^63)},
     * a ~9.2e18 range yielding a 5e17 span.
     */
    public void testUnsignedLongFromBound() {
        assertThat(numberRoundTo(ulLiteral(0), intLiteral(1000)), equalTo(100.0));
    }

    public void testUnsignedLongBounds() {
        assertThat(numberRoundTo(ulLiteral(0), ulLiteral(1000)), equalTo(100.0));
    }

    /**
     * A tight range at the top of the unsigned_long domain: decoding each bound to a double separately
     * rounds both to 2^63 (doubles quantize in steps of 2048 there) and loses the range entirely, so the
     * range must be computed exactly before converting to a double.
     */
    public void testUnsignedLongTightHighBounds() {
        BigInteger from = BigInteger.ONE.shiftLeft(63);
        assertThat(numberRoundTo(ulLiteral(from), ulLiteral(from.add(BigInteger.valueOf(100)))), equalTo(10.0));
    }

    /**
     * Same tight range, mixing a signed long and an unsigned_long bound: both sides still have to be subtracted
     * exactly, decoding only the unsigned one.
     */
    public void testMixedSignedUnsignedTightHighBounds() {
        BigInteger max = BigInteger.valueOf(Long.MAX_VALUE);
        assertThat(numberRoundTo(longLiteral(Long.MAX_VALUE), ulLiteral(max.add(BigInteger.valueOf(100)))), equalTo(10.0));
        assertThat(numberRoundTo(ulLiteral(max.subtract(BigInteger.valueOf(100))), longLiteral(Long.MAX_VALUE)), equalTo(10.0));
    }

    /**
     * Two signed longs lose precision the same way once they exceed 2^53: both of these bounds round to 2^63 as
     * doubles, which used to yield a zero width.
     */
    public void testLongTightHighBounds() {
        assertThat(numberRoundTo(longLiteral(Long.MAX_VALUE - 100), longLiteral(Long.MAX_VALUE)), equalTo(10.0));
    }

    /**
     * Below 2^63 but above 2^53: doubles are 16 apart at 1e17, so {@code 1e17 + 10} used to round up to
     * {@code 1e17 + 16}, giving a 16-wide range and a bucket width of 5 instead of 1.
     */
    public void testLongBoundsAboveDoublePrecision() {
        assertThat(numberRoundTo(longLiteral(100_000_000_000_000_000L), longLiteral(100_000_000_000_000_010L), 10), equalTo(1.0));
    }

    /**
     * The full long domain: the exact range ({@code 2^64 - 1}) overflows a signed long, so the subtraction must
     * not be done in long arithmetic.
     */
    public void testLongFullRange() {
        assertThat(numberRoundTo(longLiteral(Long.MIN_VALUE), longLiteral(Long.MAX_VALUE)), equalTo(1e18));
    }

    /**
     * Small whole-number bounds are unaffected by the exact subtraction.
     */
    public void testIntBounds() {
        assertThat(numberRoundTo(intLiteral(0), intLiteral(1000)), equalTo(100.0));
        assertThat(numberRoundTo(intLiteral(-500), longLiteral(500)), equalTo(100.0));
    }

    /**
     * The absolute bound positions, as consumed by the {@code include_empty_buckets} cursor: unsigned_long
     * bounds must be decoded, not read as the (2^63 shifted) encoded value.
     */
    public void testNumericRangeBoundsDecodeUnsignedLong() {
        Bucket bucket = bucket(ulLiteral(new BigInteger("17764691215469285192")), new Literal(Source.EMPTY, 1000L, DataType.LONG));
        assertThat(bucket.numericRangeFrom(FoldContext.small()), equalTo(1.7764691215469285E19));
        assertThat(bucket.numericRangeTo(FoldContext.small()), equalTo(1000.0));
    }

    private static double numberRoundTo(Literal from, Literal to) {
        return numberRoundTo(from, to, 20);
    }

    private static double numberRoundTo(Literal from, Literal to, int buckets) {
        return bucket(from, to, buckets).getNumberRoundTo(FoldContext.small());
    }

    private static Bucket bucket(Literal from, Literal to) {
        return bucket(from, to, 20);
    }

    private static Bucket bucket(Literal from, Literal to, int buckets) {
        return new Bucket(
            Source.EMPTY,
            new Literal(Source.EMPTY, 100.0, DataType.DOUBLE),
            new Literal(Source.EMPTY, buckets, DataType.INTEGER),
            from,
            to,
            null,
            EsqlTestUtils.TEST_CFG
        );
    }

    private static Literal ulLiteral(long value) {
        return ulLiteral(BigInteger.valueOf(value));
    }

    private static Literal ulLiteral(BigInteger value) {
        return new Literal(Source.EMPTY, NumericUtils.asLongUnsigned(value), DataType.UNSIGNED_LONG);
    }

    private static Literal intLiteral(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal longLiteral(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }
}
