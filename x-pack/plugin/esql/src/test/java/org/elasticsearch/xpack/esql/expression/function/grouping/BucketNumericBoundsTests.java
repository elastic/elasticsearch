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
 * Direct tests for the numeric span of 4-arg {@link Bucket} with {@code unsigned_long} from/to.
 * Unsigned_long bounds used to fold through their sortable encoding; these combinations would
 * produce a NaN or wildly-wrong span if that happened again.
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
     * The absolute bound positions, as consumed by the {@code include_empty_buckets} cursor: unsigned_long
     * bounds must be decoded, not read as the (2^63 shifted) encoded value.
     */
    public void testNumericRangeBoundsDecodeUnsignedLong() {
        Bucket bucket = bucket(ulLiteral(new BigInteger("17764691215469285192")), new Literal(Source.EMPTY, 1000L, DataType.LONG));
        assertThat(bucket.numericRangeFrom(FoldContext.small()), equalTo(1.7764691215469285E19));
        assertThat(bucket.numericRangeTo(FoldContext.small()), equalTo(1000.0));
    }

    private static double numberRoundTo(Literal from, Literal to) {
        return bucket(from, to).getNumberRoundTo(FoldContext.small());
    }

    private static Bucket bucket(Literal from, Literal to) {
        return new Bucket(
            Source.EMPTY,
            new Literal(Source.EMPTY, 100.0, DataType.DOUBLE),
            new Literal(Source.EMPTY, 20, DataType.INTEGER),
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
}
