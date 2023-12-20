/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.XYEncodingUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Function;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;

public class CartesianShapeCoordinateEncoderTests extends ESTestCase {

    public void verifyEncoding(
        double min,
        double max,
        double invalidValue,
        Function<Double, Double> allowedDiff,
        Function<Double, Integer> encode,
        Function<Integer, Double> decode,
        Function<Double, Integer> expected
    ) {
        double randomValue = randomDoubleBetween(min, max, true);
        assertThat(encode.apply((double) Float.POSITIVE_INFINITY), equalTo(Integer.MAX_VALUE));
        assertThat(encode.apply((double) Float.NEGATIVE_INFINITY), equalTo(Integer.MIN_VALUE));
        int encoded = encode.apply(randomValue);
        assertThat(encoded, equalTo(expected.apply(randomValue)));
        Exception e = expectThrows(IllegalArgumentException.class, "Expect failure for " + invalidValue, () -> encode.apply(invalidValue));
        assertThat(e.getMessage(), endsWith("must be between " + min + " and " + max));

        assertThat(decode.apply(encoded), closeTo(randomValue, allowedDiff.apply(randomValue)));
        assertThat(decode.apply(Integer.MAX_VALUE), equalTo(Double.POSITIVE_INFINITY));
        assertThat(decode.apply(Integer.MIN_VALUE), equalTo(Double.NEGATIVE_INFINITY));
    }

    public void verifyEncoding(Function<Double, Integer> encode, Function<Integer, Double> decode) {
        verifyEncoding(
            -Float.MAX_VALUE,
            Float.MAX_VALUE,
            randomFrom(Float.NaN),
            v -> Math.abs(v - v.floatValue()),
            encode,
            decode,
            v -> XYEncodingUtils.encode(v.floatValue())
        );
    }

    public void testX() {
        verifyEncoding(CoordinateEncoder.CARTESIAN::encodeX, CoordinateEncoder.CARTESIAN::decodeX);
    }

    public void testY() {
        verifyEncoding(CoordinateEncoder.CARTESIAN::encodeY, CoordinateEncoder.CARTESIAN::decodeY);
    }
}
