/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDimensionCodec.Scratch;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class DerivedMetricsDimensionCodecTests extends ESTestCase {

    public void testRoundTrip() {
        assertRoundTrip("checkout", "eu-west-1");
        assertRoundTrip("checkout");
        assertRoundTrip(new String[] { null, null });
        assertRoundTrip("", "not empty");
    }

    /**
     * A document missing a dimension forms its own series rather than sharing a placeholder, so which dimensions were present has to
     * survive the round trip and has to make the encoding differ.
     */
    public void testAbsentDimensionsAreDistinctFromEmptyOnes() {
        Scratch scratch = new Scratch();
        BytesRef absent = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "a", null }, 2, scratch));
        BytesRef empty = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "a", "" }, 2, scratch));
        assertNotEquals(absent, empty);
        assertArrayEquals(new String[] { "a", null }, DerivedMetricsDimensionCodec.decode(absent, 2));
        assertArrayEquals(new String[] { "a", "" }, DerivedMetricsDimensionCodec.decode(empty, 2));
    }

    public void testSameValuesEncodeIdentically() {
        Scratch scratch = new Scratch();
        BytesRef first = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "checkout", "eu" }, 2, scratch));
        BytesRef second = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "checkout", "eu" }, 2, scratch));
        assertEquals(first, second);
    }

    public void testDifferentOrderIsADifferentSeries() {
        Scratch scratch = new Scratch();
        BytesRef first = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "a", "b" }, 2, scratch));
        BytesRef second = BytesRef.deepCopyOf(DerivedMetricsDimensionCodec.encode(new String[] { "b", "a" }, 2, scratch));
        assertNotEquals(first, second);
    }

    /**
     * The scratch buffer starts small and has to grow without corrupting what it already holds.
     */
    public void testScratchGrowsForLargeValues() {
        Scratch scratch = new Scratch();
        String large = randomAlphaOfLength(5000);
        assertRoundTrip(scratch, "small", large, "after");
    }

    public void testUnicodeSurvives() {
        assertRoundTrip("λ-service", "東京", "emoji 🚀");
    }

    public void testManyDimensionsSpanMultipleBitmapBytes() {
        String[] values = new String[20];
        for (int i = 0; i < values.length; i++) {
            values[i] = i % 3 == 0 ? null : "value-" + i;
        }
        assertEquals(3, DerivedMetricsDimensionCodec.bitmapLength(values.length));
        assertRoundTrip(values);
    }

    public void testReusingScratchDoesNotLeakPreviousValues() {
        Scratch scratch = new Scratch();
        DerivedMetricsDimensionCodec.encode(new String[] { "a-long-earlier-value", "second" }, 2, scratch);
        BytesRef shorter = DerivedMetricsDimensionCodec.encode(new String[] { "x", null }, 2, scratch);
        assertArrayEquals(new String[] { "x", null }, DerivedMetricsDimensionCodec.decode(shorter, 2));
    }

    private static void assertRoundTrip(String... values) {
        assertRoundTrip(new Scratch(), values);
    }

    private static void assertRoundTrip(Scratch scratch, String... values) {
        BytesRef encoded = DerivedMetricsDimensionCodec.encode(values, values.length, scratch);
        String[] decoded = DerivedMetricsDimensionCodec.decode(encoded, values.length);
        assertEquals(Arrays.toString(values), Arrays.toString(decoded));
    }
}
