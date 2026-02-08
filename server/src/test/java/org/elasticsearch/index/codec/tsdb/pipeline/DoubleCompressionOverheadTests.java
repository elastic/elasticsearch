/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Arrays;

public class DoubleCompressionOverheadTests extends CompressionOverheadTestCase {

    public void testConstantData() throws IOException {
        assertMaxOneByteOverhead("constant", constantDoubleData());
    }

    public void testPercentageData() throws IOException {
        assertMaxOneByteOverhead("percentage", percentageData());
    }

    public void testMonotonicData() throws IOException {
        assertMaxOneByteOverhead("monotonic", monotonicDoubleData());
    }

    public void testGaugeData() throws IOException {
        assertMaxOneByteOverhead("gauge", gaugeDoubleData());
    }

    public void testRandomData() throws IOException {
        assertMaxOneByteOverhead("random", randomDoubleData());
    }

    private void assertMaxOneByteOverhead(final String dataType, final double[] originalDoubles) throws IOException {
        assertMaxOneByteOverhead("double:" + dataType, doublesToSortableLongs(originalDoubles));
    }

    private long[] doublesToSortableLongs(final double[] doubles) {
        final long[] longs = new long[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            longs[i] = NumericUtils.doubleToSortableLong(doubles[i]);
        }
        return longs;
    }

    private double[] constantDoubleData() {
        final double[] data = new double[BLOCK_SIZE];
        Arrays.fill(data, randomDoubleBetween(1.0, 1000.0, true));
        return data;
    }

    private double[] percentageData() {
        final double[] data = new double[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = randomDoubleBetween(0.0, 100.0, true);
        }
        return data;
    }

    private double[] monotonicDoubleData() {
        final double[] data = new double[BLOCK_SIZE];
        double value = randomDoubleBetween(0.0, 100.0, true);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            value += randomDoubleBetween(0.1, 10.0, true);
            data[i] = value;
        }
        return data;
    }

    private double[] gaugeDoubleData() {
        final double[] data = new double[BLOCK_SIZE];
        final double baseline = randomDoubleBetween(50.0, 100.0, true);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = baseline + randomDoubleBetween(-10.0, 10.0, true);
        }
        return data;
    }

    private double[] randomDoubleData() {
        final double[] data = new double[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = randomDoubleBetween(-1_000_000.0, 1_000_000.0, true);
        }
        return data;
    }
}
