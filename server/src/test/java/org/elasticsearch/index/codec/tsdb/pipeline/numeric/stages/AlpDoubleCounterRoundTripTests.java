/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class AlpDoubleCounterRoundTripTests extends ESTestCase {

    private static final int[] BLOCK_SIZES = { 128, 512, 1024, 2048 };

    private static final int MAX_DECIMAL_PLACES = 15;

    public void testCounterRouteRoundTrip() throws IOException {
        for (int decimalPlaces = 1; decimalPlaces <= MAX_DECIMAL_PLACES; decimalPlaces++) {
            for (int bs : BLOCK_SIZES) {
                assertRoundTrip(counterPipeline(bs), counterStorageOrderBlock(bs, decimalPlaces), bs);
            }
        }
    }

    public void testCounterRouteRoundTripWithResets() throws IOException {
        for (int bs : BLOCK_SIZES) {
            assertRoundTrip(counterPipeline(bs), counterWithResets(bs), bs);
        }
    }

    private static PipelineConfig counterPipeline(int blockSize) {
        final int kMax = Math.clamp((long) blockSize / 32, 4, 64);
        return PipelineConfig.forDoubles(blockSize).alpDoubleStage().splitDelta(kMax).delta().offset().gcd().bitPack();
    }

    private static void assertRoundTrip(PipelineConfig config, long[] values, int blockSize) throws IOException {
        final long[] original = Arrays.copyOf(values, values.length);
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        final ByteBuffersDataOutput bufferOut = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(bufferOut, "test", "test")) {
            blockEncoder.encode(values, values.length, out);
        }

        final NumericDecoder decoder = NumericCodecFactory.DEFAULT.createDecoder(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();
        final long[] decoded = new long[blockSize];
        blockDecoder.decode(decoded, original.length, bufferOut.toDataInput());

        assertArrayEquals("bs=" + blockSize, original, Arrays.copyOf(decoded, original.length));
    }

    // Counter in TSDB storage order: descends within each _tsid run, jumps up at each boundary.
    private static long[] counterStorageOrderBlock(int size, int decimalPlaces) {
        final long[] values = new long[size];
        final double scale = Math.pow(10, decimalPlaces);
        final int runLength = 64;
        final double stepPerSample = 3.0;
        final double subSecond = 0.123456789;
        double seriesBase = 500_000.0;
        double current = seriesBase;
        for (int i = 0; i < size; i++) {
            if (i > 0 && (i % runLength) == 0) {
                seriesBase += 100_000.0;
                current = seriesBase;
            } else if (i > 0) {
                current -= stepPerSample;
            }
            values[i] = NumericUtils.doubleToSortableLong(Math.round((current + subSecond) * scale) / scale);
        }
        return values;
    }

    private static long[] counterWithResets(int size) {
        final long[] values = new long[size];
        final int runLength = 64;
        final double stepPerSample = 3.0;
        double current = 0.0;
        for (int i = 0; i < size; i++) {
            if (i > 0 && (i % runLength) == 0) {
                current = 0.0;
            } else if (i > 0) {
                current += stepPerSample;
            }
            values[i] = NumericUtils.doubleToSortableLong(Math.round((current + 0.5) * 100.0) / 100.0);
        }
        return values;
    }
}
