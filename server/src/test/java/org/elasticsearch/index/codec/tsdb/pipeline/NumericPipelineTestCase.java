/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public abstract class NumericPipelineTestCase extends ESTestCase {

    protected static final Logger logger = LogManager.getLogger(NumericPipelineTestCase.class);

    protected static final int BLOCK_SIZE = ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    protected long[] timestampData() {
        final long[] data = new long[BLOCK_SIZE];
        long baseTimestamp = randomLongBetween(1000000L, 2000000L);
        long interval = randomLongBetween(100L, 200L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = baseTimestamp + (i * interval);
        }
        return data;
    }

    protected long[] counterData() {
        final long[] data = new long[BLOCK_SIZE];
        long counter = randomLongBetween(1000000L, 2000000L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            counter += randomIntBetween(1, 100);
            data[i] = counter;
        }
        return data;
    }

    protected long[] gaugeData() {
        final long[] data = new long[BLOCK_SIZE];
        long baseline = randomLongBetween(40000L, 60000L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = baseline + randomIntBetween(-100, 100);
        }
        return data;
    }

    protected long[] gcdData() {
        final long[] data = new long[BLOCK_SIZE];
        long gcd = randomLongBetween(10L, 100L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = gcd * randomIntBetween(1, 1000);
        }
        return data;
    }

    protected long[] constantData() {
        final long[] data = new long[BLOCK_SIZE];
        Arrays.fill(data, randomLongBetween(1L, 1000L));
        return data;
    }

    protected long[] randomData() {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = randomLongBetween(0, 1_000_000L);
        }
        return data;
    }

    protected long[] decreasingTimestampData() {
        final long[] data = new long[BLOCK_SIZE];
        long baseTimestamp = randomLongBetween(1000000L, 2000000L);
        long interval = randomLongBetween(100L, 200L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = baseTimestamp - (i * interval);
        }
        return data;
    }

    protected long[] boundaryData() {
        final long[] data = new long[BLOCK_SIZE];
        data[0] = 0L;
        data[1] = 1L;
        data[2] = Long.MAX_VALUE / 2;
        for (int i = 3; i < BLOCK_SIZE; i++) {
            data[i] = randomLongBetween(0, Long.MAX_VALUE / 2);
        }
        return data;
    }

    protected long[] smallData() {
        final long[] data = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = randomIntBetween(0, 255);
        }
        return data;
    }

    protected long[] timestampWithJitterData() {
        return timestampWithJitterData(10);
    }

    protected long[] timestampWithJitterData(int jitterProbabilityPercent) {
        final long[] data = new long[BLOCK_SIZE];
        final long baseDelta = 1000L;
        final double jitterRatio = 0.05;
        final long maxJitter = (long) (baseDelta * jitterRatio);
        long current = randomLongBetween(0L, 1_000_000L);
        for (int i = 0; i < BLOCK_SIZE; i++) {
            data[i] = current;
            long jitter = 0;
            if (maxJitter > 0 && randomIntBetween(1, 100) <= jitterProbabilityPercent) {
                jitter = randomLongBetween(-maxJitter, maxJitter);
            }
            current += baseDelta + jitter;
        }
        return data;
    }

    protected long[] allStagesData() {
        final long[] data = new long[BLOCK_SIZE];
        long baseDelta = 100L;
        long step = 2L;
        long current = randomLongBetween(1_000_000L, 10_000_000L);
        data[0] = current;
        for (int i = 1; i < BLOCK_SIZE; i++) {
            long delta = baseDelta + (i - 1) * step;
            current += delta;
            data[i] = current;
        }
        return data;
    }

    protected void assertRoundTrip(final NumericCodec codec, final long[] original, final String dataType) throws IOException {
        final EncodeResult result = encodeAndDecode(codec, original.clone());
        assertArrayEquals(original, result.decoded);
        logCompressionStats(dataType, original.length, result.encodedSize);
    }

    protected void assertRoundTripWithAllDataTypes(final NumericCodec codec) throws IOException {
        try (codec) {
            assertRoundTrip(codec, randomData(), "random");
            assertRoundTrip(codec, constantData(), "constant");
            assertRoundTrip(codec, smallData(), "small");
            assertRoundTrip(codec, timestampData(), "timestamp");
            assertRoundTrip(codec, counterData(), "counter");
            assertRoundTrip(codec, gaugeData(), "gauge");
            assertRoundTrip(codec, gcdData(), "gcd");
            assertRoundTrip(codec, decreasingTimestampData(), "decreasing-timestamp");
            assertRoundTrip(codec, boundaryData(), "boundary");
            assertRoundTrip(codec, timestampWithJitterData(), "timestamp-with-jitter");
            assertRoundTrip(codec, allStagesData(), "all-stages");
        }
    }

    private void logCompressionStats(final String dataType, int valueCount, int encodedSize) {
        int originalSize = valueCount * Long.BYTES;
        double ratio = (double) originalSize / encodedSize;
        double savings = (1.0 - (double) encodedSize / originalSize) * 100;
        logger.info(
            "[{}] original={} bytes, encoded={} bytes, ratio={}, savings={} %",
            dataType,
            originalSize,
            encodedSize,
            String.format("%.2fx", ratio),
            String.format("%.1f", savings)
        );
    }

    protected void assertMultipleBlocks(final NumericCodec codec) throws IOException {
        try (codec) {
            var encoder = codec.newEncoder();
            var decoder = codec.newDecoder();

            for (int block = 0; block < randomIntBetween(5, 10); block++) {
                final long[] original = randomData();
                final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
                final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

                encoder.encode(original.clone(), original.length, out);

                final long[] decoded = new long[BLOCK_SIZE];
                decoder.decode(decoded, new ByteArrayDataInput(buffer, 0, out.getPosition()));

                assertArrayEquals("Block " + block + " failed", original, decoded);
            }
        }
    }

    protected EncodeResult encodeAndDecode(final NumericCodec codec, final long[] values) throws IOException {
        final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        codec.newEncoder().encode(values, values.length, out);
        int encodedSize = out.getPosition();

        final long[] decoded = new long[BLOCK_SIZE];
        codec.newDecoder().decode(decoded, new ByteArrayDataInput(buffer, 0, encodedSize));

        return new EncodeResult(decoded, encodedSize);
    }

    protected record EncodeResult(long[] decoded, int encodedSize) {}
}
