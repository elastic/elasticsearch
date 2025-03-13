/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.random.RandomGenerator;

public class ResamplerTests extends ESTestCase {

    private Resampler createResampler(GetStackTracesRequest request, double sampleRate, long totalCount) {
        return new Resampler(request, sampleRate, totalCount) {
            @Override
            protected RandomGenerator createRandom(GetStackTracesRequest request) {
                return DeterministicRandom.of(0.0d, 1.0d);
            }
        };
    }

    public void testNoResamplingNoSampleRateAdjustment() {
        // corresponds to profiling-events-5pow01
        double sampleRate = 1.0d / Math.pow(5.0d, 1);
        int requestedSamples = 20_000;
        int actualTotalSamples = 10_000;

        GetStackTracesRequest request = new GetStackTracesRequest(
            requestedSamples,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(false);

        Resampler resampler = createResampler(request, sampleRate, actualTotalSamples);

        int actualSamplesSingleTrace = 5_000;
        assertEquals(5_000, resampler.adjustSampleCount(actualSamplesSingleTrace));
    }

    public void testNoResamplingButAdjustSampleRate() {
        // corresponds to profiling-events-5pow01
        double sampleRate = 1.0d / Math.pow(5.0d, 1);
        int requestedSamples = 20_000;
        int actualTotalSamples = 10_000;

        GetStackTracesRequest request = new GetStackTracesRequest(
            requestedSamples,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(true);

        Resampler resampler = createResampler(request, sampleRate, actualTotalSamples);

        int actualSamplesSingleTrace = 5_000;
        assertEquals(25_000, resampler.adjustSampleCount(actualSamplesSingleTrace));
    }

    public void testResamplingNoSampleRateAdjustment() {
        // corresponds to profiling-events-5pow01
        double sampleRate = 1.0d / Math.pow(5.0d, 1);
        int requestedSamples = 20_000;
        int actualTotalSamples = 40_000;

        GetStackTracesRequest request = new GetStackTracesRequest(
            requestedSamples,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(false);

        Resampler resampler = createResampler(request, sampleRate, actualTotalSamples);

        int actualSamplesSingleTrace = 20_000;
        assertEquals(20_000, resampler.adjustSampleCount(actualSamplesSingleTrace));
    }

    public void testResamplingNoSampleRateAdjustmentWithQuery() {
        double sampleRate = 1.0d;
        int requestedSamples = 1;
        int actualTotalSamples = 200;
        // there is only one event
        int actualSamplesSingleTrace = 200;

        GetStackTracesRequest request = new GetStackTracesRequest(
            requestedSamples,
            1.0d,
            1.0d,
            1.0d,
            new BoolQueryBuilder().filter(
                new RangeQueryBuilder("@timestamp").lt("2023-10-19 15:33:00").gte("2023-09-20 15:31:52").format("yyyy-MM-dd HH:mm:ss")
            ),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        request.setAdjustSampleCount(false);
        // use the real resampler here to ensure we have a stable seed even for complex queries
        Resampler resampler = new Resampler(request, sampleRate, actualTotalSamples);

        assertEquals(200, resampler.adjustSampleCount(actualSamplesSingleTrace));
    }

    public void testResamplingAndSampleRateAdjustment() {
        // corresponds to profiling-events-5pow01
        double sampleRate = 1.0d / Math.pow(5.0d, 1);
        int requestedSamples = 20_000;
        int actualTotalSamples = 40_000;

        GetStackTracesRequest request = new GetStackTracesRequest(
            requestedSamples,
            1.0d,
            1.0d,
            1.0d,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        request.setAdjustSampleCount(true);

        Resampler resampler = createResampler(request, sampleRate, actualTotalSamples);

        int actualSamplesSingleTrace = 20_000;
        assertEquals(100_000, resampler.adjustSampleCount(actualSamplesSingleTrace));
    }

    private static class DeterministicRandom implements RandomGenerator {
        private final double[] values;
        private int idx;

        private DeterministicRandom(double... values) {
            this.values = values;
            this.idx = 0;
        }

        public static RandomGenerator of(double... values) {
            return new DeterministicRandom(values);
        }

        @Override
        public long nextLong() {
            return Double.doubleToLongBits(nextDouble());
        }

        @Override
        public double nextDouble() {
            return values[idx++ % values.length];
        }
    }
}
