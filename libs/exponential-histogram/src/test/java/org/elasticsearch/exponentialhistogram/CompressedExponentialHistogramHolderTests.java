/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils.randomHistogram;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class CompressedExponentialHistogramHolderTests extends ExponentialHistogramTestCase {

    public void testSetFromCompressedHistogramCopiesData() throws IOException {
        ReleasableExponentialHistogram original = randomHistogramWithDoubleZeroThreshold();
        ReleasableExponentialHistogram other = randomHistogramWithDoubleZeroThreshold();

        CompressedExponentialHistogram source = toCompressedHistogram(original);
        ExponentialHistogramCircuitBreaker ehBreaker = breaker();

        try (CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(ehBreaker)) {
            holder.set(source);
            assertThat(holder.accessor(), equalTo(original));
            // Resetting the source to different data should not alter the holder's copy
            source.reset(
                other.zeroBucket().zeroThreshold(),
                other.valueCount(),
                other.sum(),
                other.min(),
                other.max(),
                new BytesRef(encodeHistogramBytes(other))
            );

            assertThat(holder.accessor(), equalTo(original));
        }
    }

    public void testSetFromEmptyHistogram() {
        ExponentialHistogramCircuitBreaker ehBreaker = breaker();
        try (CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(ehBreaker)) {
            holder.set(ExponentialHistogram.empty());
            ExponentialHistogram result = holder.accessor();
            assertThat(result.valueCount(), equalTo(0L));
            assertThat(result.sum(), equalTo(0.0));
            assertTrue(Double.isNaN(result.min()));
            assertTrue(Double.isNaN(result.max()));
        }
    }

    public void testSetOverwritesPreviousValue() {
        ExponentialHistogramCircuitBreaker ehBreaker = breaker();
        try (CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(ehBreaker)) {
            ReleasableExponentialHistogram first = randomHistogramWithDoubleZeroThreshold();
            ReleasableExponentialHistogram second = randomHistogramWithDoubleZeroThreshold();
            holder.set(first);
            assertThat(holder.accessor(), equalTo(first));

            holder.set(second);
            assertThat(holder.accessor(), equalTo(second));
        }
    }

    public void testCrankyCircuitBreaker() {
        CrankyCircuitBreakerService crankyBreakerService = new CrankyCircuitBreakerService();
        CircuitBreaker esBreaker = crankyBreakerService.getBreaker(CircuitBreaker.REQUEST);
        ExponentialHistogramCircuitBreaker ehBreaker = breaker(esBreaker);
        try {
            while (true) {
                try (CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(ehBreaker)) {
                    try (ReleasableExponentialHistogram hist = randomHistogram(ehBreaker)) {
                        holder.set(hist);
                    }
                }
            }
        } catch (CircuitBreakingException e) {
            // expected
        }
        assertThat(esBreaker.getUsed(), equalTo(0L));
    }

    public void testLargeHistogramTracksMemory() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        ExponentialHistogramCircuitBreaker ehBreaker = breaker(esBreaker);

        ExponentialHistogramBuilder largeHistoBuilder = ExponentialHistogram.builder(0, ExponentialHistogramCircuitBreaker.noop());
        for (int i = 0; i < 1000; i++) {
            largeHistoBuilder.setPositiveBucket(i, 1);
        }
        ExponentialHistogram largeHistogram = largeHistoBuilder.build();

        try (CompressedExponentialHistogramHolder holder = CompressedExponentialHistogramHolder.create(ehBreaker)) {
            assertThat(esBreaker.getUsed(), lessThan(1000L));
            holder.set(largeHistogram);
            assertThat(esBreaker.getUsed(), greaterThan(1000L));
        }

    }

    private ReleasableExponentialHistogram randomHistogramWithDoubleZeroThreshold() {
        ExponentialHistogram random = randomHistogram();
        ReleasableExponentialHistogram input = ExponentialHistogram.builder(random, breaker())
            .zeroBucket(ZeroBucket.create(random.zeroBucket().zeroThreshold(), random.zeroBucket().count()))
            .build();
        autoReleaseOnTestEnd(input);
        return input;
    }

    private static CompressedExponentialHistogram toCompressedHistogram(ExponentialHistogram input) throws IOException {
        byte[] encodedBytes = encodeHistogramBytes(input);
        CompressedExponentialHistogram decoded = new CompressedExponentialHistogram();
        decoded.reset(
            input.zeroBucket().zeroThreshold(),
            input.valueCount(),
            input.sum(),
            input.min(),
            input.max(),
            newBytesRef(encodedBytes)
        );
        return decoded;
    }

    private static byte[] encodeHistogramBytes(ExponentialHistogram input) throws IOException {
        ByteArrayOutputStream encodedStream = new ByteArrayOutputStream();
        CompressedExponentialHistogram.writeHistogramBytes(encodedStream, input);
        return encodedStream.toByteArray();
    }
}
