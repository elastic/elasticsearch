/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BreakingExponentialHistogramHolderTests extends ESTestCase {

    public void testSetFromCompressedHistogramCopiesData() throws IOException {
        ExponentialHistogram original = BlockTestUtils.randomExponentialHistogram();
        ExponentialHistogram other = BlockTestUtils.randomExponentialHistogram();

        ExponentialHistogramArrayBlock.EncodedHistogramData encoded = ExponentialHistogramArrayBlock.encode(original);
        CompressedExponentialHistogram source = new CompressedExponentialHistogram();
        source.reset(
            original.zeroBucket().zeroThreshold(),
            original.valueCount(),
            original.sum(),
            original.min(),
            original.max(),
            encoded.encodedHistogram()
        );

        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        try (BreakingExponentialHistogramHolder holder = BreakingExponentialHistogramHolder.create(breaker)) {
            holder.set(source);
            assertThat(holder.accessor(), equalTo(original));

            // Resetting the source to different data should not alter the holder's copy
            ExponentialHistogramArrayBlock.EncodedHistogramData otherEncoded = ExponentialHistogramArrayBlock.encode(other);
            source.reset(
                other.zeroBucket().zeroThreshold(),
                other.valueCount(),
                other.sum(),
                other.min(),
                other.max(),
                otherEncoded.encodedHistogram()
            );

            assertThat(holder.accessor(), equalTo(original));
        }
    }

    public void testSetFromEmptyHistogram() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        try (BreakingExponentialHistogramHolder holder = BreakingExponentialHistogramHolder.create(breaker)) {
            holder.set(ExponentialHistogram.empty());
            ExponentialHistogram result = holder.accessor();
            assertThat(result.valueCount(), equalTo(0L));
            assertThat(result.sum(), equalTo(0.0));
            assertTrue(Double.isNaN(result.min()));
            assertTrue(Double.isNaN(result.max()));
        }
    }

    public void testSetOverwritesPreviousValue() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        try (BreakingExponentialHistogramHolder holder = BreakingExponentialHistogramHolder.create(breaker)) {
            ExponentialHistogram first = BlockTestUtils.randomExponentialHistogram();
            ExponentialHistogram second = BlockTestUtils.randomExponentialHistogram();
            holder.set(first);
            assertThat(holder.accessor(), equalTo(first));

            holder.set(second);
            assertThat(holder.accessor(), equalTo(second));
        }
    }

    public void testCrankyCircuitBreaker() {
        CircuitBreaker breaker = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        assertThrows(CircuitBreakingException.class, () -> {
            while (true) {
                try (BreakingExponentialHistogramHolder holder = BreakingExponentialHistogramHolder.create(breaker);) {
                    holder.set(BlockTestUtils.randomExponentialHistogram());
                }
            }
        });
        assertThat(breaker.getUsed(), equalTo(0L));
    }

}
