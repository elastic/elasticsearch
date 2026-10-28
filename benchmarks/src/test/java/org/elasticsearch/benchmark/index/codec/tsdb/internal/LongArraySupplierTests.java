/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LongArraySupplierTests extends ESTestCase {

    public void testTimestampLikeSupplierProducesIncreasingValues() {
        final long[] data = TimestampLikeSupplier.builder(randomInt(), between(64, 256)).build().get();

        for (int i = 1; i < data.length; i++) {
            assertTrue(data[i] > data[i - 1]);
        }
    }

    public void testTimestampLikeSupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        assertArrayEquals(TimestampLikeSupplier.builder(seed, size).build().get(), TimestampLikeSupplier.builder(seed, size).build().get());
    }

    public void testTimestampLikeSupplierWithJitterProbability() {
        final int size = between(64, 256);
        final long[] data = TimestampLikeSupplier.builder(randomInt(), size)
            .withBaseDelta(randomLongBetween(100, 10000))
            .withJitterRatio(randomDoubleBetween(0.01, 0.5, true))
            .withJitterProbability(randomDoubleBetween(0.0, 1.0, true))
            .build()
            .get();

        assertEquals(size, data.length);
        for (int i = 1; i < data.length; i++) {
            assertTrue(data[i] > data[i - 1]);
        }
    }

    public void testGaugeLikeSupplierWithDefaultValues() {
        final long[] data = GaugeLikeSupplier.builder(randomInt(), between(64, 256)).build().get();

        for (final long value : data) {
            assertTrue(value >= 0);
        }
    }

    public void testGaugeLikeSupplierProducesRandomWalk() {
        final long[] data = GaugeLikeSupplier.builder(randomInt(), between(64, 256))
            .withVarianceRatio(randomDoubleBetween(0.01, 0.5, true))
            .build()
            .get();

        for (final long value : data) {
            assertTrue(value >= 0);
        }
    }

    public void testGaugeLikeSupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        final double varianceRatio = randomDoubleBetween(0.01, 0.5, true);
        assertArrayEquals(
            GaugeLikeSupplier.builder(seed, size).withVarianceRatio(varianceRatio).build().get(),
            GaugeLikeSupplier.builder(seed, size).withVarianceRatio(varianceRatio).build().get()
        );
    }

    public void testCounterWithResetsSupplierProducesMonotonicWithResets() {
        final long[] data = CounterWithResetsSupplier.builder(randomInt(), between(64, 256))
            .withResetProbability(randomDoubleBetween(0.01, 0.1, true))
            .build()
            .get();

        for (final long value : data) {
            assertTrue(value >= 0);
        }
    }

    public void testCounterWithResetsSupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        final double resetProbability = randomDoubleBetween(0.01, 0.1, true);
        assertArrayEquals(
            CounterWithResetsSupplier.builder(seed, size).withResetProbability(resetProbability).build().get(),
            CounterWithResetsSupplier.builder(seed, size).withResetProbability(resetProbability).build().get()
        );
    }

    public void testCounterWithResetsSupplierWithDefaultValues() {
        final long[] data = CounterWithResetsSupplier.builder(randomInt(), between(64, 256)).build().get();

        for (final long value : data) {
            assertTrue(value >= 0);
        }
    }

    public void testNearConstantSupplierProducesMostlyConstantValues() {
        final int size = between(128, 512);
        final double outlierProbability = randomDoubleBetween(0.01, 0.1, true);
        final long[] data = NearConstantWithOutliersSupplier.builder(randomInt(), size)
            .withOutlierProbability(outlierProbability)
            .build()
            .get();

        assertEquals(size, data.length);
        final long constantCount = Arrays.stream(data).filter(v -> v == 0L).count();
        assertTrue(constantCount > size * (1.0 - outlierProbability - 0.1));
    }

    public void testNearConstantSupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        assertArrayEquals(
            NearConstantWithOutliersSupplier.builder(seed, size).build().get(),
            NearConstantWithOutliersSupplier.builder(seed, size).build().get()
        );
    }

    public void testGcdFriendlySupplierValuesAreMultiplesOfGcd() {
        final long gcd = randomLongBetween(10, 1000);
        final long[] data = GcdFriendlySupplier.builder(randomInt(), between(64, 256)).withGcd(gcd).build().get();

        for (final long value : data) {
            assertEquals(0, value % gcd);
        }
    }

    public void testGcdFriendlySupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        assertArrayEquals(GcdFriendlySupplier.builder(seed, size).build().get(), GcdFriendlySupplier.builder(seed, size).build().get());
    }

    public void testLowCardinalitySupplierLimitedDistinctValues() {
        final int distinctValues = between(3, 10);
        final long[] data = LowCardinalitySupplier.builder(randomInt(), between(64, 256))
            .withDistinctValues(distinctValues)
            .withSkew(randomDoubleBetween(0.5, 3.0, true))
            .build()
            .get();

        assertTrue(uniqueCount(data) <= distinctValues);
    }

    public void testLowCardinalitySupplierGeneratesUniqueDistinctValues() {
        final int distinctValues = between(5, 15);
        final long[] data = LowCardinalitySupplier.builder(randomInt(), 10_000)
            .withDistinctValues(distinctValues)
            .withSkew(randomDoubleBetween(0.1, 1.0, true))
            .build()
            .get();

        assertEquals(distinctValues, uniqueCount(data));
    }

    public void testLowCardinalitySupplierDeterministic() {
        final int seed = randomInt();
        final int size = between(64, 256);
        assertArrayEquals(
            LowCardinalitySupplier.builder(seed, size).build().get(),
            LowCardinalitySupplier.builder(seed, size).build().get()
        );
    }

    public void testConstantIntegerSupplierProducesConstantValues() {
        final long[] data = new ConstantIntegerSupplier(randomInt(), between(8, 24), between(64, 256)).get();

        final long expected = data[0];
        for (final long value : data) {
            assertEquals(expected, value);
        }
    }

    public void testIncreasingIntegerSupplierProducesIncreasingValues() {
        final long[] data = new IncreasingIntegerSupplier(randomInt(), between(8, 24), between(64, 256)).get();

        for (int i = 1; i < data.length; i++) {
            assertTrue(data[i] >= data[i - 1]);
        }
    }

    public void testDecreasingIntegerSupplierProducesDecreasingValues() {
        final long[] data = new DecreasingIntegerSupplier(randomInt(), between(8, 24), between(64, 256)).get();

        for (int i = 1; i < data.length; i++) {
            assertTrue(data[i] <= data[i - 1]);
        }
    }

    public void testNonSortedIntegerSupplierBoundedValues() {
        final int bitsPerValue = 16;
        final long maxValue = 1L << bitsPerValue;
        final long[] data = new NonSortedIntegerSupplier(randomInt(), bitsPerValue, between(64, 256)).get();

        for (final long value : data) {
            assertTrue(value >= 0 && value < maxValue);
        }
    }

    public void testGaugeLikeSupplierBoundsValues() {
        final long maxValue = randomLongBetween(1000, 100_000);
        final long[] data = GaugeLikeSupplier.builder(randomInt(), between(64, 256)).withMaxValue(maxValue).build().get();

        for (final long value : data) {
            assertTrue(value >= 0 && value < maxValue);
        }
    }

    public void testCounterWithResetsSupplierBoundsValues() {
        final long maxValue = randomLongBetween(1000, 100_000);
        final long[] data = CounterWithResetsSupplier.builder(randomInt(), between(64, 256)).withMaxValue(maxValue).build().get();

        for (final long value : data) {
            assertTrue(value >= 0 && value < maxValue);
        }
    }

    public void testNearConstantWithOutliersSupplierBoundsValues() {
        final long baseValue = randomLongBetween(0, 100);
        final long maxOutlier = randomLongBetween(1000, 100_000);
        final long[] data = NearConstantWithOutliersSupplier.builder(randomInt(), between(64, 256))
            .withBaseValue(baseValue)
            .withMaxOutlier(maxOutlier)
            .build()
            .get();

        for (final long value : data) {
            assertTrue(value >= 0 && (value == baseValue || value < maxOutlier));
        }
    }

    public void testGaugeLikeSupplierNominalBitsPerValue() {
        assertEquals(14, GaugeLikeSupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(20, GaugeLikeSupplier.builder(17, 128).withMaxValue(1_000_000L).build().getNominalBitsPerValue());
    }

    public void testTimestampLikeSupplierNominalBitsPerValueIsRangeBased() {
        assertEquals(18, TimestampLikeSupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(
            21,
            TimestampLikeSupplier.builder(17, 128).withBaseDelta(10_000L).withJitterRatio(0.1).build().getNominalBitsPerValue()
        );
    }

    public void testCounterWithResetsSupplierNominalBitsPerValue() {
        assertEquals(24, CounterWithResetsSupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(10, CounterWithResetsSupplier.builder(17, 128).withMaxValue(1_000L).build().getNominalBitsPerValue());
    }

    public void testLowCardinalitySupplierNominalBitsPerValue() {
        assertEquals(14, LowCardinalitySupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(20, LowCardinalitySupplier.builder(17, 128).withMaxValue(1_000_000L).build().getNominalBitsPerValue());
    }

    public void testNearConstantWithOutliersSupplierNominalBitsPerValue() {
        assertEquals(24, NearConstantWithOutliersSupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(
            20,
            NearConstantWithOutliersSupplier.builder(17, 128)
                .withBaseValue(1_000_000L)
                .withMaxOutlier(100L)
                .build()
                .getNominalBitsPerValue()
        );
    }

    public void testGcdFriendlySupplierNominalBitsPerValue() {
        assertEquals(24, GcdFriendlySupplier.builder(17, 128).build().getNominalBitsPerValue());
        assertEquals(17, GcdFriendlySupplier.builder(17, 128).withGcd(100L).withMaxMultiplier(1000L).build().getNominalBitsPerValue());
    }

    private static int uniqueCount(long[] data) {
        return Arrays.stream(data).boxed().collect(Collectors.toSet()).size();
    }
}
