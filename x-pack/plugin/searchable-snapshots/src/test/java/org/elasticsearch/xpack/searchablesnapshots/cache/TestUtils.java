/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Random;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomPicks.randomFrom;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

final class TestUtils {
    private TestUtils() {
    }

    static CacheService createCacheService(final Random random) {
        final ByteSizeValue cacheSize = new ByteSizeValue(randomIntBetween(random, 1, 100),
            randomFrom(random, List.of(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB)));
        return new CacheService(cacheSize, randomCacheRangeSize(random));
    }

    static ByteSizeValue randomCacheRangeSize(final Random random) {
        return new ByteSizeValue(randomIntBetween(random, 1, 100),
            randomFrom(random, List.of(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB)));
    }

    static long numberOfRanges(long fileSize, long rangeSize) {
        return numberOfRanges(Math.toIntExact(fileSize), Math.toIntExact(rangeSize));
    }

    static long numberOfRanges(int fileSize, int rangeSize) {
        long numberOfRanges = fileSize / rangeSize;
        if (fileSize % rangeSize > 0) {
            numberOfRanges++;
        }
        if (numberOfRanges == 0) {
            numberOfRanges++;
        }
        return numberOfRanges;
    }

    static void assertCounter(IndexInputStats.Counter counter, long total, long count, long min, long max) {
        assertThat(counter.total(), equalTo(total));
        assertThat(counter.count(), equalTo(count));
        assertThat(counter.min(), equalTo(min));
        assertThat(counter.max(), equalTo(max));
    }
}
