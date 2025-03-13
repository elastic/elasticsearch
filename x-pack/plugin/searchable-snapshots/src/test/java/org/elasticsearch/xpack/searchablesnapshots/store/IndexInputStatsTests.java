/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.elasticsearch.test.ESTestCase;

import java.util.function.LongSupplier;

import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.assertCounter;
import static org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats.SEEKING_THRESHOLD;

public class IndexInputStatsTests extends ESTestCase {

    private static final LongSupplier FAKE_CLOCK = () -> {
        assert false : "should not be called";
        return -1L;
    };

    public void testReads() {
        final long fileLength = randomLongBetween(1L, 1_000L);
        final IndexInputStats inputStats = new IndexInputStats(1, fileLength, fileLength, fileLength, FAKE_CLOCK);

        assertCounter(inputStats.getContiguousReads(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getNonContiguousReads(), 0L, 0L, 0L, 0L);

        final IndexInputStats.Counter contiguous = new IndexInputStats.Counter();
        final IndexInputStats.Counter nonContiguous = new IndexInputStats.Counter();

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            final long currentPosition = randomLongBetween(0L, inputStats.getTotalSize() - 1L);
            final long previousPosition = randomBoolean() ? currentPosition : randomLongBetween(0L, inputStats.getTotalSize() - 1L);
            final int bytesRead = randomIntBetween(1, toIntBytes(Math.max(1L, inputStats.getTotalSize() - currentPosition)));

            inputStats.incrementBytesRead(previousPosition, currentPosition, bytesRead);

            if (previousPosition == currentPosition) {
                contiguous.add(bytesRead);
            } else {
                nonContiguous.add(bytesRead);
            }
        }

        assertCounter(inputStats.getContiguousReads(), contiguous.total(), contiguous.count(), contiguous.min(), contiguous.max());
        assertCounter(
            inputStats.getNonContiguousReads(),
            nonContiguous.total(),
            nonContiguous.count(),
            nonContiguous.min(),
            nonContiguous.max()
        );
    }

    public void testSeeks() {
        final long fileLength = randomLongBetween(1L, 1_000L);
        final long seekingThreshold = randomBoolean() ? randomLongBetween(1L, fileLength) : SEEKING_THRESHOLD.getBytes();
        final IndexInputStats inputStats = new IndexInputStats(1, fileLength, fileLength, fileLength, seekingThreshold, FAKE_CLOCK);

        assertCounter(inputStats.getForwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getForwardLargeSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardLargeSeeks(), 0L, 0L, 0L, 0L);

        final IndexInputStats.Counter fwSmallSeeks = new IndexInputStats.Counter();
        final IndexInputStats.Counter fwLargeSeeks = new IndexInputStats.Counter();
        final IndexInputStats.Counter bwSmallSeeks = new IndexInputStats.Counter();
        final IndexInputStats.Counter bwLargeSeeks = new IndexInputStats.Counter();

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            final long currentPosition = randomLongBetween(0L, fileLength);
            final long seekToPosition = randomLongBetween(0L, fileLength);
            inputStats.incrementSeeks(currentPosition, seekToPosition);

            final long delta = seekToPosition - currentPosition;
            if (delta > 0) {
                IndexInputStats.Counter forwardCounter = (delta <= seekingThreshold) ? fwSmallSeeks : fwLargeSeeks;
                forwardCounter.add(delta);
            } else if (delta < 0) {
                IndexInputStats.Counter backwardCounter = (delta >= -1 * seekingThreshold) ? bwSmallSeeks : bwLargeSeeks;
                backwardCounter.add(-delta);
            }
        }

        assertCounter(
            inputStats.getForwardSmallSeeks(),
            fwSmallSeeks.total(),
            fwSmallSeeks.count(),
            fwSmallSeeks.min(),
            fwSmallSeeks.max()
        );
        assertCounter(
            inputStats.getForwardLargeSeeks(),
            fwLargeSeeks.total(),
            fwLargeSeeks.count(),
            fwLargeSeeks.min(),
            fwLargeSeeks.max()
        );

        assertCounter(
            inputStats.getBackwardSmallSeeks(),
            bwSmallSeeks.total(),
            bwSmallSeeks.count(),
            bwSmallSeeks.min(),
            bwSmallSeeks.max()
        );
        assertCounter(
            inputStats.getBackwardLargeSeeks(),
            bwLargeSeeks.total(),
            bwLargeSeeks.count(),
            bwLargeSeeks.min(),
            bwLargeSeeks.max()
        );
    }

    public void testSeekToSamePosition() {
        final IndexInputStats inputStats = new IndexInputStats(1, 123L, 123L, 123L, FAKE_CLOCK);
        final long position = randomLongBetween(0L, inputStats.getTotalSize());

        inputStats.incrementSeeks(position, position);

        assertCounter(inputStats.getForwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getForwardLargeSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardLargeSeeks(), 0L, 0L, 0L, 0L);
    }
}
