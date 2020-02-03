/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.searchablesnapshots.cache.IndexInputStats.SEEKING_THRESHOLD;
import static org.elasticsearch.xpack.searchablesnapshots.cache.TestUtils.assertCounter;

public class IndexInputStatsTests extends ESTestCase {

    public void testReads() {
        final long fileLength = randomLongBetween(1L, 1_000L);
        final IndexInputStats inputStats = new IndexInputStats(fileLength);

        assertCounter(inputStats.getContiguousReads(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getNonContiguousReads(), 0L, 0L, 0L, 0L);

        final IndexInputStats.Counter contiguous = new IndexInputStats.Counter();
        final IndexInputStats.Counter nonContiguous = new IndexInputStats.Counter();

        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            final long currentPosition = randomLongBetween(0L, inputStats.getFileLength() - 1L);
            final long previousPosition = randomBoolean() ? currentPosition : randomLongBetween(0L, inputStats.getFileLength() - 1L);
            final int bytesRead = randomIntBetween(1, Math.toIntExact(Math.max(1L, inputStats.getFileLength() - currentPosition)));

            inputStats.incrementBytesRead(previousPosition, currentPosition, bytesRead);

            if (previousPosition == currentPosition) {
                contiguous.add(bytesRead);
            } else {
                nonContiguous.add(bytesRead);
            }
        }

        assertCounter(inputStats.getContiguousReads(),
            contiguous.total(), contiguous.count(), contiguous.min(), contiguous.max());
        assertCounter(inputStats.getNonContiguousReads(),
            nonContiguous.total(), nonContiguous.count(), nonContiguous.min(), nonContiguous.max());
    }

    public void testSeeks() {
        final long fileLength = randomLongBetween(1L, 1_000L);
        final IndexInputStats inputStats = new IndexInputStats(fileLength);

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
                IndexInputStats.Counter forwardCounter = (delta <= SEEKING_THRESHOLD.getBytes()) ? fwSmallSeeks : fwLargeSeeks;
                forwardCounter.add(delta);
            } else if (delta < 0) {
                IndexInputStats.Counter backwardCounter = (delta >= -1 * SEEKING_THRESHOLD.getBytes()) ? bwSmallSeeks : bwLargeSeeks;
                backwardCounter.add(delta);
            }
        }

        assertCounter(inputStats.getForwardSmallSeeks(),
            fwSmallSeeks.total(), fwSmallSeeks.count(), fwSmallSeeks.min(), fwSmallSeeks.max());
        assertCounter(inputStats.getForwardLargeSeeks(),
            fwLargeSeeks.total(), fwLargeSeeks.count(), fwLargeSeeks.min(), fwLargeSeeks.max());

        assertCounter(inputStats.getBackwardSmallSeeks(),
            bwSmallSeeks.total(), bwSmallSeeks.count(), bwSmallSeeks.min(), bwSmallSeeks.max());
        assertCounter(inputStats.getBackwardLargeSeeks(),
            bwLargeSeeks.total(), bwLargeSeeks.count(), bwLargeSeeks.min(), bwLargeSeeks.max());
    }

    public void testSeekToSamePosition() {
        final IndexInputStats inputStats = new IndexInputStats(randomLongBetween(1L, 1_000L));
        final long position = randomLongBetween(0L, inputStats.getFileLength());

        inputStats.incrementSeeks(position, position);

        assertCounter(inputStats.getForwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getForwardLargeSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardSmallSeeks(), 0L, 0L, 0L, 0L);
        assertCounter(inputStats.getBackwardLargeSeeks(), 0L, 0L, 0L, 0L);
    }
}
