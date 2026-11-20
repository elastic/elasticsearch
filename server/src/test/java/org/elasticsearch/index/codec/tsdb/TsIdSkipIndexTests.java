/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TsIdSkipIndexTests extends ESTestCase {

    /**
     * Mirrors what the _tsid skipper of a time series segment holds: one document per entry, ordinals that never decrease, and
     * fixed size level 0 intervals that start where the previous one ended.
     */
    public void testIntervalForMatchesSkipperAdvance() throws IOException {
        final int intervalSize = randomFrom(1, 7, 64, 4096);
        final int maxDocs = randomIntBetween(1, 50_000);
        final int[] ords = new int[maxDocs];
        int ord = randomIntBetween(0, 3);
        for (int doc = 0; doc < maxDocs; doc++) {
            ords[doc] = ord;
            if (randomInt(9) == 0) {
                ord += randomIntBetween(1, 3);
            }
        }

        final TsIdSkipIndex index = TsIdSkipIndex.build(new FixedIntervalSkipper(ords, intervalSize), maxDocs);

        assertNotNull(index);
        for (int probe = -1; probe <= ords[maxDocs - 1] + 1; probe++) {
            final FixedIntervalSkipper reference = new FixedIntervalSkipper(ords, intervalSize);
            reference.advance(probe, Long.MAX_VALUE);
            final int interval = index.intervalFor(probe);
            if (probe < ords[0] || probe > ords[maxDocs - 1]) {
                assertThat("ordinal " + probe, interval, equalTo(-1));
                continue;
            }
            assertThat("ordinal " + probe, index.minDocID(interval), equalTo(reference.minDocID(0)));
            assertThat("ordinal " + probe, index.maxDocID(interval), equalTo(reference.maxDocID(0)));
            // The first document with an ordinal of at least the probe must be inside the interval.
            int firstDoc = 0;
            while (ords[firstDoc] < probe) {
                firstDoc++;
            }
            assertTrue(index.minDocID(interval) <= firstDoc && firstDoc <= index.maxDocID(interval));
        }
    }

    public void testBuildRejectsOrdinalsThatDecrease() throws IOException {
        final int[] ords = new int[100];
        for (int doc = 0; doc < ords.length; doc++) {
            ords[doc] = doc < 50 ? doc : 10;
        }
        assertThat(TsIdSkipIndex.build(new FixedIntervalSkipper(ords, 10), ords.length), nullValue());
    }

    /** A level 0 only skipper over an array of values, with the same {@code advance} contract as the real ones. */
    private static final class FixedIntervalSkipper extends DocValuesSkipper {
        private final int[] values;
        private final int intervalSize;
        private int minDocID = -1;
        private int maxDocID = -1;

        FixedIntervalSkipper(int[] values, int intervalSize) {
            this.values = values;
            this.intervalSize = intervalSize;
        }

        @Override
        public void advance(int target) {
            if (target >= values.length) {
                minDocID = maxDocID = DocIdSetIterator.NO_MORE_DOCS;
                return;
            }
            minDocID = target / intervalSize * intervalSize;
            maxDocID = Math.min(values.length, minDocID + intervalSize) - 1;
        }

        @Override
        public int numLevels() {
            return 1;
        }

        @Override
        public int minDocID(int level) {
            return minDocID;
        }

        @Override
        public int maxDocID(int level) {
            return maxDocID;
        }

        @Override
        public long minValue(int level) {
            return values[minDocID];
        }

        @Override
        public long maxValue(int level) {
            long max = Long.MIN_VALUE;
            for (int doc = minDocID; doc <= maxDocID; doc++) {
                max = Math.max(max, values[doc]);
            }
            return max;
        }

        @Override
        public int docCount(int level) {
            return maxDocID - minDocID + 1;
        }

        @Override
        public long minValue() {
            long min = Long.MAX_VALUE;
            for (int value : values) {
                min = Math.min(min, value);
            }
            return min;
        }

        @Override
        public long maxValue() {
            long max = Long.MIN_VALUE;
            for (int value : values) {
                max = Math.max(max, value);
            }
            return max;
        }

        @Override
        public int docCount() {
            return values.length;
        }
    }
}
