/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrdinalRangePresenceTests extends ESTestCase {

    private static final int GROUP = 4;

    public void testARunNeverReachesIntoAnIntervalThatCannotMatch() throws IOException {
        final Column column = randomColumn();
        final long low = between(1, 10);
        final long high = low + between(0, 4);

        final OrdinalRangePresence presence = column.presence(low, high);
        final List<Integer> actual = new ArrayList<>();
        for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS;) {
            final int runEnd = presence.docIDRunEnd();
            assertTrue("a run holds its first document", runEnd > doc);
            assertTrue("a run ends in the interval of its first document", runEnd <= column.maxDocs[column.intervalOf(doc)] + 1);
            for (int member = doc; member < runEnd; member++) {
                if (column.isPresent(member)) {
                    actual.add(member);
                }
            }
            doc = presence.advance(runEnd);
        }
        assertEquals(column.docsInIntervalsThatCanMatch(low, high), actual);
    }

    public void testNextDocVisitsEveryDocumentOfTheIntervalsThatCanMatch() throws IOException {
        final Column column = randomColumn();
        final long low = between(1, 10);
        final long high = low + between(0, 4);

        final OrdinalRangePresence presence = column.presence(low, high);
        final List<Integer> actual = new ArrayList<>();
        for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
            actual.add(doc);
        }
        assertEquals(column.docsInIntervalsThatCanMatch(low, high), actual);
    }

    public void testMixedForwardOperationsPreserveTheOriginalRank() throws IOException {
        final Column column = mixedColumn(randomBoolean() ? everyDoc(20) : randomSubset(20));
        final List<Integer> candidates = column.docsInIntervalsThatCanMatch(5, 5);
        final OrdinalRangePresence presence = column.presence(5, 5);
        assertEquals(-1, presence.docID());
        int current = -1;
        for (int step : new int[] { 2, 6, -1, 8, 14, -1, 16, 20 }) {
            final int target = step < 0 ? current + 1 : Math.max(step, current + 1);
            final int expected = candidates.stream().filter(doc -> doc >= target).findFirst().orElse(DocIdSetIterator.NO_MORE_DOCS);
            current = step < 0 ? presence.nextDoc() : presence.advance(target);
            assertEquals("step " + step, expected, current);
            if (current == DocIdSetIterator.NO_MORE_DOCS) {
                break;
            }
            assertEquals(Arrays.binarySearch(column.present, current), presence.rank());
            final int runEnd = presence.docIDRunEnd();
            assertEquals(runEnd, presence.docIDRunEnd());
            assertEquals(current, presence.docID());
            assertEquals(Arrays.binarySearch(column.present, current), presence.rank());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.advance(20));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.docID());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.docID());
    }

    public void testIntoBitSetCollectsOnlyTheCandidates() throws IOException {
        final Column column = mixedColumn(everyDoc(20));
        final List<Integer> candidates = column.docsInIntervalsThatCanMatch(5, 5);
        final OrdinalRangePresence presence = column.presence(5, 5);
        assertEquals(4, presence.advance(2));
        final FixedBitSet bits = new FixedBitSet(20);
        bits.set(0);
        for (int upTo : new int[] { 7, 14 }) {
            presence.intoBitSet(upTo, bits, 0);
            final FixedBitSet expected = new FixedBitSet(20);
            expected.set(0);
            candidates.stream().filter(doc -> doc < upTo).forEach(expected::set);
            assertEquals(expected, bits);
            assertEquals(
                candidates.stream().filter(doc -> doc >= upTo).findFirst().orElse(DocIdSetIterator.NO_MORE_DOCS).intValue(),
                presence.docID()
            );
        }
    }

    public void testAnExcludedParentIsSkippedInOneJump() throws IOException {
        final Column column = new Column(
            new int[] { 3, 7, 11, 15, 19 },
            new long[] { 1, 1, 1, 1, 5 },
            new long[] { 1, 1, 1, 1, 5 },
            everyDoc(20)
        );
        final Skipper skipper = new Skipper(column);
        final OrdinalRangePresence presence = new OrdinalRangePresence(new Presence(column.present), skipper, 5, 5);
        assertEquals(16, presence.nextDoc());
        assertEquals(2, skipper.advances);
    }

    public void testDocumentsBeyondTheLastIntervalAreKept() throws IOException {
        final int[] maxDocs = { 99 };
        final Column column = new Column(maxDocs, new long[] { 1 }, new long[] { 10 }, everyDoc(200));
        final OrdinalRangePresence presence = column.presence(20, 30);
        assertEquals(100, presence.nextDoc());
        assertEquals(200, presence.docIDRunEnd());
        assertEquals(150, presence.advance(150));
    }

    private static Column mixedColumn(int[] present) {
        return new Column(new int[] { 3, 7, 11, 15, 19 }, new long[] { 2, 5, 1, 5, 0 }, new long[] { 2, 5, 1, 6, 0 }, present);
    }

    private static int[] randomSubset(int count) {
        return Arrays.stream(everyDoc(count)).filter(doc -> randomInt(2) != 0).toArray();
    }

    private static int[] everyDoc(int count) {
        final int[] docs = new int[count];
        Arrays.setAll(docs, i -> i);
        return docs;
    }

    private static Column randomColumn() {
        final int intervals = between(1, 40);
        final int[] maxDocs = new int[intervals];
        final long[] mins = new long[intervals];
        final long[] maxs = new long[intervals];
        int doc = -1;
        for (int i = 0; i < intervals; i++) {
            doc += between(1, 300);
            maxDocs[i] = doc;
            final long a = between(0, 14);
            final long b = between(0, 14);
            mins[i] = Math.min(a, b);
            maxs[i] = Math.max(a, b);
        }
        final int maxDoc = maxDocs[intervals - 1] + 1;
        final List<Integer> present = new ArrayList<>();
        for (int d = 0; d < maxDoc; d++) {
            if (randomInt(5) != 0) {
                present.add(d);
            }
        }
        return new Column(maxDocs, mins, maxs, present.stream().mapToInt(Integer::intValue).toArray());
    }

    private static final class Column {
        final int[] maxDocs;
        final long[] mins;
        final long[] maxs;
        final int[] present;
        final int maxDoc;

        Column(int[] maxDocs, long[] mins, long[] maxs, int[] present) {
            this.maxDocs = maxDocs;
            this.mins = mins;
            this.maxs = maxs;
            this.present = present;
            this.maxDoc = maxDocs[maxDocs.length - 1] + 1;
        }

        OrdinalRangePresence presence(long low, long high) {
            return new OrdinalRangePresence(new Presence(present), new Skipper(this), low, high);
        }

        boolean isPresent(int doc) {
            return Arrays.binarySearch(present, doc) >= 0;
        }

        List<Integer> docsInIntervalsThatCanMatch(long low, long high) {
            final List<Integer> docs = new ArrayList<>();
            for (int doc : present) {
                final int interval = intervalOf(doc);
                if (maxs[interval] >= low && mins[interval] <= high) {
                    docs.add(doc);
                }
            }
            return docs;
        }

        int intervalOf(int doc) {
            final int interval = intervalOfOrEnd(doc);
            assertTrue("no interval holds " + doc, interval < maxDocs.length);
            return interval;
        }

        int intervalOfOrEnd(int doc) {
            for (int i = 0; i < maxDocs.length; i++) {
                if (maxDocs[i] >= doc) {
                    return i;
                }
            }
            return maxDocs.length;
        }
    }

    private static final class Presence extends ColumnIterator {
        private final int[] docs;
        private int at = -1;

        Presence(int[] docs) {
            this.docs = docs;
        }

        @Override
        public int rank() {
            return at;
        }

        @Override
        public boolean advanceExact(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            return at < 0 ? -1 : at >= docs.length ? NO_MORE_DOCS : docs[at];
        }

        @Override
        public int nextDoc() {
            at = Math.min(at + 1, docs.length);
            return docID();
        }

        @Override
        public int advance(int target) {
            while (at < docs.length && (at < 0 || docs[at] < target)) {
                at++;
            }
            return docID();
        }

        @Override
        public int docIDRunEnd() {
            int end = at;
            while (end + 1 < docs.length && docs[end + 1] == docs[end] + 1) {
                end++;
            }
            return docs[end] + 1;
        }

        @Override
        public long cost() {
            return docs.length;
        }
    }

    private static final class Skipper extends DocValuesSkipper {
        private final Column column;
        private int interval = -1;
        private int advances;

        Skipper(Column column) {
            this.column = column;
        }

        @Override
        public void advance(int target) {
            advances++;
            assertTrue("target must be beyond the current interval", interval < 0 || target > column.maxDocs[interval]);
            interval = column.intervalOfOrEnd(target);
        }

        @Override
        public int numLevels() {
            return 2;
        }

        private int groupStart() {
            return interval / GROUP * GROUP;
        }

        private int groupEnd() {
            return Math.min(groupStart() + GROUP, column.maxDocs.length) - 1;
        }

        @Override
        public int minDocID(int level) {
            return interval < 0 || interval >= column.maxDocs.length ? DocIdSetIterator.NO_MORE_DOCS : 0;
        }

        @Override
        public int maxDocID(int level) {
            if (interval < 0) {
                return -1;
            }
            if (interval >= column.maxDocs.length) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return column.maxDocs[level == 0 ? interval : groupEnd()];
        }

        @Override
        public long minValue(int level) {
            if (level == 0) {
                return column.mins[interval];
            }
            long min = Long.MAX_VALUE;
            for (int i = groupStart(); i <= groupEnd(); i++) {
                min = Math.min(min, column.mins[i]);
            }
            return min;
        }

        @Override
        public long maxValue(int level) {
            if (level == 0) {
                return column.maxs[interval];
            }
            long max = Long.MIN_VALUE;
            for (int i = groupStart(); i <= groupEnd(); i++) {
                max = Math.max(max, column.maxs[i]);
            }
            return max;
        }

        @Override
        public int docCount(int level) {
            return 1;
        }

        @Override
        public long minValue() {
            return 0;
        }

        @Override
        public long maxValue() {
            return Long.MAX_VALUE;
        }

        @Override
        public int docCount() {
            return column.maxDoc;
        }
    }
}
