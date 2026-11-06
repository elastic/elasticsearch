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
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.codec.profiler.Timer;
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

    public void testObservingTheSkipsDoesNotChangeTheDocumentsVisited() throws IOException {
        final Column column = randomColumn();
        final long low = between(1, 10);
        final long high = low + between(0, 4);
        final List<Integer> observed = visitAll(column.presence(low, high, (from, to) -> {}));
        assertEquals(visitAll(column.presence(low, high)), observed);
        assertEquals(column.docsInIntervalsThatCanMatch(low, high), observed);
    }

    public void testEverySpanReportedHoldsOnlyDocumentsOfIntervalsThatCannotMatch() throws IOException {
        final Column column = randomColumn();
        final long low = between(1, 10);
        final long high = low + between(0, 4);
        final List<int[]> spans = new ArrayList<>();
        visitAll(column.presence(low, high, (from, to) -> spans.add(new int[] { from, to })));

        int previousEnd = 0;
        for (int[] span : spans) {
            assertTrue("a span is not empty", span[1] > span[0]);
            assertTrue("spans do not overlap and come in order", span[0] >= previousEnd);
            previousEnd = span[1];
        }
        final List<Integer> matching = column.docsInIntervalsThatCanMatch(low, high);
        for (int doc : column.present) {
            final boolean inSpan = spans.stream().anyMatch(span -> doc >= span[0] && doc < span[1]);
            assertEquals("document " + doc + " is skipped exactly when its interval cannot match", matching.contains(doc) == false, inSpan);
        }
    }

    public void testAColumnWhereEveryIntervalCanMatchReportsNoSpan() throws IOException {
        final Column column = new Column(new int[] { 9, 19, 29 }, new long[] { 1, 2, 3 }, new long[] { 5, 6, 7 }, everyDoc(30));
        final List<int[]> spans = new ArrayList<>();
        visitAll(column.presence(4, 4, (from, to) -> spans.add(new int[] { from, to })));
        assertTrue(spans.isEmpty());
    }

    public void testASkippedParentIsReportedOnceWithItsWholeSpan() throws IOException {
        final Column column = new Column(
            new int[] { 3, 7, 11, 15, 19 },
            new long[] { 1, 1, 1, 1, 5 },
            new long[] { 1, 1, 1, 1, 5 },
            everyDoc(20)
        );
        final List<int[]> spans = new ArrayList<>();
        assertEquals(16, column.presence(5, 5, (from, to) -> spans.add(new int[] { from, to })).nextDoc());
        assertEquals(1, spans.size());
        assertArrayEquals(new int[] { 0, 16 }, spans.get(0));
    }

    private static List<Integer> visitAll(OrdinalRangePresence presence) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    public void testNavigationTimeCoversTheSkipperAndTheAdvancesInsideTheMethodAndNothingElse() throws IOException {
        final Column column = new Column(
            new int[] { 3, 7, 11, 15, 19 },
            new long[] { 1, 1, 1, 1, 5 },
            new long[] { 1, 1, 1, 1, 5 },
            everyDoc(20)
        );
        final long[] now = { 11 };
        final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
        final Skipper skipper = new Skipper(column);
        skipper.onAdvance = () -> now[0] += 70;
        final Presence underlying = new Presence(column.present);
        underlying.onNext = () -> now[0] += 9;
        underlying.onAdvance = () -> now[0] += 30;
        final Timer timer = stats.timer(ProfiledIntervalSkipObserver.ORDINAL_SKIP_NAVIGATION_TIME);
        final OrdinalRangePresence presence = new OrdinalRangePresence(underlying, skipper, 5, 5, IntervalSkipObserver.NONE, timer);

        assertEquals(16, presence.nextDoc());

        assertEquals(
            "two skipper advances and one jump of the underlying iterator, not its nextDoc",
            170L,
            (long) TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.ordinal_skip.navigation_time_nanos")
        );
    }

    public void testNavigationIsNotTimedWhileTheDocumentStaysInTheCachedInterval() throws IOException {
        final Column column = new Column(new int[] { 99 }, new long[] { 1 }, new long[] { 10 }, everyDoc(100));
        final long[] now = { 0 };
        final long[] reads = { 0 };
        final ProfileCollector stats = TestCollectors.withClock(() -> {
            reads[0]++;
            return now[0];
        });
        final Timer timer = stats.timer(ProfiledIntervalSkipObserver.ORDINAL_SKIP_NAVIGATION_TIME);
        final OrdinalRangePresence presence = column.presence(5, 5, IntervalSkipObserver.NONE, timer);
        presence.nextDoc();
        final long readsAfterTheFirstNavigation = reads[0];
        for (int doc = 1; doc < 100; doc++) {
            assertEquals(doc, presence.nextDoc());
        }
        assertEquals("the other 99 documents were inside the interval the first call cached", readsAfterTheFirstNavigation, reads[0]);
    }

    public void testATimedPresenceFindsTheSameDocumentsAsAnUntimedOne() throws IOException {
        final Column column = randomColumn();
        final long low = between(1, 10);
        final long high = low + between(0, 4);
        final ProfileCollector stats = TestCollectors.withClock(new java.util.concurrent.atomic.AtomicLong()::incrementAndGet);
        final Timer timer = stats.timer(ProfiledIntervalSkipObserver.ORDINAL_SKIP_NAVIGATION_TIME);
        final List<Integer> timed = new ArrayList<>();
        final OrdinalRangePresence presence = column.presence(low, high, IntervalSkipObserver.NONE, timer);
        for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
            timed.add(doc);
        }
        assertEquals(visitAll(column.presence(low, high)), timed);
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
        final OrdinalRangePresence presence = new OrdinalRangePresence(
            new Presence(column.present),
            skipper,
            5,
            5,
            IntervalSkipObserver.NONE,
            Timer.INERT
        );
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
            return presence(low, high, IntervalSkipObserver.NONE);
        }

        OrdinalRangePresence presence(long low, long high, IntervalSkipObserver observer) {
            return presence(low, high, observer, Timer.INERT);
        }

        OrdinalRangePresence presence(long low, long high, IntervalSkipObserver observer, Timer navigationTimer) {
            return new OrdinalRangePresence(new Presence(present), new Skipper(this), low, high, observer, navigationTimer);
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
        Runnable onNext = () -> {};
        Runnable onAdvance = () -> {};

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
            onNext.run();
            at = Math.min(at + 1, docs.length);
            return docID();
        }

        @Override
        public int advance(int target) {
            onAdvance.run();
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
        Runnable onAdvance = () -> {};

        Skipper(Column column) {
            this.column = column;
        }

        @Override
        public void advance(int target) {
            onAdvance.run();
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
