/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class PlainStringColumnSkipperTests extends ColumnarStringTestCase {

    private static final int INTERVAL = PlainSkipIndexCodec.DEFAULT_INTERVAL_SIZE;

    public void testPlainColumnRecordsItsRegionAndLastDocument() throws IOException {
        final int count = INTERVAL * between(20, 30) + between(0, INTERVAL - 1);
        final BytesRef[] docValues = ascendingRuns(count, between(2, 4));
        withColumn(docValues, (metadata, reader) -> {
            final StringColumnMetadata.Plain.Skipper skipper = ((StringColumnMetadata.Plain) metadata).skipper();
            assertNotNull(skipper);
            assertEquals(PlainSkipIndexCodec.ID, skipper.codecId());
            assertEquals(PlainSkipIndexCodec.DEFAULT_MAX_LEVEL, skipper.maxLevel());
            assertEquals(PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES, skipper.maxBoundBytes());
            assertTrue(skipper.boundsAreExact(metadata.maxLength()));
            assertEquals(count - 1, skipper.maxDocId());
            assertTrue(skipper.dataLength() > 0);
        });
    }

    public void testAValueLongerThanTheCapMakesTheColumnBoundsInexact() throws IOException {
        final int count = INTERVAL * between(20, 30);
        final BytesRef[] docValues = ascendingRuns(count, between(2, 4));
        final int longValueDoc = between(0, count - 1);
        docValues[longValueDoc] = new BytesRef(
            docValues[longValueDoc].utf8ToString() + "z".repeat(PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES)
        );
        withColumn(docValues, (metadata, reader) -> {
            final StringColumnMetadata.Plain.Skipper skipper = ((StringColumnMetadata.Plain) metadata).skipper();
            assertNotNull(skipper);
            assertTrue(metadata.maxLength() > skipper.maxBoundBytes());
            assertFalse(skipper.boundsAreExact(metadata.maxLength()));
        });
    }

    public void testColumnsWithoutLocalityWriteNoRegionAndStillAnswerRanges() throws IOException {
        final int count = INTERVAL * between(2, 6);
        final BytesRef[][] docSlots = new BytesRef[count][];
        final BytesRef[] values = new BytesRef[count];
        for (int doc = 0; doc < count; doc++) {
            values[doc] = new BytesRef(randomAlphaOfLength(between(1, 12)));
            docSlots[doc] = new BytesRef[] { values[doc] };
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (int query = 0; query < 10; query++) {
                final BytesRef lower = randomBound(values);
                final BytesRef upper = randomBound(values);
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                assertEquals(
                    expectedInRange(docSlots, lower, includeLower, upper, includeUpper),
                    matched(reader.matchRange(lower, includeLower, upper, includeUpper))
                );
            }
        });
    }

    public void testAColumnWrittenWithoutASkipIndexHasNoRegionAndStillAnswersRanges() throws IOException {
        final int count = INTERVAL * between(20, 30);
        final BytesRef[] clustered = ascendingRuns(count, between(2, 4));
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            docSlots[doc] = new BytesRef[] { clustered[doc] };
        }
        final StringColumnOptions options = StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE)
            .withoutSkipIndex();
        withColumn(docSlots, options, (metadata, reader) -> {
            assertNull(((StringColumnMetadata.Plain) metadata).skipper());
            for (int query = 0; query < 10; query++) {
                final BytesRef lower = randomBound(clustered);
                final BytesRef upper = randomBound(clustered);
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                assertEquals(
                    expectedInRange(docSlots, lower, includeLower, upper, includeUpper),
                    matched(reader.matchRange(lower, includeLower, upper, includeUpper))
                );
            }
        });
    }

    public void testASortedColumnWritesNoRegion() throws IOException {
        final int count = INTERVAL * between(2, 6);
        withColumn(ascendingRuns(count, 1), (metadata, reader) -> assertNull(((StringColumnMetadata.Plain) metadata).skipper()));
    }

    public void testRangeMatchesWhatAScanFinds() throws IOException {
        final int runs = between(2, 4);
        assertRangesMatchAScan(INTERVAL * runs * between(3, 4), runs, true);
    }

    public void testSmallColumnsAnswerRangesLikeAScanWhetherOrNotTheyKeepARegion() throws IOException {
        assertRangesMatchAScan(between(1, INTERVAL * 2), between(1, 4), false);
    }

    private void assertRangesMatchAScan(int count, int runs, boolean keepsARegion) throws IOException {
        final BytesRef[][] docSlots = new BytesRef[count][];
        final BytesRef[] clustered = ascendingRuns(count, runs);
        for (int doc = 0; doc < count; doc++) {
            if (rarely()) {
                continue;
            }
            final int slots = rarely() ? between(0, 3) : 1;
            docSlots[doc] = new BytesRef[slots];
            for (int slot = 0; slot < slots; slot++) {
                docSlots[doc][slot] = rarely() ? null : slot == 0 ? clustered[doc] : clustered[Math.min(count - 1, doc + between(0, 100))];
            }
        }
        withColumn(docSlots, (metadata, reader) -> {
            if (keepsARegion) {
                assertKeepsARegion(metadata);
            }
            for (int query = 0; query < 20; query++) {
                final BytesRef lower = randomBound(clustered);
                final BytesRef upper = randomBound(clustered);
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                assertEquals(
                    "range " + lower + " " + includeLower + " " + upper + " " + includeUpper,
                    expectedInRange(docSlots, lower, includeLower, upper, includeUpper),
                    matched(reader.matchRange(lower, includeLower, upper, includeUpper))
                );
            }
        });
    }

    public void testIntervalsOutsideTheRangeAreNotVisited() throws IOException {
        final int count = INTERVAL * between(20, 30);
        final BytesRef[] docValues = ascendingRuns(count, 2);
        final BytesRef lower = docValues[count / 20];
        final BytesRef upper = docValues[count / 10];
        withColumn(docValues, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            final DocIdSetIterator range = reader.matchRange(lower, true, upper, true);
            final int matches = matched(range).size();
            assertTrue("the range holds documents", matches > 0);
            final int candidates = candidates(reader.matchRange(lower, true, upper, true));
            assertTrue(
                "visited [" + candidates + "] of [" + count + "] for [" + matches + "] matches",
                candidates <= matches + 4 * INTERVAL
            );
        });
    }

    public void testGroupsOfIntervalsOutsideTheRangeAreSteppedOverTogether() throws IOException {
        final int groupIntervals = 1 << (PlainSkipIndexCodec.DEFAULT_LEVEL_SHIFT * 2);
        final int count = INTERVAL * groupIntervals * 3;
        final BytesRef[] docValues = ascending(count);
        final BytesRef first = docValues[0];
        docValues[0] = docValues[1];
        docValues[1] = first;
        final BytesRef lower = docValues[count - INTERVAL];
        final BytesRef upper = docValues[count - 1];
        withColumn(docValues, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            final int candidates = candidates(reader.matchRange(lower, true, upper, true));
            assertEquals(INTERVAL, candidates);
            final BytesRef[][] docSlots = singleValued(docValues);
            for (int query = 0; query < 10; query++) {
                final BytesRef from = randomBound(docValues);
                final BytesRef to = randomBound(docValues);
                assertEquals(expectedInRange(docSlots, from, true, to, true), matched(reader.matchRange(from, true, to, true)));
            }
            final List<Integer> expected = expectedInRange(docSlots, lower, true, upper, true);
            for (int attempt = 0; attempt < 10; attempt++) {
                final int target = between(0, count - 1);
                final int firstAtOrAfter = expected.stream().filter(doc -> doc >= target).findFirst().orElse(DocIdSetIterator.NO_MORE_DOCS);
                assertEquals("advance to " + target, firstAtOrAfter, reader.matchRange(lower, true, upper, true).advance(target));
            }
        });
    }

    public void testADocumentHoldingOnlyNullsNeverMatches() throws IOException {
        final int count = INTERVAL * 2 * between(3, 5);
        final BytesRef[] docValues = ascendingRuns(count, 2);
        final BytesRef[][] docSlots = singleValued(docValues);
        for (int doc = 0; doc < count; doc += between(500, 1500)) {
            docSlots[doc] = new BytesRef[] { null };
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            assertTrue(metadata.hasNullSlots());
            assertEquals(expectedInRange(docSlots, null, true, null, true), matched(reader.matchRange(null, true, null, true)));
            assertEquals(
                expectedInRange(docSlots, new BytesRef("term-"), true, new BytesRef("term-~"), true),
                matched(reader.matchRange(new BytesRef("term-"), true, new BytesRef("term-~"), true))
            );
        });
    }

    public void testIntoBitSetFillsTheDocumentsNextDocFinds() throws IOException {
        final int runs = between(2, 4);
        final int count = INTERVAL * runs * between(3, 4);
        final BytesRef[] docValues = ascendingRuns(count, runs);
        final BytesRef[][] docSlots = singleValued(docValues);
        withColumn(docValues, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            for (int query = 0; query < 5; query++) {
                final BytesRef lower = randomBound(docValues);
                final BytesRef upper = randomBound(docValues);
                final FixedBitSet bits = new FixedBitSet(count);
                final DocIdSetIterator range = reader.matchRange(lower, true, upper, true);
                int doc = range.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    range.intoBitSet(Math.min(count, doc + between(1, 3 * INTERVAL)), bits, 0);
                    doc = range.docID();
                }
                final List<Integer> found = new ArrayList<>();
                for (int set = bits.nextSetBit(0); set != DocIdSetIterator.NO_MORE_DOCS; set = set + 1 < count
                    ? bits.nextSetBit(set + 1)
                    : DocIdSetIterator.NO_MORE_DOCS) {
                    found.add(set);
                }
                assertEquals(expectedInRange(docSlots, lower, true, upper, true), found);
            }
        });
    }

    public void testADocumentHoldingNoSlotNeverMatchesAnIntervalInsideTheRange() throws IOException {
        final int count = INTERVAL * 2 * between(3, 4);
        final BytesRef[][] docSlots = singleValued(ascendingRuns(count, 2));
        for (int doc = 0; doc < count; doc += between(300, 1200)) {
            docSlots[doc] = new BytesRef[0];
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            assertFalse(metadata.hasNullSlots());
            for (int query = 0; query < 4; query++) {
                final BytesRef lower = query % 2 == 0 ? null : new BytesRef("term-");
                final BytesRef upper = query < 2 ? null : new BytesRef("term-~");
                assertEquals(expectedInRange(docSlots, lower, true, upper, true), matched(reader.matchRange(lower, true, upper, true)));
            }
            assertEquals(List.of(), expectedInRange(new BytesRef[][] { new BytesRef[0] }, null, true, null, true));
        });
    }

    public void testAColumnOfOnlyEmptyArraysMatchesNothing() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(1, INTERVAL * 2)][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[0];
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals(List.of(), matched(reader.matchRange(null, true, null, true)));
            assertEquals(List.of(), matched(reader.matchRange(new BytesRef("a"), true, new BytesRef("z"), true)));
        });
    }

    public void testRangesOverValuesLongerThanTheBoundCap() throws IOException {
        final int count = INTERVAL * 2 * between(3, 4);
        final String suffix = "p".repeat(PlainSkipIndexCodec.DEFAULT_MAX_BOUND_BYTES + between(0, 64));
        final BytesRef[] docValues = ascendingRuns(count, 2);
        for (int doc = 0; doc < count; doc++) {
            docValues[doc] = new BytesRef(docValues[doc].utf8ToString() + suffix);
        }
        final BytesRef[][] docSlots = singleValued(docValues);
        withColumn(docSlots, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            assertFalse(((StringColumnMetadata.Plain) metadata).skipper().boundsAreExact(metadata.maxLength()));
            for (int query = 0; query < 20; query++) {
                final BytesRef lower = randomBound(docValues);
                final BytesRef upper = randomBound(docValues);
                final boolean includeLower = randomBoolean();
                final boolean includeUpper = randomBoolean();
                assertEquals(
                    expectedInRange(docSlots, lower, includeLower, upper, includeUpper),
                    matched(reader.matchRange(lower, includeLower, upper, includeUpper))
                );
            }
        });
    }

    public void testTwoIteratorsOverOneReaderDoNotShareACursor() throws IOException {
        final int count = INTERVAL * 2 * between(3, 4);
        final BytesRef[] docValues = ascendingRuns(count, 2);
        final BytesRef[][] docSlots = singleValued(docValues);
        final BytesRef lowerA = docValues[count / 10];
        final BytesRef upperA = docValues[count / 5];
        final BytesRef lowerB = docValues[count / 3];
        final BytesRef upperB = docValues[count / 2 - 1];
        withColumn(docSlots, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            final DocIdSetIterator first = reader.matchRange(lowerA, true, upperA, true);
            final DocIdSetIterator second = reader.matchRange(lowerB, true, upperB, true);
            final List<Integer> foundFirst = new ArrayList<>();
            final List<Integer> foundSecond = new ArrayList<>();
            int a = first.nextDoc();
            int b = second.nextDoc();
            while (a != DocIdSetIterator.NO_MORE_DOCS || b != DocIdSetIterator.NO_MORE_DOCS) {
                if (a != DocIdSetIterator.NO_MORE_DOCS) {
                    foundFirst.add(a);
                    a = first.nextDoc();
                }
                if (b != DocIdSetIterator.NO_MORE_DOCS) {
                    foundSecond.add(b);
                    b = second.advance(b + between(1, INTERVAL));
                }
            }
            assertEquals(expectedInRange(docSlots, lowerA, true, upperA, true), foundFirst);
            for (int doc : foundSecond) {
                assertTrue(expectedInRange(docSlots, lowerB, true, upperB, true).contains(doc));
            }
        });
    }

    public void testAdvanceLandsOnTheNextDocumentTheRangeCouldHold() throws IOException {
        final int count = INTERVAL * 2 * between(4, 6);
        final BytesRef[] docValues = ascendingRuns(count, 2);
        final BytesRef lower = docValues[count / 8];
        final BytesRef upper = docValues[count / 4];
        final BytesRef[][] docSlots = singleValued(docValues);
        withColumn(docValues, (metadata, reader) -> {
            assertKeepsARegion(metadata);
            final List<Integer> expected = expectedInRange(docSlots, lower, true, upper, true);
            final DocIdSetIterator range = reader.matchRange(lower, true, upper, true);
            final List<Integer> advanced = new ArrayList<>();
            int target = 0;
            while (target < count) {
                final int doc = range.advance(target);
                if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                assertTrue(doc >= target);
                advanced.add(doc);
                target = doc + between(1, INTERVAL);
            }
            for (int doc : advanced) {
                assertTrue("advance returned [" + doc + "] outside the range", expected.contains(doc));
            }
        });
    }

    private static void assertKeepsARegion(StringColumnMetadata metadata) {
        assertNotNull(
            "the column has to keep a region for this test to reach the skipper",
            ((StringColumnMetadata.Plain) metadata).skipper()
        );
    }

    private static BytesRef[] ascending(int count) {
        return ascendingRuns(count, 1);
    }

    private static BytesRef[] ascendingRuns(int count, int runs) {
        final BytesRef[] values = new BytesRef[count];
        final int perRun = (count + runs - 1) / runs;
        for (int doc = 0; doc < count; doc++) {
            values[doc] = new BytesRef(String.format(Locale.ROOT, "term-%08d", doc % perRun));
        }
        return values;
    }

    private static BytesRef randomBound(BytesRef[] values) {
        if (rarely()) {
            return null;
        }
        return rarely() ? new BytesRef(randomAlphaOfLength(between(0, 6))) : values[between(0, values.length - 1)];
    }

    private static List<Integer> expectedInRange(
        BytesRef[][] docSlots,
        BytesRef lower,
        boolean includeLower,
        BytesRef upper,
        boolean includeUpper
    ) {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (docSlots[doc] == null) {
                continue;
            }
            for (BytesRef value : docSlots[doc]) {
                if (value != null && inRange(value, lower, includeLower, upper, includeUpper)) {
                    docs.add(doc);
                    break;
                }
            }
        }
        return docs;
    }

    private static boolean inRange(BytesRef value, BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        if (lower != null) {
            final int cmp = value.compareTo(lower);
            if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                return false;
            }
        }
        if (upper != null) {
            final int cmp = value.compareTo(upper);
            return cmp < 0 || (cmp == 0 && includeUpper);
        }
        return true;
    }

    private static List<Integer> matched(DocIdSetIterator iterator) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static int candidates(DocIdSetIterator range) throws IOException {
        final TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(range);
        assertNotNull("an unordered range is answered in two phases", twoPhase);
        int candidates = 0;
        final DocIdSetIterator approximation = twoPhase.approximation();
        while (approximation.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            candidates++;
        }
        return candidates;
    }
}
