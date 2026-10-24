/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class PlainRangePresenceTests extends ESTestCase {

    public void testTheDocumentsKeptAreExactlyThoseWithASlotInAnEntryTheBoundsKeep() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 25));
            final BytesRef[][] docSlots = PlainSkipBoundsTests.randomDocSlots(between(1, 600), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                for (int i = 0; i < 25; i++) {
                    final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                    final List<Integer> kept = collect(fixture.presence(query));
                    assertEquals(PlainBlockOracle.keptDocs(fixture, query), kept);
                    assertTrue(kept.containsAll(PlainBlockOracle.matchingDocs(fixture, query)));
                }
            }
        }
    }

    public void testAnAdvanceReturnsExactlyTheFirstKeptDocumentFromItsTarget() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 12));
            final BytesRef[][] docSlots = PlainSkipBoundsTests.randomDocSlots(between(1, 600), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                final List<Integer> kept = PlainBlockOracle.keptDocs(fixture, query);
                final PlainRangePresence presence = fixture.presence(query);
                int target = -1;
                int previous = -1;
                while (true) {
                    target += between(1, 40);
                    final int requested = target;
                    final int doc = presence.advance(requested);
                    assertEquals(doc, presence.docID());
                    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                        assertTrue(kept.stream().noneMatch(d -> d >= requested));
                        break;
                    }
                    assertTrue(doc >= requested && doc > previous);
                    assertEquals(kept.stream().filter(d -> d >= requested).findFirst().orElseThrow(), Integer.valueOf(doc));
                    previous = doc;
                    target = doc;
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.docID());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
            }
        }
    }

    public void testARunEndsBeforeADocumentTheIterationWouldNotReturn() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 8));
            final BytesRef[][] docSlots = PlainSkipBoundsTests.randomDocSlots(between(1, 600), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                final BitSet kept = new BitSet();
                PlainBlockOracle.keptDocs(fixture, query).forEach(kept::set);
                final BitSet present = new BitSet();
                fixture.docs.forEach(present::set);
                final PlainRangePresence presence = fixture.presence(query);
                for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
                    final int runEnd = presence.docIDRunEnd();
                    assertTrue(runEnd > doc);
                    for (int inRun = present.nextSetBit(doc); inRun >= 0 && inRun < runEnd; inRun = present.nextSetBit(inRun + 1)) {
                        assertTrue("document " + inRun + " of the run [" + doc + ", " + runEnd + ")", kept.get(inRun));
                    }
                }
            }
        }
    }

    public void testADocumentSpanningEntriesIsKeptWhenAnyOfItsEntriesIsKept() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            { new BytesRef("a"), new BytesRef("a") },
            { new BytesRef("a"), new BytesRef("z"), new BytesRef("a"), new BytesRef("a") },
            { new BytesRef("a"), new BytesRef("a") },
            { new BytesRef("a"), new BytesRef("a") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final BytesRef z = new BytesRef("z");
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(z, true, z, true));
            assertEquals(1, presence.nextDoc());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
            final BytesRef a = new BytesRef("a");
            assertEquals(List.of(0, 1, 2, 3), collect(fixture.presence(PlainSkipBounds.Query.range(a, true, a, true))));
        }
    }

    public void testADocumentWithNoSlotsIsNeverKept() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            new BytesRef[0],
            { new BytesRef("a") },
            new BytesRef[0],
            null,
            { null },
            new BytesRef[0] };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            assertEquals(List.of(1), collect(fixture.presence(PlainSkipBounds.Query.range(null, true, null, true))));
            assertEquals(List.of(), collect(fixture.presence(PlainSkipBounds.Query.range(new BytesRef("b"), true, null, true))));
        }
    }

    public void testWholeEntriesOutsideTheQueryAreSkippedWithoutVisitingTheirDocuments() throws IOException {
        final int blocks = 10;
        final int perBlock = 8;
        final BytesRef[][] docSlots = new BytesRef[blocks * perBlock][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(new byte[] { (byte) ('a' + doc / perBlock) }) };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, perBlock)) {
            final BytesRef wanted = new BytesRef("e");
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(wanted, true, wanted, true));
            assertEquals(4 * perBlock, presence.nextDoc());
            assertEquals(5 * perBlock, presence.docIDRunEnd());
            assertEquals(5 * perBlock - 1, presence.advance(5 * perBlock - 1));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
            final PlainRangePresence prefix = fixture.presence(PlainSkipBounds.Query.prefix(new BytesRef("j")));
            assertEquals(9 * perBlock, prefix.nextDoc());
            assertEquals(10 * perBlock, prefix.docIDRunEnd());
        }
    }

    public void testTheFinalPartialEntryAndTargetsPastTheEndTerminate() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[2 * 8 + 3][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc >= 16 ? "z" : "a") };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 8)) {
            final PlainSkipBounds.Query z = PlainSkipBounds.Query.range(new BytesRef("z"), true, new BytesRef("z"), true);
            PlainRangePresence presence = fixture.presence(z);
            assertEquals(16, presence.nextDoc());
            assertEquals(19, presence.docIDRunEnd());
            presence = fixture.presence(z);
            assertEquals(18, presence.advance(18));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
            presence = fixture.presence(z);
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.advance(docSlots.length + 100));
            presence = fixture.presence(PlainSkipBounds.Query.range(null, true, new BytesRef("a"), true));
            assertEquals(0, presence.nextDoc());
            assertEquals(15, presence.advance(15));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.nextDoc());
        }
    }

    public void testTwoIteratorsOverTheSameColumnDoNotShareState() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.randomPool(8);
        final BytesRef[][] docSlots = PlainSkipBoundsTests.randomDocSlots(400, pool);
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
            final PlainSkipBounds.Query first = PlainBlockOracle.randomQuery(pool);
            final PlainSkipBounds.Query second = PlainBlockOracle.randomQuery(pool);
            final PlainRangePresence one = fixture.presence(first);
            final PlainRangePresence two = fixture.presence(second);
            final List<Integer> keptByOne = new ArrayList<>();
            final List<Integer> keptByTwo = new ArrayList<>();
            int docOne = one.nextDoc();
            int docTwo = two.nextDoc();
            while (docOne != DocIdSetIterator.NO_MORE_DOCS || docTwo != DocIdSetIterator.NO_MORE_DOCS) {
                if (docOne != DocIdSetIterator.NO_MORE_DOCS) {
                    keptByOne.add(docOne);
                    docOne = one.nextDoc();
                }
                if (docTwo != DocIdSetIterator.NO_MORE_DOCS) {
                    keptByTwo.add(docTwo);
                    docTwo = two.advance(docTwo + between(1, 3));
                }
            }
            assertEquals(PlainBlockOracle.keptDocs(fixture, first), keptByOne);
            assertTrue(PlainBlockOracle.keptDocs(fixture, second).containsAll(keptByTwo));
        }
    }

    public void testIntoBitSetSetsExactlyTheKeptDocumentsAndLeavesTheIteratorAfterThem() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(2, 12));
            final BytesRef[][] docSlots = new BytesRef[between(1, 600)][];
            for (int doc = 0; doc < docSlots.length; doc++) {
                if (randomInt(9) != 0) {
                    docSlots[doc] = new BytesRef[] { pool[pool.length * (docSlots.length - doc - 1) / docSlots.length] };
                }
            }
            docSlots[docSlots.length - 1] = new BytesRef[] { pool[0] };
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                assertTrue(fixture.slots().oneSlotADocument());
                final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                final List<Integer> kept = PlainBlockOracle.keptDocs(fixture, query);
                final PlainRangePresence presence = fixture.presence(query);
                final List<Integer> collected = new ArrayList<>();
                int doc = presence.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    final int upTo = doc + between(1, 200);
                    final int offset = between(0, doc);
                    final FixedBitSet bits = new FixedBitSet(upTo - offset);
                    presence.intoBitSet(upTo, bits, offset);
                    for (int bit = bits.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bit + 1 < bits.length()
                        ? bits.nextSetBit(bit + 1)
                        : DocIdSetIterator.NO_MORE_DOCS) {
                        collected.add(bit + offset);
                    }
                    doc = presence.docID();
                    assertTrue(doc >= upTo);
                    if (doc != DocIdSetIterator.NO_MORE_DOCS && randomBoolean()) {
                        collected.add(doc);
                        doc = presence.nextDoc();
                    }
                }
                assertEquals(kept, collected);
            }
        }
    }

    public void testALargeGapBetweenDocumentsIsCrossedInOneAdvance() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[2_000_008][];
        for (int doc = 0; doc < 8; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef("a") };
        }
        for (int doc = 2_000_000; doc < 2_000_008; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef("b") };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
            final BytesRef b = new BytesRef("b");
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(b, true, b, true));
            assertEquals(2_000_000, presence.nextDoc());
            assertEquals(2_000_004, presence.docIDRunEnd());
            assertEquals(2_000_004, presence.advance(2_000_004));
            assertEquals(2_000_008, presence.docIDRunEnd());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.advance(2_000_008));
            final PlainRangePresence none = fixture.presence(PlainSkipBounds.Query.range(new BytesRef("c"), true, null, true));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, none.nextDoc());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, none.docID());
            final PlainRangePresence early = fixture.presence(PlainSkipBounds.Query.range(null, true, new BytesRef("a"), true));
            assertEquals(0, early.nextDoc());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, early.advance(8));
        }
    }

    public void testAnEntryStartingAtOrBeforeTheCurrentDocumentIsCorrupt() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[8][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc < 4 ? "a" : "b") };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
            final BytesRef b = new BytesRef("b");
            try (PlainSkipFixture corrupt = fixture.withByte(dir, 28, (byte) 0)) {
                final PlainRangePresence presence = corrupt.presence(PlainSkipBounds.Query.range(b, true, b, true));
                expectThrows(CorruptIndexException.class, presence::nextDoc);
            }
        }
    }

    public void testAStoredDocumentIdCannotShortenARun() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = twoBlocks(dir)) {
            final PlainSkipBounds.Query keepFirstBlock = PlainSkipBounds.Query.range(new BytesRef("a"), true, new BytesRef("a"), true);
            for (int stored : new int[] { 0, 1, 4, 100 }) {
                try (PlainSkipFixture corrupt = fixture.withByte(dir, 28, (byte) stored)) {
                    final PlainRangePresence presence = corrupt.presence(keepFirstBlock);
                    assertEquals(0, presence.nextDoc());
                    assertEquals(4, presence.docIDRunEnd());
                    final FixedBitSet bits = new FixedBitSet(8);
                    presence.intoBitSet(8, bits, 0);
                    assertEquals(4, bits.cardinality());
                    assertEquals(4, bits.nextClearBit(0));
                    assertEquals(List.of(0, 1, 2, 3), collect(corrupt.presence(keepFirstBlock)));
                }
            }
        }
    }

    public void testAJumpToADocumentThatDoesNotOwnTheEntryIsCorrupt() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = twoBlocks(dir)) {
            final PlainSkipBounds.Query skipFirstBlock = PlainSkipBounds.Query.range(new BytesRef("b"), true, new BytesRef("b"), true);
            assertEquals(List.of(4, 5, 6, 7), collect(fixture.presence(skipFirstBlock)));
            for (int stored : new int[] { 0, 6, 8, 100 }) {
                try (PlainSkipFixture corrupt = fixture.withByte(dir, 28, (byte) stored)) {
                    final PlainRangePresence presence = corrupt.presence(skipFirstBlock);
                    expectThrows(CorruptIndexException.class, presence::nextDoc);
                }
            }
        }
    }

    private static PlainSkipFixture twoBlocks(Directory dir) throws IOException {
        final BytesRef[][] docSlots = new BytesRef[8][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc < 4 ? "a" : "b") };
        }
        return PlainSkipFixture.build(dir, docSlots, 4);
    }

    public void testARunAfterAGapInThePresenceEndsWhereItsEntryEnds() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[228][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            if (doc < 2 || (doc >= 100 && doc < 226)) {
                docSlots[doc] = new BytesRef[] { new BytesRef("a") };
            } else if (doc >= 226) {
                docSlots[doc] = new BytesRef[] { new BytesRef("b") };
            }
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 128)) {
            final BytesRef a = new BytesRef("a");
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(a, true, a, true));
            assertEquals(0, presence.nextDoc());
            assertEquals(2, presence.docIDRunEnd());
            assertEquals(100, presence.advance(100));
            assertEquals(226, presence.docIDRunEnd());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, presence.advance(226));
        }
    }

    public void testARunReadAfterAGapInsideAKeptEntryEndsPastTheDocument() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[34][];
        for (int start : new int[] { 0, 10, 20, 30 }) {
            for (int doc = start; doc < start + 4; doc++) {
                docSlots[doc] = new BytesRef[] { new BytesRef("a") };
            }
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 8)) {
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(null, true, null, true));
            assertEquals(0, presence.nextDoc());
            assertEquals(4, presence.docIDRunEnd());
            for (int doc = 1; doc < 4; doc++) {
                assertEquals(doc, presence.nextDoc());
            }
            assertEquals(10, presence.nextDoc());
            assertEquals(14, presence.docIDRunEnd());
            assertEquals(11, presence.nextDoc());
            assertEquals(14, presence.docIDRunEnd());
            assertEquals(List.of(12, 13, 20, 21, 22, 23, 30, 31, 32, 33), collect(presence));
        }
    }

    public void testTheRankIsThatOfTheWrappedIterator() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[40][];
        for (int doc = 0; doc < docSlots.length; doc += 3) {
            docSlots[doc] = new BytesRef[] { new BytesRef(doc >= 24 ? "q" : "a") };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final BytesRef q = new BytesRef("q");
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(q, true, q, true));
            for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
                assertEquals(fixture.docs.indexOf(doc), presence.rank());
            }
        }
    }

    public void testExactPositioningIsNotSupported() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            final PlainRangePresence presence = fixture.presence(PlainSkipBounds.Query.range(null, true, null, true));
            expectThrows(UnsupportedOperationException.class, () -> presence.advanceExact(0));
            expectThrows(UnsupportedOperationException.class, () -> presence.ranks(new int[] { 0 }, 0, 1, new int[1]));
        }
    }

    private static List<Integer> collect(PlainRangePresence presence) throws IOException {
        final List<Integer> kept = new ArrayList<>();
        for (int doc = presence.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = presence.nextDoc()) {
            kept.add(doc);
        }
        return kept;
    }
}
