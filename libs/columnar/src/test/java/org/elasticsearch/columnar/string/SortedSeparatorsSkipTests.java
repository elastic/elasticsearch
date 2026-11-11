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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortedSeparatorsSkipTests extends ESTestCase {

    public void testTheEntriesTheReaderRulesOutAreExactlyThoseTheSeparatorsRuleOut() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 25));
            final BytesRef[][] docSlots = PlainSkipTestValues.orderedDocSlots(between(1, 600), pool, randomBoolean());
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                assertNotNull(fixture.skipper);
                final BytesRef[] mins = PlainBlockOracle.mins(fixture);
                final BytesRef[] maxes = PlainBlockOracle.maxes(fixture);
                final PlainSkipKind.Reader reader = fixture.reader();
                for (int i = 0; i < 25; i++) {
                    final PlainSkipQuery query = PlainBlockOracle.randomQuery(pool);
                    for (int entry = 0; entry < fixture.entryCount(); entry++) {
                        final boolean excluded = reader.excludes(entry, query);
                        assertEquals(PlainBlockOracle.excludes(mins[entry], maxes[entry], query), excluded);
                        if (excluded) {
                            assertNoSlotMatches(fixture, entry, query);
                        }
                    }
                }
            }
        }
    }

    public void testTheNextCandidateIsTheFirstEntryNotRuledOut() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 25));
            final BytesRef[][] docSlots = PlainSkipTestValues.orderedDocSlots(between(1, 600), pool, randomBoolean());
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipKind.Reader reader = fixture.reader();
                for (int i = 0; i < 25; i++) {
                    final PlainSkipQuery query = PlainBlockOracle.randomQuery(pool);
                    for (int from = 0; from <= fixture.entryCount(); from++) {
                        int expected = from;
                        while (expected < fixture.entryCount() && reader.excludes(expected, query)) {
                            expected++;
                        }
                        assertEquals(expected, reader.nextCandidate(from, query));
                    }
                }
            }
        }
    }

    public void testAnEntryWithoutAValueTakesTheNextSeparator() throws IOException {
        final BytesRef b = new BytesRef("b");
        final BytesRef d = new BytesRef("d");
        final BytesRef[][] ascending = new BytesRef[][] { { null }, { b }, { null }, { d }, { null } };
        final BytesRef[][] descending = new BytesRef[][] { { null }, { d }, { null }, { b }, { null } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, ascending, 1)) {
            assertExcluded(fixture, term("a"), true, true, true, true, true);
            assertExcluded(fixture, term("b"), false, false, true, true, true);
            assertExcluded(fixture, term("c"), true, false, true, true, true);
            assertExcluded(fixture, term("d"), true, false, false, false, false);
            assertExcluded(fixture, term("e"), true, true, true, true, true);
            assertEquals(1, fixture.reader().nextCandidate(0, term("c")));
            assertEquals(5, fixture.reader().nextCandidate(2, term("c")));
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, descending, 1)) {
            assertExcluded(fixture, term("a"), true, true, true, true, true);
            assertExcluded(fixture, term("b"), true, false, false, false, false);
            assertExcluded(fixture, term("c"), true, false, true, true, true);
            assertExcluded(fixture, term("d"), false, false, true, true, true);
            assertExcluded(fixture, term("e"), true, true, true, true, true);
            assertEquals(1, fixture.reader().nextCandidate(0, term("c")));
        }
    }

    public void testAConstantColumnIsInOrderEitherWay() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[6][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef("m") };
        }
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            assertNotNull(fixture.skipper);
            assertExcluded(fixture, term("m"), false, false, false);
            assertExcluded(fixture, term("a"), true, true, true);
            assertExcluded(fixture, term("z"), true, true, true);
            assertEquals(3, fixture.reader().nextCandidate(0, term("z")));
        }
    }

    public void testAColumnOutOfOrderGetsNoRegion() throws IOException {
        final List<List<String>> unordered = List.of(
            List.of("a", "c", "b"),
            List.of("c", "a", "b"),
            List.of("b", "b", "c", "a"),
            List.of("a", "a", "b", "b", "a")
        );
        for (List<String> values : unordered) {
            try (Directory dir = newDirectory(); IndexOutput skipIndex = dir.createOutput("skip", IOContext.DEFAULT)) {
                final PlainSkipKind.Writer writer = PlainSkipKind.forWriting()
                    .newWriter(dir, IOContext.DEFAULT, "unordered", values.size());
                for (int slot = 0; slot < values.size(); slot++) {
                    writer.add(slot, slot, slot / 2, new BytesRef(values.get(slot)));
                }
                assertNull(values.toString(), writer.finish(skipIndex));
                assertEquals("nothing is written to the skip index", 0, skipIndex.getFilePointer());
                assertNoStagedFiles(dir);
            }
        }
    }

    public void testTheStagedFilesAreRemovedWhenTheWriterIsClosedUnfinished() throws IOException {
        try (Directory dir = newDirectory()) {
            final PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "unfinished", 4);
            writer.add(0, 0, 0, new BytesRef("a"));
            writer.close();
            assertNoStagedFiles(dir);
        }
    }

    public void testASlotOutOfSequenceIsRejected() throws IOException {
        try (Directory dir = newDirectory()) {
            try (PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "sequence", 4)) {
                writer.add(0, 0, 0, new BytesRef("a"));
                expectThrows(IllegalStateException.class, () -> writer.add(2, 2, 0, new BytesRef("b")));
            }
            try (PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "chunks", 4)) {
                writer.add(0, 0, 3, new BytesRef("a"));
                expectThrows(IllegalStateException.class, () -> writer.add(1, 1, 2, new BytesRef("b")));
            }
            try (
                PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "short", 4);
                IndexOutput skipIndex = dir.createOutput("skip", IOContext.DEFAULT)
            ) {
                writer.add(0, 0, 0, new BytesRef("a"));
                expectThrows(IllegalStateException.class, () -> writer.finish(skipIndex));
            }
            assertNoStagedFiles(dir);
        }
    }

    public void testAColumnOfNullsGetsNoRegion() throws IOException {
        try (Directory dir = newDirectory(); IndexOutput skipIndex = dir.createOutput("skip", IOContext.DEFAULT)) {
            final PlainSkipKind.Writer writer = PlainSkipKind.forWriting().newWriter(dir, IOContext.DEFAULT, "nulls", 3);
            for (int slot = 0; slot < 3; slot++) {
                writer.add(slot, slot, 0, null);
            }
            assertNull(writer.finish(skipIndex));
            assertNoStagedFiles(dir);
        }
    }

    public void testAnUnknownDirectionIsCorrupt() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeEntries(dir)) {
            try (PlainSkipFixture corrupt = fixture.withByte(dir, 0, (byte) between(2, 100))) {
                expectThrows(CorruptIndexException.class, corrupt::reader);
            }
        }
    }

    public void testSeparatorsOutOfOrderAreCorruptWhenTheyRuleAnEntryOut() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeEntries(dir)) {
            final int firstSeparatorByte = SortedSeparatorsSkip.HEADER_BYTES + 3 * SortedSeparatorsSkip.ENTRY_BYTES
                + SortedSeparatorsSkip.LENGTH_BYTES;
            try (PlainSkipFixture corrupt = fixture.withByte(dir, firstSeparatorByte, (byte) 'z')) {
                final PlainSkipKind.Reader reader = corrupt.reader();
                expectThrows(CorruptIndexException.class, () -> reader.excludes(0, term("a")));
            }
        }
    }

    public void testARegionPastTheEndOfTheSkipIndexIsCorrupt() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeEntries(dir)) {
            final StringColumnMetadata.Plain.Skipper skipper = fixture.skipper;
            for (StringColumnMetadata.Plain.Skipper bad : new StringColumnMetadata.Plain.Skipper[] {
                new StringColumnMetadata.Plain.Skipper(
                    skipper.kind(),
                    skipper.dataOffset() + 1,
                    skipper.entryCount(),
                    skipper.payloadLength()
                ),
                new StringColumnMetadata.Plain.Skipper(
                    skipper.kind(),
                    skipper.dataOffset(),
                    skipper.entryCount() + 1,
                    skipper.payloadLength()
                ),
                new StringColumnMetadata.Plain.Skipper(
                    skipper.kind(),
                    skipper.dataOffset(),
                    skipper.entryCount(),
                    skipper.payloadLength() + 1
                ),
                new StringColumnMetadata.Plain.Skipper(skipper.kind(), -1, skipper.entryCount(), skipper.payloadLength()) }) {
                expectThrows(
                    CorruptIndexException.class,
                    () -> SortedSeparatorsSkip.INSTANCE.newReader(bad, fixture.numValues, fixture.input)
                );
            }
        }
    }

    private static PlainSkipFixture threeEntries(Directory dir) throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") }, { new BytesRef("b") }, { new BytesRef("c") } };
        return PlainSkipFixture.build(dir, docSlots, 1);
    }

    private static PlainSkipQuery term(String value) {
        final BytesRef bytes = new BytesRef(value);
        return PlainSkipQuery.range(bytes, true, bytes, true);
    }

    private static void assertExcluded(PlainSkipFixture fixture, PlainSkipQuery query, boolean... expected) throws IOException {
        assertEquals(expected.length, fixture.entryCount());
        final PlainSkipKind.Reader reader = fixture.reader();
        for (int entry = 0; entry < expected.length; entry++) {
            assertEquals("entry " + entry, expected[entry], reader.excludes(entry, query));
        }
    }

    private static void assertNoSlotMatches(PlainSkipFixture fixture, int entry, PlainSkipQuery query) {
        final long end = entry + 1 < fixture.entryCount() ? fixture.entryFirstSlot[entry + 1] : fixture.numValues;
        for (long slot = fixture.entryFirstSlot[entry]; slot < end; slot++) {
            final BytesRef value = fixture.slotValues[(int) slot];
            assertFalse("an excluded entry owns a match", value != null && PlainBlockOracle.matches(value, query));
        }
    }

    private static void assertNoStagedFiles(Directory dir) throws IOException {
        final List<String> staged = new ArrayList<>();
        for (String file : dir.listAll()) {
            if (file.contains("columnar-plain-separators")) {
                staged.add(file);
            }
        }
        assertEquals(List.of(), staged);
    }
}
