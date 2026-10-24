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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class PlainSkipBoundsTests extends ESTestCase {

    public void testEntryBoundsAreTheExactSmallestAndLargestValue() throws IOException {
        for (int round = 0; round < 20; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 30));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 400), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final BytesRef[] mins = PlainBlockOracle.mins(fixture);
                final BytesRef[] maxes = PlainBlockOracle.maxes(fixture);
                final PlainSkipBounds.Reader reader = fixture.reader();
                assertEquals(fixture.entryCount(), reader.entryCount());
                for (int block = 0; block < fixture.entryCount(); block++) {
                    if (mins[block] == null) {
                        assertTrue(reader.excludes(block, PlainSkipBounds.Query.range(null, true, null, true)));
                        continue;
                    }
                    assertFalse(reader.excludes(block, PlainSkipBounds.Query.range(mins[block], true, maxes[block], true)));
                    assertTrue(reader.excludes(block, PlainSkipBounds.Query.range(null, true, mins[block], false)));
                    assertTrue(reader.excludes(block, PlainSkipBounds.Query.range(maxes[block], false, null, true)));
                    assertFalse(reader.excludes(block, PlainSkipBounds.Query.range(null, true, mins[block], true)));
                    assertFalse(reader.excludes(block, PlainSkipBounds.Query.range(maxes[block], true, null, true)));
                }
            }
        }
    }

    public void testAQueryRulesOutAnEntryExactlyWhenTheBoundsProveItEmpty() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 20));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 300), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final BytesRef[] mins = PlainBlockOracle.mins(fixture);
                final BytesRef[] maxes = PlainBlockOracle.maxes(fixture);
                final PlainSkipBounds.Reader reader = fixture.reader();
                for (int i = 0; i < 40; i++) {
                    final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                    for (int block = 0; block < fixture.entryCount(); block++) {
                        assertEquals(PlainBlockOracle.excludes(mins[block], maxes[block], query), reader.excludes(block, query));
                    }
                }
            }
        }
    }

    public void testNoEntryHoldingAMatchingValueIsExcluded() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 20));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 300), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipBounds.Reader reader = fixture.reader();
                for (int i = 0; i < 40; i++) {
                    final PlainSkipBounds.Query query = PlainBlockOracle.randomQuery(pool);
                    for (int address = 0; address < fixture.slotValues.length; address++) {
                        final BytesRef value = fixture.slotValues[address];
                        if (value != null && PlainBlockOracle.matches(value, query)) {
                            assertFalse(reader.excludes(fixture.entryOfSlot[address], query));
                        }
                    }
                }
            }
        }
    }

    public void testLongValuesSharedPrefixesZerosAndHighBytesKeepTheirExactOrder() throws IOException {
        final byte[] stem = new byte[700];
        Arrays.fill(stem, (byte) 'x');
        final BytesRef low = bytes(stem, 0, 0);
        final BytesRef mid = bytes(stem, 0xFF, 0xFF);
        final BytesRef high = bytes(stem, 0xFF, 0xFF, 0xFF);
        final BytesRef beyond = bytes(stem, 0xFF, 0xFF, 0xFF, 0);
        final BytesRef[][] docSlots = new BytesRef[][] { { mid, low, high, beyond }, { new BytesRef() }, { null } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
            final PlainSkipBounds.Reader reader = fixture.reader();
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.range(low, true, low, true)));
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.range(beyond, true, beyond, true)));
            assertTrue(reader.excludes(0, PlainSkipBounds.Query.range(bytes(stem, 0xFF, 0xFF, 0xFF, 0, 0), true, null, true)));
            assertTrue(reader.excludes(0, PlainSkipBounds.Query.range(null, true, low, false)));
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.range(null, true, low, true)));
            assertTrue(reader.excludes(0, PlainSkipBounds.Query.range(null, true, bytes(stem, 0), true)));
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.prefix(bytes(stem, 0xFF))));
            assertTrue(reader.excludes(0, PlainSkipBounds.Query.prefix(bytes(stem, 0xFF, 0xFF, 0xFF, 0, 0))));
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.prefix(new BytesRef())));
            assertFalse(reader.excludes(1, PlainSkipBounds.Query.range(new BytesRef(), true, new BytesRef(), true)));
            assertTrue(reader.excludes(1, PlainSkipBounds.Query.range(low, true, null, true)));
        }
    }

    public void testAnEmptyStringIsAValueAndANullIsNot() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef() }, { null }, { null }, { null }, { new BytesRef("a") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            final PlainSkipBounds.Reader reader = fixture.reader();
            final PlainSkipBounds.Query everything = PlainSkipBounds.Query.range(null, true, null, true);
            final PlainSkipBounds.Query emptyTerm = PlainSkipBounds.Query.range(new BytesRef(), true, new BytesRef(), true);
            assertFalse(reader.excludes(0, everything));
            assertFalse(reader.excludes(0, emptyTerm));
            assertFalse(reader.excludes(0, PlainSkipBounds.Query.prefix(new BytesRef())));
            assertTrue(reader.excludes(0, PlainSkipBounds.Query.range(new BytesRef(), false, null, true)));
            for (int nullBlock = 1; nullBlock <= 3; nullBlock++) {
                assertTrue(reader.excludes(nullBlock, everything));
                assertTrue(reader.excludes(nullBlock, emptyTerm));
                assertTrue(reader.excludes(nullBlock, PlainSkipBounds.Query.prefix(new BytesRef())));
            }
            assertTrue(reader.excludes(4, emptyTerm));
        }
    }

    public void testTheSerializedBoundsAreTheExactBytesOfTheValues() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 30));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 400), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock(), randomBoolean())
            ) {
                assertSerializedBoundsAreExact(fixture);
            }
        }
    }

    static void assertSerializedBoundsAreExact(PlainSkipFixture fixture) throws IOException {
        final List<PlainSkipFixture.Parsed> parsed = PlainSkipFixture.parse(fixture.input.clone(), fixture.skipper);
        final BytesRef[] mins = PlainBlockOracle.mins(fixture);
        final BytesRef[] maxes = PlainBlockOracle.maxes(fixture);
        assertEquals(fixture.entryCount(), parsed.size());
        for (int block = 0; block < parsed.size(); block++) {
            assertEquals(fixture.entryFirstSlot[block], parsed.get(block).firstSlot());
            assertEquals(fixture.slotDocs[(int) fixture.entryFirstSlot[block]], parsed.get(block).firstDoc());
            if (mins[block] == null) {
                assertNull(parsed.get(block).min());
                assertNull(parsed.get(block).max());
            } else {
                assertEquals(mins[block], parsed.get(block).min());
                assertEquals(maxes[block], parsed.get(block).max());
            }
        }
    }

    public void testTheDirectoryNamesTheDocumentHoldingTheFirstSlotOfEachEntry() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 10));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 500), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipBounds.Reader reader = fixture.reader();
                for (int block = 0; block < fixture.entryCount(); block++) {
                    assertEquals(fixture.slotDocs[(int) fixture.entryFirstSlot[block]], reader.firstDoc(block));
                }
            }
        }
    }

    public void testTheRegionHoldsExactlyTheDirectoryAndTheBoundsOfTheEntries() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            { new BytesRef("abc"), new BytesRef("b") },
            { null },
            { new BytesRef(new byte[3000]) },
            { new BytesRef() } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final long directory = 3L * 20;
            final long payload = (1 + 8 + 3 + 1) + (1 + 8 + 3000 + 3000) + (1 + 8);
            assertEquals(3, fixture.entryCount());
            assertEquals(3, fixture.skipper.entryCount());
            assertEquals(payload, fixture.skipper.payloadLength());
            assertEquals(fixture.leadingBytes + directory + payload, fixture.input.length());
        }
    }

    public void testAValueIsNeverCutToFitTheRegion() throws IOException {
        final byte[] huge = new byte[300_000];
        for (int i = 0; i < huge.length; i++) {
            huge[i] = (byte) (i * 31);
        }
        final BytesRef value = new BytesRef(huge);
        final BytesRef[][] docSlots = new BytesRef[][] { { value }, { new BytesRef("a") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            assertEquals(1, fixture.entryCount());
            assertEquals(1 + 8 + 300_000 + 1, fixture.skipper.payloadLength());
            assertSerializedBoundsAreExact(fixture);
            assertFalse(fixture.reader().excludes(0, PlainSkipBounds.Query.range(value, true, value, true)));
        }
    }

    public void testNothingIsAppendedWhenTheWriterIsNotFinishedAndStagedFilesAreRemoved() throws IOException {
        try (Directory dir = newDirectory()) {
            final int before = dir.listAll().length;
            try (IndexOutput out = dir.createOutput("kept.cns", IOContext.DEFAULT)) {
                final PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "kept", 2);
                writer.add(0, 0, 0, new BytesRef("a"));
                writer.add(1, 1, 1, new BytesRef("b"));
                assertNotNull(writer.finish(out));
                assertThat(out.getFilePointer(), greaterThan(0L));
            }
            assertEquals(before + 1, dir.listAll().length);
            try (IndexOutput out = dir.createOutput("never.cns", IOContext.DEFAULT)) {
                final PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "never", 2);
                writer.add(0, 0, 0, new BytesRef("a"));
                writer.close();
                writer.close();
                assertEquals(0, out.getFilePointer());
            }
            assertEquals(before + 2, dir.listAll().length);
        }
    }

    public void testSlotsMustArriveInOrderAndTheOwnerNeverGoesBack() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "w", 8)) {
            writer.add(0, 0, 0, new BytesRef("a"));
            writer.add(1, 0, 0, new BytesRef("b"));
            expectThrows(IllegalStateException.class, () -> writer.add(3, 2, 0, new BytesRef("c")));
        }
        try (Directory dir = newDirectory(); PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "w", 8)) {
            writer.add(0, 0, 0, new BytesRef("a"));
            writer.add(1, 1, 3, new BytesRef("b"));
            expectThrows(IllegalStateException.class, () -> writer.add(2, 1, 2, new BytesRef("c")));
        }
    }

    public void testFewerSlotsThanExpectedCannotBeFinished() throws IOException {
        try (
            Directory dir = newDirectory();
            IndexOutput out = dir.createOutput("few.cns", IOContext.DEFAULT);
            PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "few", 3)
        ) {
            writer.add(0, 0, 0, new BytesRef("a"));
            writer.add(1, 1, 0, new BytesRef("b"));
            expectThrows(IllegalStateException.class, () -> writer.finish(out));
        }
    }

    public void testEveryEntryIsFoundFromEachOfItsSlots() throws IOException {
        for (int round = 0; round < 20; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 10));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 500), pool);
            final int[] owners = PlainSkipFixture.evenOwners(docSlots, 1);
            for (int slot = 1, owner = 0; slot < owners.length; slot++) {
                owner += between(0, 3);
                owners[slot] = owner;
            }
            try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, owners, false)) {
                final PlainSkipBounds.Reader reader = fixture.reader();
                assertEquals(fixture.entryFirstSlot.length, reader.entryCount());
                for (int entry = 0; entry < reader.entryCount(); entry++) {
                    assertEquals(fixture.entryFirstSlot[entry], reader.firstSlot(entry));
                    final long end = entry + 1 < reader.entryCount() ? fixture.entryFirstSlot[entry + 1] : fixture.numValues;
                    assertEquals(end, reader.endSlot(entry));
                }
                for (int slot = 0; slot < fixture.numValues; slot++) {
                    assertEquals("slot " + slot, fixture.entryOfSlot[slot], reader.entryOf(slot));
                }
            }
        }
    }

    public void testAColumnWithoutSlotsWritesNoRegion() throws IOException {
        try (Directory dir = newDirectory(); IndexOutput out = dir.createOutput("none.cns", IOContext.DEFAULT)) {
            try (PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "none", 0)) {
                assertNull(writer.finish(out));
            }
            assertEquals(0, out.getFilePointer());
        }
    }

    public void testOpeningAReaderAndAskingAboutAnEntryReadsAFewBytesWhateverTheColumnSize() throws IOException {
        final int blocks = 20_000;
        final BytesRef[][] docSlots = new BytesRef[blocks * 4][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "value-%08d", doc)) };
        }
        final Counter bytesRead = new Counter();
        try (Directory delegate = newDirectory(); Directory dir = new ReadCountingDirectory(delegate, bytesRead)) {
            try (PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
                assertEquals(blocks, fixture.skipper.entryCount());
                final long afterBuild = bytesRead.get();
                final PlainSkipBounds.Reader reader = fixture.reader();
                assertEquals("opening reads nothing", afterBuild, bytesRead.get());
                assertTrue(reader.excludes(17, PlainSkipBounds.Query.range(new BytesRef("zzz"), true, null, true)));
                assertThat("one block reads its entry and its bounds", bytesRead.get() - afterBuild, lessThan(100L));
                reader.firstDoc(blocks - 1);
                assertThat(bytesRead.get() - afterBuild, lessThan(120L));
            }
        }
    }

    public void testACorruptRegionIsRejected() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            { new BytesRef("a"), new BytesRef("b") },
            { new BytesRef("c") },
            { new BytesRef("d") } };
        final PlainSkipBounds.Query query = PlainSkipBounds.Query.range(null, true, null, true);
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final int directory = 2 * 20;
            expectCorrupt(dir, fixture, directory + 0, (byte) 9, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, 12 + 7, (byte) 0x40, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, 20 + 12 + 7, (byte) 0x7F, reader -> reader.excludes(1, query));
            expectCorrupt(dir, fixture, 8 + 3, (byte) 0x80, reader -> reader.firstDoc(0));
            expectCorrupt(dir, fixture, 7, (byte) 0x80, reader -> reader.firstSlot(0));
            expectCorrupt(dir, fixture, 0, (byte) 1, reader -> reader.firstSlot(0));
            expectCorrupt(dir, fixture, 20, (byte) 0, reader -> reader.endSlot(0));
            expectCorrupt(dir, fixture, 20, (byte) 9, reader -> reader.entryOf(3));
            expectCorrupt(dir, fixture, directory + 1 + 3, (byte) 0x7F, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, directory + 1, (byte) 0x7F, reader -> reader.excludes(0, query));
            final PlainSkipBounds.Reader reader = fixture.reader();
            expectThrows(CorruptIndexException.class, () -> reader.excludes(2, query));
            expectThrows(CorruptIndexException.class, () -> reader.firstDoc(-1));
        }
    }

    public void testBoundsWhoseMinimumExceedsTheMaximumNeverPrune() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a"), new BytesRef("z") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final long minimumByte = 20 + 1 + 8;
            try (
                PlainSkipFixture lowered = fixture.withByte(dir, minimumByte, (byte) 'z');
                PlainSkipFixture reversed = lowered.withByte(dir, minimumByte + 1, (byte) 'a')
            ) {
                final PlainSkipBounds.Reader reader = reversed.reader();
                final BytesRef middle = new BytesRef("m");
                expectThrows(
                    CorruptIndexException.class,
                    () -> reader.excludes(0, PlainSkipBounds.Query.range(middle, true, middle, true))
                );
                expectThrows(CorruptIndexException.class, () -> reader.excludes(0, PlainSkipBounds.Query.prefix(middle)));
                assertFalse(reader.excludes(0, PlainSkipBounds.Query.range(null, true, null, true)));
            }
            try (PlainSkipFixture intact = fixture.withByte(dir, minimumByte, (byte) 'a')) {
                final BytesRef middle = new BytesRef("m");
                assertFalse(intact.reader().excludes(0, PlainSkipBounds.Query.range(middle, true, middle, true)));
            }
        }
    }

    public void testEntriesThatOverlapOrDoNotStartAtTheBeginningAreCorrupt() throws IOException {
        final PlainSkipBounds.Query everything = PlainSkipBounds.Query.range(null, true, null, true);
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeBlocks(dir)) {
            expectCorrupt(dir, fixture, 20 + 12, (byte) 0, reader -> reader.excludes(1, everything));
            expectCorrupt(dir, fixture, 20 + 12, (byte) 0, reader -> reader.excludes(0, everything));
            expectCorrupt(dir, fixture, 40 + 12, (byte) 12, reader -> reader.excludes(1, everything));
            expectCorrupt(dir, fixture, 40 + 12, (byte) 12, reader -> reader.excludes(2, everything));
            expectCorrupt(dir, fixture, 12, (byte) 1, reader -> reader.excludes(0, everything));
            final PlainSkipBounds.Reader intact = fixture.reader();
            for (int block = 0; block < 3; block++) {
                assertFalse(intact.excludes(block, everything));
            }
        }
    }

    public void testAPayloadShorterThanItsEntriesIsCorrupt() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeBlocks(dir)) {
            final StringColumnMetadata.Plain.Skipper skipper = fixture.skipper;
            final PlainSkipBounds.Reader shorter = new PlainSkipBounds.Reader(
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset(), skipper.entryCount(), skipper.payloadLength() - 1),
                fixture.numValues,
                fixture.input
            );
            expectThrows(CorruptIndexException.class, () -> shorter.excludes(2, PlainSkipBounds.Query.range(null, true, null, true)));
        }
    }

    public void testRegionArithmeticThatOverflowsIsCorrupt() throws IOException {
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = threeBlocks(dir)) {
            for (StringColumnMetadata.Plain.Skipper bad : new StringColumnMetadata.Plain.Skipper[] {
                new StringColumnMetadata.Plain.Skipper(Long.MAX_VALUE, 3, 5),
                new StringColumnMetadata.Plain.Skipper(Long.MAX_VALUE - 5, 3, 10),
                new StringColumnMetadata.Plain.Skipper(0, 3, Long.MAX_VALUE),
                new StringColumnMetadata.Plain.Skipper(1, Integer.MAX_VALUE, 0) }) {
                expectThrows(CorruptIndexException.class, () -> new PlainSkipBounds.Reader(bad, fixture.numValues, fixture.input));
            }
        }
    }

    public void testASkipperPresenceByteMustBeZeroOrOne() throws IOException {
        final ByteBuffersDataOutput none = new ByteBuffersDataOutput();
        none.writeByte((byte) 0);
        assertNull(StringColumnMetadata.Plain.Skipper.readOptionally(none.toDataInput(), 9));

        final StringColumnMetadata.Plain.Skipper skipper = new StringColumnMetadata.Plain.Skipper(5, 3, 40);
        final ByteBuffersDataOutput present = new ByteBuffersDataOutput();
        present.writeByte((byte) 1);
        skipper.writeTo(present);
        assertEquals(skipper, StringColumnMetadata.Plain.Skipper.readOptionally(present.toDataInput(), 9));

        for (byte invalid : new byte[] { 2, -1, 127 }) {
            final ByteBuffersDataOutput bad = new ByteBuffersDataOutput();
            bad.writeByte(invalid);
            skipper.writeTo(bad);
            expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readOptionally(bad.toDataInput(), 9));
        }
    }

    public void testASkipperWithNoEntriesOrMoreEntriesThanSlotsIsCorrupt() throws IOException {
        final ByteBuffersDataOutput none = new ByteBuffersDataOutput();
        new StringColumnMetadata.Plain.Skipper(0, 0, 1).writeTo(none);
        expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readFrom(none.toDataInput(), 9));
        final ByteBuffersDataOutput many = new ByteBuffersDataOutput();
        new StringColumnMetadata.Plain.Skipper(0, 10, 1).writeTo(many);
        expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readFrom(many.toDataInput(), 9));
        assertEquals(10, StringColumnMetadata.Plain.Skipper.readFrom(many.toDataInput(), 10).entryCount());
    }

    private static PlainSkipFixture threeBlocks(Directory dir) throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") }, { new BytesRef("bb") }, { new BytesRef("ccc") } };
        return PlainSkipFixture.build(dir, docSlots, 1);
    }

    public void testARegionPastTheEndOfTheSkipIndexIsCorrupt() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") }, { new BytesRef("b") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            final StringColumnMetadata.Plain.Skipper skipper = fixture.skipper;
            for (StringColumnMetadata.Plain.Skipper bad : new StringColumnMetadata.Plain.Skipper[] {
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset() + 1, skipper.entryCount(), skipper.payloadLength()),
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset(), skipper.entryCount() + 1, skipper.payloadLength()),
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset(), skipper.entryCount(), skipper.payloadLength() + 1),
                new StringColumnMetadata.Plain.Skipper(-1, skipper.entryCount(), skipper.payloadLength()) }) {
                expectThrows(CorruptIndexException.class, () -> new PlainSkipBounds.Reader(bad, fixture.numValues, fixture.input));
            }
        }
    }

    public void testTheMetadataOfASkipperMustMatchTheColumn() throws IOException {
        final StringColumnMetadata.Plain.Skipper skipper = new StringColumnMetadata.Plain.Skipper(5, 3, 40);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        skipper.writeTo(out);
        assertEquals(skipper, StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 9));
        assertEquals(skipper, StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 3));
        expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 2));
    }

    private interface Access {
        void run(PlainSkipBounds.Reader reader) throws IOException;
    }

    private void expectCorrupt(Directory dir, PlainSkipFixture fixture, long regionPosition, byte replacement, Access access)
        throws IOException {
        try (PlainSkipFixture corrupt = fixture.withByte(dir, regionPosition, replacement)) {
            final PlainSkipBounds.Reader reader = corrupt.reader();
            expectThrows(CorruptIndexException.class, () -> access.run(reader));
        }
    }

    static BytesRef[][] randomDocSlots(int docs, BytesRef[] pool) {
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int doc = 0; doc < docs; doc++) {
            switch (between(0, 9)) {
                case 0 -> docSlots[doc] = null;
                case 1 -> docSlots[doc] = new BytesRef[0];
                case 2 -> docSlots[doc] = new BytesRef[] { null };
                case 3, 4 -> {
                    final BytesRef[] slots = new BytesRef[between(2, 5)];
                    for (int i = 0; i < slots.length; i++) {
                        slots[i] = rarely() ? null : randomFrom(pool);
                    }
                    docSlots[doc] = slots;
                }
                default -> docSlots[doc] = new BytesRef[] { randomFrom(pool) };
            }
        }
        docSlots[docs - 1] = new BytesRef[] { randomFrom(pool) };
        return docSlots;
    }

    private static BytesRef bytes(byte[] stem, int... tail) {
        final byte[] bytes = Arrays.copyOf(stem, stem.length + tail.length);
        for (int i = 0; i < tail.length; i++) {
            bytes[stem.length + i] = (byte) tail[i];
        }
        return new BytesRef(bytes);
    }

    private static final class ReadCountingDirectory extends FilterDirectory {
        private final Counter bytesRead;

        ReadCountingDirectory(Directory in, Counter bytesRead) {
            super(in);
            this.bytesRead = bytesRead;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingInput(super.openInput(name, context), bytesRead);
        }
    }

    private static final class CountingInput extends FilterIndexInput {
        private final Counter bytesRead;

        CountingInput(IndexInput in, Counter bytesRead) {
            super(in.toString(), in);
            this.bytesRead = bytesRead;
        }

        @Override
        public byte readByte() throws IOException {
            bytesRead.increment();
            return super.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            bytesRead.add(len);
            super.readBytes(b, offset, len);
        }

        @Override
        public IndexInput clone() {
            return new CountingInput(in.clone(), bytesRead);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingInput(in.slice(sliceDescription, offset, length), bytesRead);
        }

        @Override
        public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
            final RandomAccessInput delegate = in.randomAccessSlice(offset, length);
            return new RandomAccessInput() {
                @Override
                public long length() {
                    return delegate.length();
                }

                @Override
                public byte readByte(long pos) throws IOException {
                    bytesRead.increment();
                    return delegate.readByte(pos);
                }

                @Override
                public void readBytes(long pos, byte[] bytes, int off, int len) throws IOException {
                    bytesRead.add(len);
                    delegate.readBytes(pos, bytes, off, len);
                }

                @Override
                public short readShort(long pos) throws IOException {
                    bytesRead.add(Short.BYTES);
                    return delegate.readShort(pos);
                }

                @Override
                public int readInt(long pos) throws IOException {
                    bytesRead.add(Integer.BYTES);
                    return delegate.readInt(pos);
                }

                @Override
                public long readLong(long pos) throws IOException {
                    bytesRead.add(Long.BYTES);
                    return delegate.readLong(pos);
                }

                @Override
                public void prefetch(long offset, long length) throws IOException {
                    delegate.prefetch(offset, length);
                }
            };
        }
    }
}
