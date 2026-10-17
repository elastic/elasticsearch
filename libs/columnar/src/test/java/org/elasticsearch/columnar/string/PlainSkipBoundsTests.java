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
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.lessThan;

public class PlainSkipBoundsTests extends ESTestCase {

    public void testBlockBoundsAreTheExactSmallestAndLargestValue() throws IOException {
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
                assertEquals(fixture.blockCount(), reader.blockCount());
                for (int block = 0; block < fixture.blockCount(); block++) {
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

    public void testAQueryRulesOutABlockExactlyWhenTheBoundsProveItEmpty() throws IOException {
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
                    for (int block = 0; block < fixture.blockCount(); block++) {
                        assertEquals(PlainBlockOracle.excludes(mins[block], maxes[block], query), reader.excludes(block, query));
                    }
                }
            }
        }
    }

    public void testNoBlockHoldingAMatchingValueIsExcluded() throws IOException {
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
                            assertFalse(reader.excludes(address / fixture.valuesPerBlock, query));
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
        assertEquals(fixture.blockCount(), parsed.size());
        for (int block = 0; block < parsed.size(); block++) {
            assertEquals(fixture.slotDocs[block * fixture.valuesPerBlock], parsed.get(block).firstDoc());
            if (mins[block] == null) {
                assertNull(parsed.get(block).min());
                assertNull(parsed.get(block).max());
            } else {
                assertEquals(mins[block], parsed.get(block).min());
                assertEquals(maxes[block], parsed.get(block).max());
            }
        }
    }

    public void testTheDirectoryNamesTheDocumentHoldingTheFirstSlotOfEachBlock() throws IOException {
        for (int round = 0; round < 25; round++) {
            final BytesRef[] pool = PlainSkipTestValues.randomPool(between(1, 10));
            final BytesRef[][] docSlots = randomDocSlots(between(1, 500), pool);
            try (
                Directory dir = newDirectory();
                PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, PlainSkipFixture.randomValuesPerBlock())
            ) {
                final PlainSkipBounds.Reader reader = fixture.reader();
                for (int block = 0; block < fixture.blockCount(); block++) {
                    assertEquals(fixture.slotDocs[block * fixture.valuesPerBlock], reader.firstDoc(block));
                }
            }
        }
    }

    public void testTheRegionHoldsExactlyTheDirectoryAndTheBoundsOfTheBlocks() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            { new BytesRef("abc"), new BytesRef("b") },
            { null },
            { new BytesRef(new byte[3000]) },
            { new BytesRef() } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 2)) {
            final long directory = 3L * 12;
            final long payload = (1 + 8 + 3 + 1) + (1 + 8 + 3000 + 3000) + (1 + 8);
            assertEquals(3, fixture.blockCount());
            assertEquals(3, fixture.skipper.blockCount());
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
            assertEquals(1, fixture.blockCount());
            assertEquals(1 + 8 + 300_000 + 1, fixture.skipper.payloadLength());
            assertSerializedBoundsAreExact(fixture);
            assertFalse(fixture.reader().excludes(0, PlainSkipBounds.Query.range(value, true, value, true)));
        }
    }

    public void testNothingIsAppendedWhenTheRegionIsNotKeptAndStagedFilesAreRemoved() throws IOException {
        try (Directory dir = newDirectory()) {
            final int before = dir.listAll().length;
            try (IndexOutput out = dir.createOutput("kept.cns", IOContext.DEFAULT)) {
                final PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "kept", 1, 2);
                writer.add(0, 0, new BytesRef("a"));
                writer.add(1, 1, new BytesRef("b"));
                assertNull(writer.finish(out, false));
                assertEquals(0, out.getFilePointer());
            }
            assertEquals(before + 1, dir.listAll().length);
            try (IndexOutput out = dir.createOutput("never.cns", IOContext.DEFAULT)) {
                final PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "never", 1, 2);
                writer.add(0, 0, new BytesRef("a"));
                writer.close();
                writer.close();
                assertEquals(0, out.getFilePointer());
            }
            assertEquals(before + 2, dir.listAll().length);
        }
    }

    public void testSlotsMustArriveOneBlockAfterAnother() throws IOException {
        try (
            Directory dir = newDirectory();
            PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "w", 2, 8)
        ) {
            writer.add(0, 0, new BytesRef("a"));
            writer.add(1, 0, new BytesRef("b"));
            expectThrows(IllegalStateException.class, () -> writer.add(4, 2, new BytesRef("c")));
        }
        try (
            Directory dir = newDirectory();
            PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "w", 2, 8)
        ) {
            writer.add(0, 0, new BytesRef("a"));
            writer.add(2, 1, new BytesRef("b"));
            expectThrows(IllegalStateException.class, () -> writer.add(1, 1, new BytesRef("c")));
        }
    }

    public void testAColumnWithoutSlotsWritesNoRegion() throws IOException {
        try (Directory dir = newDirectory(); IndexOutput out = dir.createOutput("none.cns", IOContext.DEFAULT)) {
            try (PlainSkipBounds.Writer writer = new PlainSkipBounds.Writer(dir, IOContext.DEFAULT, "none", 128, 0)) {
                assertNull(writer.finish(out, true));
            }
            assertEquals(0, out.getFilePointer());
        }
    }

    public void testOpeningAReaderAndAskingAboutABlockReadsAFewBytesWhateverTheColumnSize() throws IOException {
        final int blocks = 20_000;
        final BytesRef[][] docSlots = new BytesRef[blocks * 4][];
        for (int doc = 0; doc < docSlots.length; doc++) {
            docSlots[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "value-%08d", doc)) };
        }
        final AtomicLong bytesRead = new AtomicLong();
        try (Directory delegate = newDirectory(); Directory dir = new ReadCountingDirectory(delegate, bytesRead)) {
            try (PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 4)) {
                assertEquals(blocks, fixture.skipper.blockCount());
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
            final int directory = 2 * 12;
            expectCorrupt(dir, fixture, directory + 0, (byte) 9, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, 4 + 7, (byte) 0x40, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, 12 + 4 + 7, (byte) 0x7F, reader -> reader.excludes(1, query));
            expectCorrupt(dir, fixture, 3, (byte) 0x80, reader -> reader.firstDoc(0));
            expectCorrupt(dir, fixture, directory + 1 + 3, (byte) 0x7F, reader -> reader.excludes(0, query));
            expectCorrupt(dir, fixture, directory + 1, (byte) 0x7F, reader -> reader.excludes(0, query));
            final PlainSkipBounds.Reader reader = fixture.reader();
            expectThrows(CorruptIndexException.class, () -> reader.excludes(2, query));
            expectThrows(CorruptIndexException.class, () -> reader.firstDoc(-1));
        }
    }

    public void testARegionPastTheEndOfTheSkipIndexIsCorrupt() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") }, { new BytesRef("b") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            final StringColumnMetadata.Plain.Skipper skipper = fixture.skipper;
            for (StringColumnMetadata.Plain.Skipper bad : new StringColumnMetadata.Plain.Skipper[] {
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset() + 1, skipper.blockCount(), skipper.payloadLength()),
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset(), skipper.blockCount() + 1, skipper.payloadLength()),
                new StringColumnMetadata.Plain.Skipper(skipper.dataOffset(), skipper.blockCount(), skipper.payloadLength() + 1),
                new StringColumnMetadata.Plain.Skipper(-1, skipper.blockCount(), skipper.payloadLength()) }) {
                expectThrows(CorruptIndexException.class, () -> new PlainSkipBounds.Reader(bad, fixture.input));
            }
        }
    }

    public void testTheMetadataOfASkipperMustMatchTheColumn() throws IOException {
        final StringColumnMetadata.Plain.Skipper skipper = new StringColumnMetadata.Plain.Skipper(5, 3, 40);
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        skipper.writeTo(out);
        assertEquals(skipper, StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 9, 4));
        expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 13, 4));
        expectThrows(CorruptIndexException.class, () -> StringColumnMetadata.Plain.Skipper.readFrom(out.toDataInput(), 9, 2));
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
        private final AtomicLong bytesRead;

        ReadCountingDirectory(Directory in, AtomicLong bytesRead) {
            super(in);
            this.bytesRead = bytesRead;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingInput(super.openInput(name, context), bytesRead);
        }
    }

    private static final class CountingInput extends FilterIndexInput {
        private final AtomicLong bytesRead;

        CountingInput(IndexInput in, AtomicLong bytesRead) {
            super(in.toString(), in);
            this.bytesRead = bytesRead;
        }

        @Override
        public byte readByte() throws IOException {
            bytesRead.incrementAndGet();
            return super.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            bytesRead.addAndGet(len);
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
                    bytesRead.incrementAndGet();
                    return delegate.readByte(pos);
                }

                @Override
                public void readBytes(long pos, byte[] bytes, int off, int len) throws IOException {
                    bytesRead.addAndGet(len);
                    delegate.readBytes(pos, bytes, off, len);
                }

                @Override
                public short readShort(long pos) throws IOException {
                    bytesRead.addAndGet(Short.BYTES);
                    return delegate.readShort(pos);
                }

                @Override
                public int readInt(long pos) throws IOException {
                    bytesRead.addAndGet(Integer.BYTES);
                    return delegate.readInt(pos);
                }

                @Override
                public long readLong(long pos) throws IOException {
                    bytesRead.addAndGet(Long.BYTES);
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
