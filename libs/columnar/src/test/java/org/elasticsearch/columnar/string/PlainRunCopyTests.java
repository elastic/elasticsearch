/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import static org.hamcrest.Matchers.greaterThan;

/**
 * A plain column written from a run of another plain column's slots, with the run's chunks copied as they were
 * stored ({@link PlainValues.Writer#copy}). Every slot has to read back as it was written, whatever the two
 * columns' block sizes, and wherever the run's repeats and nulls land against the target's blocks of lengths.
 *
 * <p>Both columns are written straight through {@link StringColumnWriter}; the cursor that feeds the target hands
 * it the run, as a merge cursor would. A merge through an index writer is covered by {@code PlainRunMergeTests}.
 */
public class PlainRunCopyTests extends ColumnarStringTestCase {

    /** Counts what the writer reports copying. */
    private static final class CopyCount extends InfoStream {
        int copies;

        @Override
        public void message(String component, String message) {
            copies++;
        }

        @Override
        public boolean isEnabled(String component) {
            return StringColumnWriter.INFO_STREAM_COMPONENT.equals(component);
        }

        @Override
        public void close() {}
    }

    /** What a target column read back as, and how many runs its writer copied. */
    private record Written(StringColumnMetadata.Plain metadata, int copies) {}

    public void testCopiedRunReadsBackAsWritten() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            final boolean nulls = randomBoolean();
            // A column of one length is one with no null, so only a null-free run can be one.
            final int constantLength = nulls == false && randomBoolean() ? between(1, 12) : -1;
            final int maxSlots = randomBoolean() ? 1 : 4;
            final BytesRef[][] source = randomSlots(between(50, 1500), maxSlots, nulls, constantLength);
            // The documents around the run are of the run's shape or not, so the target is sometimes a column of
            // one length and sometimes one of codes around a run that was not.
            final int aroundLength = randomBoolean() ? constantLength : -1;
            final BytesRef[][] prefix = randomSlots(between(0, 300), maxSlots, nulls && randomBoolean(), aroundLength);
            final BytesRef[][] suffix = randomSlots(between(0, 300), maxSlots, nulls && randomBoolean(), aroundLength);
            final int from = between(0, source.length - 1);
            final int to = between(from + 1, source.length);
            final ChunkCodec codec = randomChunkCodec();
            final Written written = copyRun(source, from, to, prefix, suffix, options(codec), options(codec));
            if (written.copies() > 0) {
                assertEquals("one run, copied once", 1, written.copies());
            }
        }
    }

    /** Small chunks and a long run, so there is always a whole chunk to copy. */
    public void testALongRunIsCopied() throws IOException {
        final ChunkCodec codec = randomChunkCodec();
        final BytesRef[][] source = randomSlots(2000, 3, true, -1);
        final BytesRef[][] prefix = randomSlots(between(1, 200), 3, true, -1);
        final BytesRef[][] suffix = randomSlots(between(1, 200), 3, true, -1);
        final Written written = copyRun(source, between(0, 100), between(1900, 2000), prefix, suffix, options(codec), options(codec));
        assertEquals(1, written.copies());
    }

    /**
     * Almost every slot repeats the one before, and the run is placed off the target's blocks of lengths, so a repeat
     * falls on nearly every block start of the target. Each has to be recorded as the value it repeats, over bytes
     * already written, for the target's bytes to stay the source's.
     */
    public void testRepeatsLandingOnTargetBlockStarts() throws IOException {
        final BytesRef[][] source = new BytesRef[3000][];
        BytesRef value = new BytesRef(randomAlphaOfLength(40));
        for (int doc = 0; doc < source.length; doc++) {
            if (random().nextInt(500) == 0) {
                value = new BytesRef(randomAlphaOfLength(40));
            }
            source[doc] = new BytesRef[] { value };
        }
        // Fewer documents before the run than a block of lengths holds, and never a multiple of one.
        final BytesRef[][] prefix = randomSlots(between(1, 127), 1, false, -1);
        final ChunkCodec codec = randomChunkCodec();
        final Written written = copyRun(source, 0, source.length, prefix, new BytesRef[0][], options(codec), options(codec));
        assertEquals(1, written.copies());
    }

    /**
     * A column written by a copy records a repeat that lands on a block start as the value it repeats, starting over
     * bytes already written. Copying from such a column in turn has to keep reading those slots as they were.
     */
    public void testCopyingFromAColumnThatWasCopied() throws IOException {
        for (int iter = 0; iter < 60; iter++) {
            final boolean nulls = randomBoolean();
            final int maxSlots = randomBoolean() ? 1 : 3;
            final BytesRef[][] source = randomSlots(between(1500, 4000), maxSlots, nulls, -1);
            final ChunkCodec codec = randomChunkCodec();
            final StringColumnOptions options = options(codec);
            final BytesRef[][] prefix = randomSlots(between(1, 127), maxSlots, nulls, -1);
            final BytesRef[][] middle = concat(prefix, source);
            final byte[] segmentId = new byte[16];
            random().nextBytes(segmentId);
            try (Directory dir = newDirectory()) {
                final StringColumnMetadata sourceMeta;
                try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "src", segmentId)) {
                    sourceMeta = StringColumnWriter.write(
                        source.length,
                        totals(source),
                        () -> cursor(source),
                        options,
                        null,
                        dir,
                        IOContext.DEFAULT,
                        out.outputs()
                    );
                }
                final StringColumnMetadata.Plain midMeta;
                try (ColumnTestFiles.Inputs in = ColumnTestFiles.open(dir, "src", segmentId)) {
                    final PlainStringColumnReader reader = (PlainStringColumnReader) StringColumnReader.open(
                        plainOf(sourceMeta),
                        in.inputs()
                    );
                    final PlainRun run = new PlainRun(reader.plainValues(), 0, reader.numValues(), reader.valuesSorted());
                    midMeta = writeAndCheck(dir, "mid", segmentId, middle, options, prefix.length, run, null);
                }
                // Now the copied column is itself the source of a run, placed off its blocks of lengths.
                final BytesRef[][] prefix2 = randomSlots(between(1, 127), maxSlots, nulls, -1);
                final int from = between(0, 200);
                final int to = between(middle.length - 200, middle.length);
                assertTrue(from < to);
                final BytesRef[][] target = concat(prefix2, Arrays.copyOfRange(middle, from, to));
                try (ColumnTestFiles.Inputs in = ColumnTestFiles.open(dir, "mid", segmentId)) {
                    final PlainStringColumnReader reader = (PlainStringColumnReader) StringColumnReader.open(midMeta, in.inputs());
                    final PlainRun run = new PlainRun(
                        reader.plainValues(),
                        reader.firstValueAddress(from),
                        reader.firstValueAddress(to),
                        reader.valuesSorted()
                    );
                    writeAndCheck(dir, "dst", segmentId, target, options(codec), prefix2.length, run, null);
                }
            }
        }
    }

    /** Chunks are only copied into a stream under the codec they were stored with; anything else is written value by value. */
    public void testAnotherCodecIsNotCopied() throws IOException {
        final BytesRef[][] source = randomSlots(2000, 3, true, -1);
        final BytesRef[][] prefix = randomSlots(between(1, 200), 3, true, -1);
        final Written written = copyRun(
            source,
            0,
            source.length,
            prefix,
            new BytesRef[0][],
            options(ChunkCodec.ZSTD),
            options(ChunkCodec.IDENTITY)
        );
        assertEquals(0, written.copies());
    }

    /** A run too short to hold a whole chunk is not worth closing the target's chunk early for. */
    public void testARunHoldingNoWholeChunkIsNotCopied() throws IOException {
        final BytesRef[][] source = randomSlots(500, 1, false, -1);
        final StringColumnOptions options = options(ChunkCodec.ZSTD).withSizes(sizes(64 * 1024));
        final Written written = copyRun(source, 10, 20, randomSlots(10, 1, false, -1), new BytesRef[0][], options, options);
        assertEquals(0, written.copies());
    }

    /**
     * A column in term order stays in term order when the run between two sorted stretches keeps it, and is not
     * when the run starts below the value before it.
     */
    public void testSortedness() throws IOException {
        final BytesRef[][] low = sortedSlots(0, 300);
        final BytesRef[][] middle = sortedSlots(1000, 1500);
        final BytesRef[][] high = sortedSlots(5000, 300);
        final ChunkCodec codec = randomChunkCodec();
        final Written sorted = copyRun(middle, 0, middle.length, low, high, options(codec), options(codec));
        assertEquals(1, sorted.copies());
        assertTrue(sorted.metadata().valuesSorted());

        final Written unsorted = copyRun(middle, 0, middle.length, high, low, options(codec), options(codec));
        assertEquals(1, unsorted.copies());
        assertFalse(unsorted.metadata().valuesSorted());
    }

    /**
     * Writes {@code source}, then a target of {@code prefix}, the source's documents {@code [from, to)} and
     * {@code suffix}, offering the writer those documents as a run. Checks the target reads back slot by slot, and
     * that it describes itself as the same target written value by value does.
     */
    private Written copyRun(
        BytesRef[][] source,
        int from,
        int to,
        BytesRef[][] prefix,
        BytesRef[][] suffix,
        StringColumnOptions sourceOptions,
        StringColumnOptions targetOptions
    ) throws IOException {
        final BytesRef[][] target = concat(prefix, Arrays.copyOfRange(source, from, to), suffix);
        final int runDoc = prefix.length;
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata sourceMeta;
            try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "src", segmentId)) {
                sourceMeta = StringColumnWriter.write(
                    source.length,
                    totals(source),
                    () -> cursor(source),
                    sourceOptions,
                    null,
                    dir,
                    IOContext.DEFAULT,
                    out.outputs()
                );
            }
            try (ColumnTestFiles.Inputs sourceInputs = ColumnTestFiles.open(dir, "src", segmentId)) {
                final PlainStringColumnReader sourceReader = (PlainStringColumnReader) StringColumnReader.open(
                    plainOf(sourceMeta),
                    sourceInputs.inputs()
                );
                // Every document holds a slot, so a document's rank is its id.
                final long runFrom = sourceReader.firstValueAddress(from);
                final long runTo = to == source.length ? sourceReader.numValues() : sourceReader.firstValueAddress(to);
                final PlainRun run = new PlainRun(sourceReader.plainValues(), runFrom, runTo, sourceReader.valuesSorted());

                final CopyCount copies = new CopyCount();
                final StringColumnMetadata.Plain copied = writeAndCheck(dir, "dst", segmentId, target, targetOptions, runDoc, run, copies);
                // The same target, value by value, which is what the copy has to be indistinguishable from.
                final StringColumnMetadata.Plain reference = writeAndCheck(dir, "ref", segmentId, target, targetOptions, -1, null, null);
                assertEquals(reference.numValues(), copied.numValues());
                assertEquals(reference.numNullSlots(), copied.numNullSlots());
                assertEquals(reference.valueBytes(), copied.valueBytes());
                assertEquals(reference.minLength(), copied.minLength());
                assertEquals(reference.maxLength(), copied.maxLength());
                assertEquals(reference.values().constant(), copied.values().constant());
                if (sourceReader.valuesSorted() || reference.valuesSorted() == false) {
                    // Sortedness is only carried over from a source that recorded it; a run in order within a source
                    // that was not is taken as out of order.
                    assertEquals(reference.valuesSorted(), copied.valuesSorted());
                }
                if (copies.copies > 0) {
                    assertThat(copied.values().chunks().numChunks(), greaterThan(0));
                }
                return new Written(copied, copies.copies);
            }
        }
    }

    private StringColumnMetadata.Plain writeAndCheck(
        Directory dir,
        String name,
        byte[] segmentId,
        BytesRef[][] target,
        StringColumnOptions options,
        int runDoc,
        PlainRun run,
        InfoStream infoStream
    ) throws IOException {
        final StringColumnMetadata written;
        try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, name, segmentId)) {
            written = StringColumnWriter.write(
                target.length,
                totals(target),
                () -> withRun(cursor(target), runDoc, run),
                options,
                null,
                dir,
                IOContext.DEFAULT,
                out.outputs(),
                infoStream == null ? InfoStream.NO_OUTPUT : infoStream
            );
        }
        final StringColumnMetadata.Plain plain = plainOf(written);
        try (ColumnTestFiles.Inputs inputs = ColumnTestFiles.open(dir, name, segmentId)) {
            final StringColumnReader reader = StringColumnReader.open(plain, inputs.inputs());
            for (int doc = 0; doc < target.length; doc++) {
                final long first = reader.firstValueAddress(doc);
                assertEquals("slots of document " + doc, target[doc].length, reader.valueCount(doc));
                for (int slot = 0; slot < target[doc].length; slot++) {
                    final BytesRef expected = target[doc][slot];
                    final String where = name + ": document " + doc + ", slot " + slot;
                    assertEquals(where, expected == null, reader.isNullSlot(first + slot));
                    assertEquals(where, expected, reader.valueAt(first + slot));
                    if (expected != null) {
                        assertEquals(where, expected.length, reader.byteLengthAt(first + slot));
                    }
                }
            }
        }
        return plain;
    }

    /** {@code values}, offering {@code run} at the start of document {@code runDoc}. */
    private static StringColumnValues withRun(StringColumnValues values, int runDoc, PlainRun run) {
        return new StringColumnValues() {
            @Override
            public PlainRun plainRun() {
                return values.docID() == runDoc ? run : null;
            }

            @Override
            public int valueCount() {
                return values.valueCount();
            }

            @Override
            public int nullCount() throws IOException {
                return values.nullCount();
            }

            @Override
            public void nextValue() throws IOException {
                values.nextValue();
            }

            @Override
            public BytesRef value() throws IOException {
                return values.value();
            }

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }

            @Override
            public long cost() {
                return values.cost();
            }
        };
    }

    /** A plain column's options: no dictionary to take the column over, small chunks and random block sizes. */
    private static StringColumnOptions options(ChunkCodec codec) {
        return new StringColumnOptions(DictionaryPolicy.NONE, SummaryPolicy.NONE, codec, sizes(randomFrom(64, 256, 1024)));
    }

    private static StringColumnOptions.Sizes sizes(int chunkBytes) {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        final int valuesPerBlock = randomFrom(128, 256);
        return new StringColumnOptions.Sizes(
            valuesPerBlock,
            randomBoolean() ? ChunkBounds.ofBytes(chunkBytes) : new ChunkBounds(chunkBytes, randomFrom(100, 128, 1024)),
            defaults.escapeChunks(),
            defaults.packedOrdinalBlockSize(),
            defaults.compressedOrdinalBlockSize(),
            defaults.escapeRankBlockSize(),
            defaults.slotCountsBlockSize(),
            randomLengthBlockSize(valuesPerBlock)
        );
    }

    /**
     * Documents of one to {@code maxSlots} slots each, drawn from a small pool and often repeating the slot before,
     * so the column stores repeats, and the repeats fall at every offset against its blocks of lengths.
     */
    private static BytesRef[][] randomSlots(int numDocs, int maxSlots, boolean nulls, int constantLength) {
        // At least two values and only the first possibly empty, so a column of any size holds bytes to chunk.
        final BytesRef[] pool = new BytesRef[between(2, 8)];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new BytesRef(randomAlphaOfLength(constantLength >= 0 ? constantLength : between(i == 0 ? 0 : 1, 20)));
        }
        final BytesRef[][] docs = new BytesRef[numDocs][];
        BytesRef previous = pool[0];
        for (int doc = 0; doc < numDocs; doc++) {
            final BytesRef[] slots = new BytesRef[between(1, maxSlots)];
            for (int slot = 0; slot < slots.length; slot++) {
                if (nulls && random().nextInt(8) == 0) {
                    continue;
                }
                previous = random().nextInt(3) == 0 ? previous : randomFrom(pool);
                slots[slot] = previous;
            }
            docs[doc] = slots;
        }
        return docs;
    }

    /** {@code numDocs} single-slot documents in term order, from {@code start} up, each value often held twice. */
    private static BytesRef[][] sortedSlots(int start, int numDocs) {
        final BytesRef[][] docs = new BytesRef[numDocs][];
        int value = start;
        for (int doc = 0; doc < numDocs; doc++) {
            if (randomBoolean()) {
                value++;
            }
            docs[doc] = new BytesRef[] { new BytesRef(String.format(Locale.ROOT, "value-%08d", value)) };
        }
        return docs;
    }

    private static BytesRef[][] concat(BytesRef[][]... parts) {
        int length = 0;
        for (BytesRef[][] part : parts) {
            length += part.length;
        }
        final BytesRef[][] all = new BytesRef[length][];
        int at = 0;
        for (BytesRef[][] part : parts) {
            System.arraycopy(part, 0, all, at, part.length);
            at += part.length;
        }
        return all;
    }
}
