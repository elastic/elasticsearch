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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * A dictionary column stages its escapes in a temporary file, created on the first of them, and nothing else:
 * its ordinals stream into the data as the values are walked, and every table and every other stream goes
 * straight into its file. So a column without escapes creates no file at all; one with escapes can fail partway
 * through, and the file it opened before the failure is still a file to delete.
 *
 * <p>The ordinals are held back only until the trial that chooses their shape has what it needs, then replayed
 * into the stream the trial chose, so the columns here are sized around that trial: fewer ordinals than it needs,
 * exactly as many, and more, in every shape the pass distinguishes.
 */
public class StringColumnTempFileTests extends ColumnarStringTestCase {

    private static final DictionaryPolicy ROOMY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);
    /** Admits any column with a term worth naming, so a column shaped to escape or not does as it is shaped. */
    private static final DictionaryPolicy FORCED = new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE);
    private static final String COLUMN_FILES = "column";

    /** Writes {@code docSlots} as a dictionary column into {@code dir}. */
    private void write(Directory dir, BytesRef[][] docSlots) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, COLUMN_FILES, segmentId)) {
            StringColumnWriter.write(
                docSlots.length,
                totals(docSlots),
                () -> cursor(docSlots),
                new StringColumnOptions(
                    ROOMY,
                    StringColumnOptions.DEFAULT_SUMMARY,
                    randomChunkCodec(),
                    new StringColumnOptions.Sizes(
                        randomValidBlockSize(),
                        ChunkBounds.ofBytes(randomTargetChunkBytes()),
                        ChunkBounds.ofBytes(randomTargetChunkBytes()),
                        StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
                        StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
                        StringColumnOptions.DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
                        StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE,
                        ColumNARDocValuesFormat.MAX_BLOCK_SIZE
                    ),
                    StringColumnOptions.DEFAULT_SINGLE_VALUED,
                    StringColumnOptions.DEFAULT_ENABLE_SKIP_INDEX
                ),
                null,
                null,
                dir,
                IOContext.DEFAULT,
                out.outputs(),
                out.skipIndex()
            );
        }
    }

    /** Fails the nth temporary file a column asks for, and counts what it was asked for. */
    private static final class FailsNthTempOutput extends FilterDirectory {
        private final int failAt;
        int opened;
        final Set<String> suffixes = new HashSet<>();

        FailsNthTempOutput(Directory in, int failAt) {
            super(in);
            this.failAt = failAt;
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            if (++opened == failAt) {
                throw new IOException("no more temporary files for [" + prefix + "." + suffix + "]");
            }
            suffixes.add(suffix);
            return super.createTempOutput(prefix, suffix, context);
        }
    }

    /**
     * A column with escapes asks for one temporary file, for them, and when opening it fails the write fails
     * with nothing left behind. A column whose documents hold several slots writes its counts as well, straight
     * into the addressing, so both shapes are put through it.
     */
    public void testTempFilesAreDeletedWhenWritingFails() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            final BytesRef[][] docSlots = column(multiValued, true);
            // How many temporary files a write that succeeds asks for, so every one of them can be failed.
            final int total;
            try (Directory real = newDirectory()) {
                final FailsNthTempOutput counting = new FailsNthTempOutput(real, -1);
                write(counting, docSlots);
                total = counting.opened;
                assertEquals(
                    "the only temporary file, " + (multiValued ? "multi-valued" : "single-valued"),
                    Set.of("columnar-escapes"),
                    counting.suffixes
                );
                assertNoTempFiles(real, "after a write that succeeded");
            }
            assertEquals("a dictionary column with escapes stages them, and nothing else", 1, total);

            // Every temporary file the write asks for, failed in turn.
            for (int failAt = 1; failAt <= total; failAt++) {
                final Directory real = newDirectory();
                try {
                    final Directory dir = new FailsNthTempOutput(real, failAt);
                    boolean failed = false;
                    try {
                        write(dir, docSlots);
                    } catch (IOException e) {
                        failed = true;
                    }
                    assertTrue("failing temporary file " + failAt + " of " + total + " did not fail the write", failed);
                    assertNoTempFiles(
                        real,
                        "when the write failed at temporary file " + failAt + (multiValued ? " (multi-valued)" : " (single-valued)")
                    );
                } finally {
                    try {
                        real.close();
                    } catch (RuntimeException e) {
                        throw new AssertionError("failAt=" + failAt + ": " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * A write that fails after the escapes' file is open — here, on the file itself — still deletes it. The
     * failure comes from the file's own output, which is where a disk that fills up would surface.
     */
    public void testEscapesFileIsDeletedWhenWritingItFails() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            final BytesRef[][] docSlots = column(multiValued, true);
            try (Directory real = newDirectory()) {
                final Directory dir = new FilterDirectory(real) {
                    @Override
                    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
                        final IndexOutput out = super.createTempOutput(prefix, suffix, context);
                        return new FilterIndexOutput("failing " + out, out.getName(), out) {
                            @Override
                            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                                throw new IOException("no room for [" + getName() + "]");
                            }
                        };
                    }
                };
                final IOException failure = expectThrows(IOException.class, () -> write(dir, docSlots));
                assertTrue(failure.getMessage(), failure.getMessage().startsWith("no room for"));
                assertNoTempFiles(real, "when writing the escapes failed");
            }
        }
    }

    /** A column none of whose values escape asks the directory for nothing. */
    public void testAColumnWithoutEscapesStagesNothing() throws IOException {
        for (boolean multiValued : new boolean[] { false, true }) {
            final BytesRef[][] docSlots = column(multiValued, false);
            try (Directory real = newDirectory()) {
                final FailsNthTempOutput counting = new FailsNthTempOutput(real, -1);
                write(counting, docSlots);
                assertEquals("temporary files asked for, " + (multiValued ? "multi-valued" : "single-valued"), 0, counting.opened);
                assertEquals(Set.of(), counting.suffixes);
                assertNoTempFiles(real, "after a write without escapes");
            }
        }
    }

    /**
     * Every value reads back whatever shape the column took around the trial that chooses how its ordinals are
     * stored: fewer values than the trial holds back, exactly that many, and more; ordinals that repeat enough to
     * compress and ones that do not; with and without escapes and nulls; in term order and out of it; one slot a
     * document and several; with the skip index enabled and disabled.
     */
    public void testColumnReadsBackAroundTheTrial() throws IOException {
        final int compressedBlockSize = randomFrom(1024, StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE);
        final int trialValues = 2 * compressedBlockSize;
        final int[] valueCounts = { trialValues - 1, trialValues, trialValues + 1, 3 * trialValues + between(1, 100) };
        for (int values : valueCounts) {
            for (boolean repetitive : new boolean[] { false, true }) {
                for (boolean escapes : new boolean[] { false, true }) {
                    for (boolean nulls : new boolean[] { false, true }) {
                        for (boolean sorted : new boolean[] { false, true }) {
                            for (boolean multiValued : new boolean[] { false, true }) {
                                for (boolean skipIndex : new boolean[] { false, true }) {
                                    final String shape = String.format(
                                        Locale.ROOT,
                                        "values=%d repetitive=%s escapes=%s nulls=%s sorted=%s multiValued=%s skipIndex=%s",
                                        values,
                                        repetitive,
                                        escapes,
                                        nulls,
                                        sorted,
                                        multiValued,
                                        skipIndex
                                    );
                                    final BytesRef[][] docSlots = shaped(values, repetitive, escapes, nulls, sorted, multiValued);
                                    assertReadsBack(
                                        shape,
                                        docSlots,
                                        trialValues,
                                        compressedBlockSize,
                                        repetitive,
                                        escapes,
                                        nulls,
                                        sorted,
                                        skipIndex
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void assertReadsBack(
        String shape,
        BytesRef[][] docSlots,
        int trialValues,
        int compressedBlockSize,
        boolean repetitive,
        boolean escapes,
        boolean nulls,
        boolean sorted,
        boolean skipIndex
    ) throws IOException {
        final StringColumnOptions.Sizes sizes = new StringColumnOptions.Sizes(
            randomValidBlockSize(),
            randomChunkBounds(randomTargetChunkBytes()),
            randomChunkBounds(randomTargetChunkBytes()),
            StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
            compressedBlockSize,
            StringColumnOptions.DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
            StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE,
            ColumNARDocValuesFormat.MAX_BLOCK_SIZE
        );
        final StringColumnOptions options = new StringColumnOptions(
            FORCED,
            StringColumnOptions.DEFAULT_SUMMARY,
            randomChunkCodec(),
            sizes,
            StringColumnOptions.DEFAULT_SINGLE_VALUED,
            skipIndex
        );
        final long numValues = numValues(docSlots);
        withColumn(docSlots, options, (metadata, reader) -> {
            final StringColumnMetadata.Dictionary dictionary = dictionaryOf(metadata);
            assertEquals(shape, escapes, dictionary.hasEscapes());
            final NumericColumnMetadata ordinals = dictionary.ordinals();
            assertEquals(shape, numValues, ordinals.numValues());
            assertEquals(shape, numDocsWithField(docSlots), ordinals.numDocsWithField());
            assertTrue(shape + ": the ordinals are dense over the documents walked", ordinals.iterator().isDense());
            assertFalse(shape + ": the ordinals table no addresses", ordinals.hasValueAddresses());
            if (numValues < trialValues) {
                assertEquals(
                    shape + ": too few ordinals for the trial stay packed",
                    BlockBytesCodec.IDENTITY_ID,
                    ordinals.blockBytesCodecId()
                );
            }
            if (escapes == false && nulls == false && sorted == false) {
                // The shapes whose choice is settled; OrdinalEncodingTests covers the choice itself.
                assertEquals(
                    shape,
                    repetitive && numValues >= trialValues ? BlockBytesCodec.ZSTD_ID : BlockBytesCodec.IDENTITY_ID,
                    ordinals.blockBytesCodecId()
                );
            }
            // A skipper only where there is a file for it, nothing escapes, and the reader does not bisect.
            final boolean bisects = metadata.valuesSorted() && metadata.hasValueAddresses() == false;
            assertEquals(shape, skipIndex && escapes == false && bisects == false, ordinals.skipper() != null);
            if (ordinals.skipper() != null) {
                // The skipper counts a document on its first value, so one holding no slot is not among them.
                int withAValue = 0;
                for (BytesRef[] slots : docSlots) {
                    if (slots != null && slots.length > 0) {
                        withAValue++;
                    }
                }
                assertEquals(shape, withAValue, ordinals.skipper().docCount());
            }
            int rank = 0;
            for (BytesRef[] slots : docSlots) {
                if (slots == null) {
                    continue;
                }
                assertEquals(shape + " slot count at rank " + rank, slots.length, reader.valueCount(rank));
                final long first = reader.firstValueAddress(rank);
                for (int slot = 0; slot < slots.length; slot++) {
                    assertEquals(shape + " at rank " + rank + " slot " + slot, slots[slot], reader.valueAt(first + slot));
                }
                rank++;
            }
            assertEquals(shape, rank, reader.numDocsWithField());
        });
    }

    /**
     * A column of {@code values} slots in the shape asked for. Repetitive ordinals cycle through a handful of
     * terms; scattered ones are drawn at random from enough terms that a compressor finds nothing to take out
     * of them. An escape is a value seen once,
     * which no dictionary names; a null is a null slot. Sorted means the terms rise through the column, which
     * is what lets a reader bisect a single-valued column instead of scanning its ordinals.
     */
    private static BytesRef[][] shaped(
        int values,
        boolean repetitive,
        boolean escapes,
        boolean nulls,
        boolean sorted,
        boolean multiValued
    ) {
        final Random r = new Random(values * 31L + (repetitive ? 1 : 0) + (escapes ? 2 : 0) + (nulls ? 4 : 0) + (sorted ? 8 : 0));
        final int terms = repetitive ? 3 : 100;
        final BytesRef[] slots = new BytesRef[values];
        for (int i = 0; i < values; i++) {
            // A term has to be seen twice to be named, so the first slots deal every term out three times:
            // enough that the escapes and nulls put over them below leave each of them seen twice.
            final int term = sorted ? (int) ((long) i * terms / values) : (repetitive || i < 3 * terms ? i % terms : r.nextInt(terms));
            slots[i] = new BytesRef(String.format(Locale.ROOT, "term-%05d", term));
        }
        if (escapes) {
            // Spaced so the trial sees some and the rest of the column sees some, whatever the count.
            for (int i = 37; i < values; i += 211) {
                slots[i] = new BytesRef("escaped-" + i);
            }
        }
        if (nulls) {
            for (int i = 5; i < values; i += 97) {
                slots[i] = null;
            }
        }
        if (multiValued == false) {
            final BytesRef[][] docSlots = new BytesRef[values][];
            for (int i = 0; i < values; i++) {
                docSlots[i] = new BytesRef[] { slots[i] };
            }
            return docSlots;
        }
        // Documents of none to four slots, so the trial's boundary falls inside a document as often as not, and
        // a document that holds a slot count of nothing is walked like any other.
        final BytesRef[][] docSlots = new BytesRef[2 * values][];
        int docs = 0;
        for (int at = 0; at < values;) {
            final int count = Math.min(values - at, r.nextInt(5));
            final BytesRef[] doc = new BytesRef[count];
            System.arraycopy(slots, at, doc, 0, count);
            docSlots[docs++] = doc;
            at += count;
        }
        final BytesRef[][] trimmed = new BytesRef[docs][];
        System.arraycopy(docSlots, 0, trimmed, 0, docs);
        return trimmed;
    }

    private static void assertNoTempFiles(Directory dir, String when) throws IOException {
        for (String name : dir.listAll()) {
            assertFalse("temporary file [" + name + "] left behind " + when, name.contains("columnar-"));
        }
    }

    /**
     * A dictionary column, with escapes among its values when asked for. When {@code multiValued}, its
     * documents hold differing numbers of slots, so the column writes slot counts too, straight into the
     * addressing.
     */
    private BytesRef[][] column(boolean multiValued, boolean escapes) {
        final String[] terms = { "alpha", "bravo", "charlie" };
        final int docs = between(400, 1200);
        final BytesRef[][] docSlots = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            final int slots = multiValued ? d % 4 : 1;
            final BytesRef[] values = new BytesRef[slots];
            for (int i = 0; i < slots; i++) {
                values[i] = escapes && (d + i) % 9 == 4
                    ? new BytesRef("escaped-" + d + "-" + i)
                    : new BytesRef(terms[(d + i) % terms.length]);
            }
            docSlots[d] = values;
        }
        return docSlots;
    }
}
