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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.hamcrest.Matchers.greaterThan;

/** Which bound closes a chunk of which stream, and that a column cut either way reads back the same. */
public class StringColumnChunkBoundsTests extends ColumnarStringTestCase {

    private static final int SMALL_CHUNK = 32;
    private static final int LARGE_CHUNK = 1024 * 1024;

    /** A plain column's values are cut by the plain bound; nothing else it is given cuts them. */
    public void testPlainValuesAreCutByThePlainBound() throws IOException {
        final BytesRef[][] docSlots = mixedDocSlots(200);
        // Small enough that the column is several blocks: a chunk closes only on a block boundary, so a
        // column of one block is one chunk whatever it is told.
        final int blockSize = randomFrom(128, 256);
        assertEquals(
            plainChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK)),
            plainChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(SMALL_CHUNK))
        );
        assertThat(
            "a small byte target cuts the values into more chunks than a large one",
            plainChunks(docSlots, blockSize, ChunkBounds.ofBytes(SMALL_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK)),
            greaterThan(plainChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK)))
        );
    }

    /**
     * A value bound cuts a plain column the byte target never would. A chunk ends on a block boundary, so it
     * takes whole blocks until it holds at least the bound, which is where a bound the block size does not
     * divide ends up past it rather than on it.
     */
    public void testPlainValuesAreCutByTheValueBound() throws IOException {
        final BytesRef[][] docSlots = mixedDocSlots(400);
        final long values = numValues(docSlots);
        for (int blockSize : new int[] { 128, 256 }) {
            for (int maxValues : new int[] { 1, 100, 128, 200, 512, 1000 }) {
                final int chunks = plainChunks(
                    docSlots,
                    blockSize,
                    new ChunkBounds(LARGE_CHUNK, maxValues),
                    ChunkBounds.ofBytes(LARGE_CHUNK)
                );
                assertEquals("blockSize=" + blockSize + " maxValues=" + maxValues, expectedChunks(values, blockSize, maxValues), chunks);
            }
        }
    }

    /** Every value still reads back byte for byte when the value bound is what cut the column. */
    public void testAColumnCutByValuesReadsBack() throws IOException {
        final BytesRef[][] docSlots = mixedDocSlots(1000);
        for (int maxValues : new int[] { 1, 100, 128, 1024 }) {
            for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, StringColumnOptions.DEFAULT_DICTIONARY }) {
                final ChunkBounds bounds = new ChunkBounds(LARGE_CHUNK, maxValues);
                withColumn(
                    docSlots,
                    options(policy, randomValidBlockSize(), bounds, bounds),
                    (metadata, reader) -> assertValuesReadBack(docSlots, reader)
                );
            }
        }
    }

    /** The values no term names are cut by the escape bound, in bytes and in values alike. */
    public void testEscapesAreCutByTheEscapeBound() throws IOException {
        final BytesRef[][] docSlots = mostlyRepeatedDocSlots(4000, 600);
        final int blockSize = 128;
        assertEscapeChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), Integer.MAX_VALUE);
        // Every escaped value is longer than the byte target, so a block of them is always past it and each
        // block ends up in a chunk of its own — the same cut a value bound of one block would make.
        assertEscapeChunks(docSlots, blockSize, ChunkBounds.ofBytes(SMALL_CHUNK), blockSize);
        assertEscapeChunks(docSlots, blockSize, new ChunkBounds(LARGE_CHUNK, blockSize), blockSize);
        assertEscapeChunks(docSlots, blockSize, new ChunkBounds(LARGE_CHUNK, 3 * blockSize), 3 * blockSize);
    }

    /**
     * The dictionary's own terms are cut by neither of the bounds a caller chooses. They are stored as they
     * are and read where each one lies, so nothing about how the values are cut reaches them.
     */
    public void testTheTermsAreCutByNeitherBound() throws IOException {
        final BytesRef[][] docSlots = mostlyRepeatedDocSlots(4000, 600);
        final int blockSize = randomFrom(128, 256);
        final int loose = termChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK));
        assertEquals(
            "a small plain bound does not reach the terms",
            loose,
            termChunks(docSlots, blockSize, new ChunkBounds(SMALL_CHUNK, 1), ChunkBounds.ofBytes(LARGE_CHUNK))
        );
        assertEquals(
            "a small escape bound does not reach the terms",
            loose,
            termChunks(docSlots, blockSize, ChunkBounds.ofBytes(LARGE_CHUNK), new ChunkBounds(SMALL_CHUNK, 1))
        );
    }

    /** Blocks of {@code blockSize} taken whole until a chunk holds at least {@code maxValues} of them. */
    private static int expectedChunks(long values, int blockSize, int maxValues) {
        final long blocks = (values + blockSize - 1) / blockSize;
        final long blocksPerChunk = maxValues == Integer.MAX_VALUE ? blocks : (maxValues + blockSize - 1L) / blockSize;
        return Math.toIntExact((blocks + blocksPerChunk - 1) / blocksPerChunk);
    }

    /** Every slot of every document, read forwards and then at random so no read leans on the one before it. */
    private static void assertValuesReadBack(BytesRef[][] docSlots, StringColumnReader reader) throws IOException {
        final long[] addresses = new long[(int) numValues(docSlots)];
        final BytesRef[] expected = new BytesRef[addresses.length];
        int slot = 0;
        final ColumnIterator iterator = reader.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            final long first = reader.firstValueAddress(iterator.rank());
            for (int i = 0; i < docSlots[doc].length; i++) {
                addresses[slot] = first + i;
                expected[slot] = docSlots[doc][i];
                assertSlot(reader, addresses[slot], expected[slot], "doc " + doc + " slot " + i);
                slot++;
            }
        }
        assertEquals("slots read", addresses.length, slot);
        for (int probe = 0; probe < 200; probe++) {
            final int at = between(0, addresses.length - 1);
            assertSlot(reader, addresses[at], expected[at], "random read of slot " + at);
        }
    }

    private static void assertSlot(StringColumnReader reader, long address, BytesRef expected, String what) throws IOException {
        if (expected == null) {
            assertTrue(what + " is null", reader.isNullSlot(address));
        } else {
            assertFalse(what + " is a value", reader.isNullSlot(address));
            assertEquals(what, expected, reader.valueAt(address));
        }
    }

    /** Documents holding one value, several values, a null among them, and nothing but a null. */
    private static BytesRef[][] mixedDocSlots(int numDocs) {
        final BytesRef[][] docSlots = new BytesRef[numDocs][];
        for (int doc = 0; doc < numDocs; doc++) {
            docSlots[doc] = switch (doc % 4) {
                case 0 -> new BytesRef[] { new BytesRef("unique-value-padded-to-fill-chunks-" + doc) };
                case 1 -> new BytesRef[] {
                    new BytesRef("first-of-doc-" + doc),
                    new BytesRef("second-of-doc-" + doc),
                    new BytesRef("third-of-doc-" + doc) };
                case 2 -> new BytesRef[] { new BytesRef("before-null-" + doc), null, new BytesRef("after-null-" + doc) };
                default -> new BytesRef[] { null };
            };
        }
        return docSlots;
    }

    /**
     * {@code distinct} terms repeated, plus values seen once that no dictionary would keep, over documents
     * holding one slot, several, and a null among them.
     */
    private static BytesRef[][] mostlyRepeatedDocSlots(int numDocs, int distinct) {
        final BytesRef[][] docSlots = new BytesRef[numDocs][];
        for (int doc = 0; doc < numDocs; doc++) {
            final BytesRef repeated = new BytesRef("dictionary-term-padded-to-carry-the-column-" + (doc % distinct));
            final BytesRef other = new BytesRef("dictionary-term-padded-to-carry-the-column-" + ((doc + 1) % distinct));
            // Longer than the small byte target, so a block of them is always past it.
            final BytesRef once = new BytesRef("escaping-value-seen-once-and-longer-than-a-small-chunk-" + doc);
            docSlots[doc] = switch (doc % 4) {
                case 0 -> new BytesRef[] { repeated };
                case 1 -> new BytesRef[] { repeated, other };
                case 2 -> new BytesRef[] { repeated, null };
                default -> new BytesRef[] { repeated, once };
            };
        }
        return docSlots;
    }

    private static StringColumnOptions options(DictionaryPolicy policy, int valuesPerBlock, ChunkBounds plain, ChunkBounds escapes) {
        return new StringColumnOptions(
            policy,
            // What cuts a chunk is the same whatever compresses it, so the codec is free to vary.
            randomChunkCodec(),
            new StringColumnOptions.Sizes(
                valuesPerBlock,
                plain,
                escapes,
                StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE
            )
        );
    }

    private int plainChunks(BytesRef[][] docSlots, int valuesPerBlock, ChunkBounds plain, ChunkBounds escapes) throws IOException {
        final int[] chunks = new int[1];
        withColumn(
            docSlots,
            options(DictionaryPolicy.NONE, valuesPerBlock, plain, escapes),
            (metadata, reader) -> chunks[0] = plainOf(metadata).values().chunks().numChunks()
        );
        return chunks[0];
    }

    private int termChunks(BytesRef[][] docSlots, int valuesPerBlock, ChunkBounds plain, ChunkBounds escapes) throws IOException {
        final int[] chunks = new int[1];
        withColumn(
            docSlots,
            options(StringColumnOptions.DEFAULT_DICTIONARY, valuesPerBlock, plain, escapes),
            (metadata, reader) -> chunks[0] = dictionaryOf(metadata).dictionary().chunks().numChunks()
        );
        return chunks[0];
    }

    /** Asserts the escapes were cut into the chunks {@code maxValues} calls for, over however many escaped. */
    private void assertEscapeChunks(BytesRef[][] docSlots, int valuesPerBlock, ChunkBounds escapes, int maxValues) throws IOException {
        withColumn(
            docSlots,
            options(StringColumnOptions.DEFAULT_DICTIONARY, valuesPerBlock, ChunkBounds.ofBytes(LARGE_CHUNK), escapes),
            (metadata, reader) -> {
                final StringColumnMetadata.Dictionary dictionary = dictionaryOf(metadata);
                assertTrue("the shape has to escape something to say how the escapes were cut", dictionary.hasEscapes());
                assertEquals(
                    "escape chunks under " + escapes,
                    expectedChunks(dictionary.escapes().numValues(), valuesPerBlock, maxValues),
                    dictionary.escapes().chunks().numChunks()
                );
            }
        );
    }
}
