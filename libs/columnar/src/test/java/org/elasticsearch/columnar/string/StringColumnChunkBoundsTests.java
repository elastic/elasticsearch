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
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/** Which bound closes a chunk of which stream, and that a column cut either way reads back the same. */
public class StringColumnChunkBoundsTests extends ColumnarStringTestCase {

    private static final int SMALL_CHUNK = 32;
    private static final int LARGE_CHUNK = 1024 * 1024;

    /** A plain column's values are cut by the plain bound; nothing else it is given cuts them. */
    public void testPlainValuesAreCutByThePlainBound() throws IOException {
        final BytesRef[][] docSlots = uniqueDocSlots(200);
        assertEquals(2, plainChunks(docSlots, ChunkBounds.ofBytes(SMALL_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK)));
        assertEquals(1, plainChunks(docSlots, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(LARGE_CHUNK)));
        assertEquals(1, plainChunks(docSlots, ChunkBounds.ofBytes(LARGE_CHUNK), ChunkBounds.ofBytes(SMALL_CHUNK)));
    }

    /**
     * A value bound cuts a plain column the byte target never would. A chunk ends on a block boundary, so
     * one holds whole blocks: at a bound below the block size every block is its own chunk.
     */
    public void testPlainValuesAreCutByTheValueBound() throws IOException {
        final int values = 1024;
        final int perBlock = 128;
        final BytesRef[][] docSlots = uniqueDocSlots(values);
        for (int maxValues : new int[] { 128, 256, 512, 1024 }) {
            final ChunkBounds bounds = new ChunkBounds(LARGE_CHUNK, maxValues);
            final int perChunk = Math.max(perBlock, maxValues);
            assertEquals(
                "maxValues=" + maxValues,
                (values + perChunk - 1) / perChunk,
                plainChunks(docSlots, bounds, ChunkBounds.ofBytes(LARGE_CHUNK))
            );
        }
    }

    /** Every value still reads back byte for byte when the value bound is what cut the column. */
    public void testAColumnCutByValuesReadsBack() throws IOException {
        final BytesRef[][] docSlots = uniqueDocSlots(1000);
        for (int maxValues : new int[] { 128, 256, 1024 }) {
            for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, StringColumnOptions.DEFAULT_DICTIONARY }) {
                final ChunkBounds bounds = new ChunkBounds(LARGE_CHUNK, maxValues);
                withColumn(docSlots, options(policy, bounds, bounds), (metadata, reader) -> assertValuesReadBack(docSlots, reader));
            }
        }
    }

    /** The values no term names are cut by the escape bound, in bytes and in values alike. */
    public void testEscapesAreCutByTheEscapeBound() throws IOException {
        final BytesRef[][] docSlots = mostlyRepeatedDocSlots(4000, 600);
        assertEquals(1, escapeChunks(docSlots, ChunkBounds.ofBytes(LARGE_CHUNK)));
        assertTrue(
            "a small byte bound cuts the escapes into more than one chunk",
            escapeChunks(docSlots, ChunkBounds.ofBytes(SMALL_CHUNK)) > 1
        );
        assertTrue(
            "a small value bound cuts the escapes into more than one chunk",
            escapeChunks(docSlots, new ChunkBounds(LARGE_CHUNK, 128)) > 1
        );
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
                assertEquals("doc " + doc + " slot " + i, expected[slot], reader.valueAt(addresses[slot]));
                slot++;
            }
        }
        assertEquals("slots read", addresses.length, slot);
        for (int probe = 0; probe < 200; probe++) {
            final int at = between(0, addresses.length - 1);
            assertEquals("random read of slot " + at, expected[at], reader.valueAt(addresses[at]));
        }
    }

    private static BytesRef[][] uniqueDocSlots(int n) {
        final BytesRef[] docValues = new BytesRef[n];
        for (int i = 0; i < n; i++) {
            docValues[i] = new BytesRef("unique-value-padded-to-fill-chunks-" + i);
        }
        return singleValued(docValues);
    }

    /** {@code distinct} terms repeated, plus values seen once that no dictionary would keep. */
    private static BytesRef[][] mostlyRepeatedDocSlots(int n, int distinct) {
        final BytesRef[] docValues = new BytesRef[n];
        for (int i = 0; i < n; i++) {
            docValues[i] = i % 4 == 0 ? new BytesRef("escaping-value-seen-once-" + i) : new BytesRef("dictionary-term-" + (i % distinct));
        }
        return singleValued(docValues);
    }

    private static StringColumnOptions options(DictionaryPolicy policy, ChunkBounds plain, ChunkBounds escapes) {
        return new StringColumnOptions(
            policy,
            ChunkCodec.IDENTITY,
            new StringColumnOptions.Sizes(
                StringColumnOptions.DEFAULT_VALUES_PER_BLOCK,
                plain,
                escapes,
                StringColumnOptions.DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
                StringColumnOptions.DEFAULT_SLOT_COUNTS_BLOCK_SIZE
            )
        );
    }

    private int plainChunks(BytesRef[][] docSlots, ChunkBounds plain, ChunkBounds escapes) throws IOException {
        final int[] chunks = new int[1];
        withColumn(
            docSlots,
            options(DictionaryPolicy.NONE, plain, escapes),
            (metadata, reader) -> chunks[0] = plainOf(metadata).values().chunks().numChunks()
        );
        return chunks[0];
    }

    private int escapeChunks(BytesRef[][] docSlots, ChunkBounds escapes) throws IOException {
        final int[] chunks = new int[1];
        withColumn(docSlots, options(StringColumnOptions.DEFAULT_DICTIONARY, ChunkBounds.ofBytes(LARGE_CHUNK), escapes), (meta, reader) -> {
            final StringColumnMetadata.Dictionary dictionary = dictionaryOf(meta);
            assertTrue("the shape has to escape something to say how the escapes were cut", dictionary.hasEscapes());
            chunks[0] = dictionary.escapes().chunks().numChunks();
        });
        return chunks[0];
    }
}
