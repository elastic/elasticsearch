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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * End-to-end round-trip of string columns through a {@link Directory}. Each case asserts the values come back
 * byte-identical and in the exact order they were written, across the value shapes the encoder has to handle:
 * dense and sparse, empty values, wide values, and a spread of value counts.
 */
public class StringColumnTests extends ColumnarStringTestCase {

    public void testEmptyColumn() throws IOException {
        assertColumn(new BytesRef[between(1, 1000)]);
    }

    /** A handful of terms repeated across many documents — the shape a dictionary layout would target. */
    public void testRepeatedValues() throws IOException {
        String[] terms = { "nginx", "apache", "kafka", "elasticsearch" };
        int maxDoc = between(1, 3000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            docs[d] = new BytesRef(randomFrom(terms));
        }
        assertColumn(docs);
    }

    /** Every document a distinct value, so nothing repeats and every value carries its own bytes. */
    public void testAllDistinctValues() throws IOException {
        int maxDoc = between(1, 3000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            docs[d] = new BytesRef("term-" + d);
        }
        assertColumn(docs);
    }

    public void testSparseColumnRepeatedValues() throws IOException {
        int maxDoc = between(100, 4000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.3) {
                docs[d] = new BytesRef(randomFrom("a", "b", "c"));
            }
        }
        assertColumn(docs);
    }

    public void testSparseColumnDistinctValues() throws IOException {
        int maxDoc = between(100, 4000);
        BytesRef[] docs = new BytesRef[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.5) {
                docs[d] = new BytesRef("term-" + d);
            }
        }
        assertColumn(docs);
    }

    /** Empty values are legal and must survive: they encode as a zero length and no bytes. */
    public void testEmptyAndSingleByteValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(200, 600)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef(randomFrom("", "x", "yy"));
        }
        assertColumn(docs);
    }

    /** Every value empty, so every offset in the table is the same and no bytes are written at all. */
    public void testAllEmptyValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(1, 500)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef("");
        }
        assertColumn(docs);
    }

    /**
     * Value counts around the block size, with the block size pinned so the boundaries are hit deterministically
     * rather than depending on the seed: a single short block, an exactly full one, and columns that spill a few
     * values into a partial final block.
     */
    public void testValueCountsAroundBlockBoundaries() throws IOException {
        final int blockSize = ColumNARDocValuesFormat.MIN_BLOCK_SIZE;
        for (int n : new int[] {
            1,
            5,
            blockSize - 1,
            blockSize,
            blockSize + 1,
            blockSize + 2,
            2 * blockSize,
            2 * blockSize + 1,
            3 * blockSize - 1 }) {
            BytesRef[] docs = new BytesRef[n];
            for (int d = 0; d < n; d++) {
                docs[d] = new BytesRef("value-" + d);
            }
            assertColumn(docs, blockSize);
        }
    }

    /**
     * Values read in random order rather than in value order, so the single-block cache misses on most reads and
     * every block gets decoded from scratch — the pattern a query does, as opposed to the sequential scan a merge
     * does.
     */
    public void testRandomAccessAcrossBlocks() throws IOException {
        final int blockSize = ColumNARDocValuesFormat.MIN_BLOCK_SIZE;
        final BytesRef[] docs = new BytesRef[between(4 * blockSize, 6 * blockSize)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef("value-" + d);
        }
        withColumn(docs, blockSize, (metadata, reader) -> {
            assertEquals("recorded block size", blockSize, reader.blockSize());
            for (int i = 0; i < docs.length; i++) {
                // A document's id is its value address here, since the column is dense and single-valued.
                final int doc = between(0, docs.length - 1);
                assertEquals("doc " + doc, docs[doc], reader.valueAt(reader.firstValueAddress(doc)));
            }
        });
    }

    public void testWideValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(50, 300)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef(randomAlphaOfLength(between(200, 2000)));
        }
        assertColumn(docs);
    }

    public void testRandomValues() throws IOException {
        BytesRef[] docs = new BytesRef[between(1, 2000)];
        for (int d = 0; d < docs.length; d++) {
            if (rarely()) {
                continue;
            }
            docs[d] = new BytesRef(randomRealisticUnicodeOfCodepointLength(between(1, 30)));
        }
        assertColumn(docs);
    }

    /**
     * A value larger than the bytes a chunk is meant to hold. A chunk closes only on a block boundary, so it
     * has to grow past its target rather than split the value across two chunks.
     */
    public void testValueLargerThanAChunk() throws IOException {
        final BytesRef[] docs = new BytesRef[between(4, 40)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef("x".repeat(between(600, 1200)));
        }
        assertColumn(docs, randomValidBlockSize(), randomChunkCodec(), 256);
    }

    /**
     * Chunks small enough that one closes every few blocks, so values land either side of a chunk boundary
     * and the block that spans it has to be found in the chunk that holds it.
     */
    public void testValuesAcrossChunkBoundaries() throws IOException {
        final BytesRef[] docs = new BytesRef[between(500, 3000)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef("host-" + d + "." + "svc".repeat(between(1, 6)));
        }
        assertColumn(docs, randomValidBlockSize(), randomChunkCodec(), randomFrom(64, 128, 512));
    }

    /**
     * The same values under both codecs. A chunk stored verbatim is served straight from the mapped file
     * while a compressed one is decoded into a buffer, so the two take different paths to the same bytes.
     */
    public void testCodecsAgree() throws IOException {
        final BytesRef[] docs = new BytesRef[between(200, 2000)];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = randomBoolean() ? new BytesRef("") : new BytesRef(randomAlphaOfLengthBetween(1, 80));
        }
        final int blockSize = randomValidBlockSize();
        final int chunkBytes = randomFrom(128, 4096);
        assertColumn(docs, blockSize, ChunkCodec.IDENTITY, chunkBytes);
        assertColumn(docs, blockSize, ChunkCodec.ZSTD, chunkBytes);
    }

    /**
     * Values short enough that a block keeps their lengths beside them, and long enough that it packs the
     * lengths at its head instead. Both layouts have to read back the same, including where a column mixes
     * them and the choice differs from block to block.
     */
    public void testShortAndLongValueLayouts() throws IOException {
        final BytesRef[] shortValues = new BytesRef[between(300, 1500)];
        for (int d = 0; d < shortValues.length; d++) {
            shortValues[d] = new BytesRef(randomAlphaOfLengthBetween(1, 8));
        }
        assertColumn(shortValues);

        // Packed at each of the widths a length can take: one byte below 256, two below 65536, four above.
        for (int[] range : new int[][] { { 32, 255 }, { 256, 900 }, { 66_000, 66_200 } }) {
            final BytesRef[] longValues = new BytesRef[range[0] > 1000 ? between(4, 20) : between(300, 1500)];
            for (int d = 0; d < longValues.length; d++) {
                longValues[d] = new BytesRef(randomAlphaOfLengthBetween(range[0], range[1]));
            }
            assertColumn(longValues);
        }

        final BytesRef[] mixed = new BytesRef[between(300, 1500)];
        for (int d = 0; d < mixed.length; d++) {
            mixed[d] = new BytesRef(randomAlphaOfLengthBetween(1, 8));
        }
        // A run of long values in the middle, so the blocks covering it choose differently from the rest.
        for (int d = mixed.length / 3; d < Math.min(mixed.length, 2 * mixed.length / 3); d++) {
            mixed[d] = new BytesRef(randomAlphaOfLengthBetween(200, 400));
        }
        assertColumn(mixed);
    }

    /** A spread of slot counts per document, which is what puts a value-address table on the column at all. */
    public void testMultiValuedColumn() throws IOException {
        assertSlots(randomDocSlots(between(200, 2000), 8, false, false));
    }

    /** Multi-valued and sparse at once: a document may hold several slots, or none at all. */
    public void testSparseMultiValuedColumn() throws IOException {
        assertSlots(randomDocSlots(between(200, 2000), 6, true, false));
    }

    /** Null slots interleaved with values, including beside the empty string the length bias exists to separate. */
    public void testNullSlots() throws IOException {
        assertSlots(randomDocSlots(between(200, 2000), 6, randomBoolean(), true));

        final BytesRef empty = new BytesRef("");
        final BytesRef[][] beside = new BytesRef[between(50, 400)][];
        for (int d = 0; d < beside.length; d++) {
            beside[d] = new BytesRef[] { null, empty, new BytesRef("v" + d), empty, null };
        }
        assertSlots(beside);
    }

    /** A document holding nothing but nulls save one value — the least a document can carry and still be written. */
    public void testAlmostAllNullDocument() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(20, 200)][];
        for (int d = 0; d < docSlots.length; d++) {
            final BytesRef[] slots = new BytesRef[between(2, 12)];
            slots[between(0, slots.length - 1)] = new BytesRef("only-" + d);
            docSlots[d] = slots;
        }
        assertSlots(docSlots);
    }

    /**
     * A document whose slots straddle a block boundary, and one holding more slots than a whole block, with the
     * block size pinned so both happen rather than depending on the seed.
     */
    public void testDocumentSlotsAcrossBlockBoundaries() throws IOException {
        final int blockSize = ColumNARDocValuesFormat.MIN_BLOCK_SIZE;
        for (int slotsInBigDoc : new int[] { blockSize - 1, blockSize, blockSize + 1, 3 * blockSize + 7 }) {
            final BytesRef[][] docSlots = new BytesRef[between(3, 12)][];
            for (int d = 0; d < docSlots.length; d++) {
                docSlots[d] = new BytesRef[] { new BytesRef("small-" + d) };
            }
            final BytesRef[] big = new BytesRef[slotsInBigDoc];
            for (int s = 0; s < big.length; s++) {
                big[s] = new BytesRef("big-" + s);
            }
            docSlots[between(0, docSlots.length - 1)] = big;
            withColumn(docSlots, blockSize, (metadata, reader) -> assertSlots(docSlots, metadata, reader));
        }
    }

    /** Writes {@code docSlots} as a string column, reads it back, and asserts every slot round-trips in order. */
    private void assertSlots(BytesRef[][] docSlots) throws IOException {
        withColumn(docSlots, (metadata, reader) -> assertSlots(docSlots, metadata, reader));
    }

    private static void assertSlots(BytesRef[][] docSlots, StringColumnMetadata metadata, StringColumnReader reader) throws IOException {
        assertEquals("recorded layout", StringColumnLayout.PLAIN, metadata.layout());
        assertEquals("numValues", numValues(docSlots), reader.numValues());
        assertEquals("numNullSlots", numNullSlots(docSlots), metadata.numNullSlots());
        assertEquals("multi-valued", numValues(docSlots) > numDocsWithField(docSlots), metadata.multiValued());

        int seenDocs = 0;
        ColumnIterator iterator = reader.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            final int rank = iterator.rank();
            final BytesRef[] expected = docSlots[doc];
            assertEquals("slot count at doc " + doc, expected.length, reader.valueCount(rank));
            final long first = reader.firstValueAddress(rank);
            for (int slot = 0; slot < expected.length; slot++) {
                final long address = first + slot;
                if (expected[slot] == null) {
                    assertTrue("doc " + doc + " slot " + slot + " is null", reader.isNullSlot(address));
                } else {
                    assertFalse("doc " + doc + " slot " + slot + " is a value", reader.isNullSlot(address));
                    assertEquals("doc " + doc + " slot " + slot, expected[slot], reader.valueAt(address));
                }
            }
            seenDocs++;
        }
        assertEquals("documents with a value", numDocsWithField(docSlots), seenDocs);
    }

    /** Writes {@code docValues} as a string column, reads it back, and asserts every value round-trips in order. */
    private void assertColumn(BytesRef[] docValues) throws IOException {
        assertColumn(docValues, randomValidBlockSize());
    }

    private void assertColumn(BytesRef[] docValues, int blockSize) throws IOException {
        assertColumn(docValues, blockSize, randomChunkCodec(), randomTargetChunkBytes());
    }

    private void assertColumn(BytesRef[] docValues, int blockSize, ChunkCodec chunkCodec, int targetChunkBytes) throws IOException {
        final int numDocsWithField = numDocsWithField(docValues);
        withColumn(docValues, blockSize, chunkCodec, targetChunkBytes, (metadata, reader) -> {
            assertFalse("one value per document", metadata.multiValued());
            assertFalse("no null slots", metadata.hasNullSlots());
            assertEquals("recorded layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEquals("numValues", numDocsWithField, reader.numValues());

            int seenDocs = 0;
            ColumnIterator iterator = reader.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                int index = iterator.rank();
                assertEquals("value count at doc " + doc, 1, reader.valueCount(index));
                BytesRef actual = reader.valueAt(reader.firstValueAddress(index));
                assertEquals("doc " + doc, docValues[doc], actual);
                seenDocs++;
            }
            assertEquals("documents with a value", numDocsWithField, seenDocs);
        });
    }
}
