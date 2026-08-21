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

    /** Writes {@code docValues} as a string column, reads it back, and asserts every value round-trips in order. */
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

        final BytesRef[] longValues = new BytesRef[between(300, 1500)];
        for (int d = 0; d < longValues.length; d++) {
            longValues[d] = new BytesRef(randomAlphaOfLengthBetween(200, 400));
        }
        assertColumn(longValues);

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

    private void assertColumn(BytesRef[] docValues) throws IOException {
        assertColumn(docValues, randomValidBlockSize());
    }

    private void assertColumn(BytesRef[] docValues, int blockSize) throws IOException {
        assertColumn(docValues, blockSize, randomChunkCodec(), randomTargetChunkBytes());
    }

    private void assertColumn(BytesRef[] docValues, int blockSize, ChunkCodec chunkCodec, int targetChunkBytes) throws IOException {
        final int numDocsWithField = numDocsWithField(docValues);
        withColumn(docValues, blockSize, chunkCodec, targetChunkBytes, (metadata, reader) -> {
            assertFalse("string columns are single-valued for now", metadata.multiValued());
            assertEquals("recorded layout", StringColumnLayout.PLAIN, metadata.layout());
            assertEquals("numValues", numDocsWithField, reader.numValues());

            int seenDocs = 0;
            ColumnIterator iterator = reader.iterator();
            for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                int index = iterator.index();
                assertEquals("value count at doc " + doc, 1, reader.valueCount(index));
                BytesRef actual = reader.valueAt(reader.firstValueAddress(index));
                assertEquals("doc " + doc, docValues[doc], actual);
                seenDocs++;
            }
            assertEquals("documents with a value", numDocsWithField, seenDocs);
        });
    }
}
