/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;

import java.io.IOException;
import java.util.Random;

/** How a dictionary column stores its ordinals, which it decides per column from the ordinals themselves. */
public class OrdinalEncodingTests extends ColumnarStringTestCase {

    private static final int DOCS = 40_000;

    /** Every document carries the same set of terms, so the ordinals are that set over and over. */
    private static BytesRef[][] repeatingSets() {
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            docs[d] = new BytesRef[] { new BytesRef("preprod"), new BytesRef("nginx"), new BytesRef("web") };
        }
        return docs;
    }

    /**
     * One term a document, drawn at random from a vocabulary wide enough that the ordinals hold no
     * repetition and narrow enough that the column still keeps a dictionary.
     */
    private static BytesRef[][] scatteredTerms() {
        final Random r = new Random(7);
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            docs[d] = new BytesRef[] { new BytesRef("term-" + r.nextInt(1500)) };
        }
        return docs;
    }

    /** Most documents do not carry the field at all and the rest hold several, which is the shape the addressing is for. */
    private static BytesRef[][] sparseRepeatingSets() {
        final Random r = new Random(11);
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            if (r.nextInt(100) < 10) {
                docs[d] = new BytesRef[] { new BytesRef("preprod"), new BytesRef("nginx"), new BytesRef("web") };
            }
        }
        return docs;
    }

    private NumericColumnMetadata ordinalsOf(BytesRef[][] docs) throws IOException {
        final NumericColumnMetadata[] found = new NumericColumnMetadata[1];
        withColumn(docs, 1024, ChunkCodec.ZSTD, 64 * 1024, StringColumnOptions.DEFAULT_DICTIONARY, (meta, reader) -> {
            found[0] = dictionaryOf(meta).ordinals();
            // Whatever the ordinals are stored as, the column has to read back what was written.
            for (int d = 0; d < docs.length; d += 997) {
                final long first = reader.firstValueAddress(d);
                assertEquals(docs[d].length, reader.valueCount(d));
                for (int slot = 0; slot < docs[d].length; slot++) {
                    assertEquals(docs[d][slot], reader.valueAt(first + slot));
                }
            }
        });
        return found[0];
    }

    public void testRepeatingOrdinalsAreCompressed() throws IOException {
        final NumericColumnMetadata ordinals = ordinalsOf(repeatingSets());
        assertEquals("repeating ordinals should be compressed", BlockBytesCodec.ZSTD_ID, ordinals.blockBytesCodecId());
        assertEquals("a compressed column takes the larger block", 8192, ordinals.blockSize());
        assertArrayEquals(
            "a compressed column leaves the repetition in the bytes the codec reaches",
            NumericPipeline.compressedOrdinalPipeline(ordinals.blockSize()).transformIds(),
            ordinals.transformIds()
        );
    }

    public void testScatteredOrdinalsAreStoredPacked() throws IOException {
        final NumericColumnMetadata ordinals = ordinalsOf(scatteredTerms());
        assertEquals("ordinals that do not repeat should stay packed", BlockBytesCodec.IDENTITY_ID, ordinals.blockBytesCodecId());
        assertEquals("a packed column keeps the small block", 128, ordinals.blockSize());
        assertArrayEquals(
            "a packed column still sets its runs and outliers aside",
            NumericPipeline.ordinalPipeline(ordinals.blockSize()).transformIds(),
            ordinals.transformIds()
        );
    }

    /**
     * A column only some documents carry still reads back what those hold. The ordinals table no addressing
     * of their own, so the column's own table is the only thing that says where a document's slots are, and
     * a rank counts the documents that carry the field rather than the documents.
     */
    public void testSparseMultiValuedColumnReadsBack() throws IOException {
        final BytesRef[][] docs = sparseRepeatingSets();
        withColumn(docs, 1024, ChunkCodec.ZSTD, 64 * 1024, StringColumnOptions.DEFAULT_DICTIONARY, (meta, reader) -> {
            assertTrue("expected the column to table where a document's slots begin", meta.hasValueAddresses());
            int rank = 0;
            for (BytesRef[] slots : docs) {
                if (slots == null) {
                    continue;
                }
                assertEquals("slot count at rank " + rank, slots.length, reader.valueCount(rank));
                final long first = reader.firstValueAddress(rank);
                for (int slot = 0; slot < slots.length; slot++) {
                    assertEquals(slots[slot], reader.valueAt(first + slot));
                }
                rank++;
            }
            assertEquals("expected every document that carries the field to take a rank", rank, reader.numDocsWithField());
        });
    }

    /**
     * Ordinals that come in long runs, as they do on a segment sorted by the field itself. The run stage
     * stores those about as small as a compressor would, so the column stays packed rather than paying the
     * larger block on every point read.
     */
    public void testRunningOrdinalsAreStoredPacked() throws IOException {
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            docs[d] = new BytesRef[] { new BytesRef("term-" + (d / 500)) };
        }
        final NumericColumnMetadata ordinals = ordinalsOf(docs);
        assertEquals("a column of runs should stay packed", BlockBytesCodec.IDENTITY_ID, ordinals.blockBytesCodecId());
        assertEquals("a packed column keeps the small block", 128, ordinals.blockSize());
    }

    /** Too few ordinals to fill the larger block, so there is nothing for a compressor to work with. */
    public void testShortColumnIsStoredPacked() throws IOException {
        final BytesRef[][] docs = new BytesRef[64][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = new BytesRef[] { new BytesRef("preprod"), new BytesRef("nginx") };
        }
        assertEquals(BlockBytesCodec.IDENTITY_ID, ordinalsOf(docs).blockBytesCodecId());
    }
}
