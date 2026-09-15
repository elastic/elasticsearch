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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Where a document's slots begin, over a column of more blocks of counts than one. The addresses inside a
 * block are summed from its base rather than stored, so what matters is that a rank answers the same
 * wherever the read arrives from and whichever block it was last in.
 */
public class SlotAddressingTests extends ColumnarStringTestCase {

    /** Enough documents for several blocks of counts, and a boundary that is not the last one. */
    private static final int DOCS = 5 * AddressingWriter.COUNTS_BLOCK_SIZE + 37;

    /**
     * Slot counts that vary the way a real column's do: documents holding nothing, documents holding one,
     * and a few holding many, so no block packs to the same width and none is a single run.
     */
    private static BytesRef[][] varyingSlots() {
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            final int slots = switch (d % 7) {
                case 0 -> 0;
                case 1, 2, 3 -> 1;
                case 4 -> 2;
                case 5 -> 9;
                default -> 3;
            };
            docs[d] = new BytesRef[slots];
            for (int s = 0; s < slots; s++) {
                docs[d][s] = new BytesRef("d" + d + "s" + s);
            }
        }
        return docs;
    }

    /** The address each document's slots begin at, which is every count before it. */
    private static long[] expectedAddresses(BytesRef[][] docs) {
        final long[] addresses = new long[docs.length];
        long address = 0;
        for (int d = 0; d < docs.length; d++) {
            addresses[d] = address;
            address += docs[d].length;
        }
        return addresses;
    }

    public void testRanksAnswerTheSameInAnyOrder() throws IOException {
        final BytesRef[][] docs = varyingSlots();
        final long[] expected = expectedAddresses(docs);
        withColumn(docs, 128, (meta, reader) -> {
            assertTrue("the slots are not in step with the documents", meta.hasValueAddresses());

            // Ascending, which is how a scan arrives and the order the block cache is built for.
            for (int rank = 0; rank < docs.length; rank++) {
                assertEquals("address at rank " + rank, expected[rank], reader.firstValueAddress(rank));
                assertEquals("count at rank " + rank, docs[rank].length, reader.valueCount(rank));
            }
            // Descending, so every block is entered from its far end.
            for (int rank = docs.length - 1; rank >= 0; rank--) {
                assertEquals("descending address at rank " + rank, expected[rank], reader.firstValueAddress(rank));
            }
            // Shuffled, so a read lands in a block the one before it was not in.
            final List<Integer> ranks = new ArrayList<>();
            for (int rank = 0; rank < docs.length; rank++) {
                ranks.add(rank);
            }
            Collections.shuffle(ranks, random());
            for (int rank : ranks) {
                assertEquals("shuffled address at rank " + rank, expected[rank], reader.firstValueAddress(rank));
                assertEquals("shuffled count at rank " + rank, docs[rank].length, reader.valueCount(rank));
            }
        });
    }

    /**
     * A document whose slots are the last of its block, and the one that starts the next. Their addresses
     * come from different bases, and the first of them is the only count a block's final entry closes.
     */
    public void testDocumentsEitherSideOfABlockBoundary() throws IOException {
        final BytesRef[][] docs = varyingSlots();
        final long[] expected = expectedAddresses(docs);
        withColumn(docs, 128, (meta, reader) -> {
            for (int boundary = AddressingWriter.COUNTS_BLOCK_SIZE; boundary < docs.length; boundary +=
                AddressingWriter.COUNTS_BLOCK_SIZE) {
                assertEquals("last of a block", expected[boundary - 1], reader.firstValueAddress(boundary - 1));
                assertEquals("count of the last of a block", docs[boundary - 1].length, reader.valueCount(boundary - 1));
                assertEquals("first of the next", expected[boundary], reader.firstValueAddress(boundary));
                assertEquals("count of the first of the next", docs[boundary].length, reader.valueCount(boundary));
            }
        });
    }

    /** Every document holds the same number of slots, which is one run a block and the shape {@code tags} takes. */
    public void testConstantCountsReadBack() throws IOException {
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            docs[d] = new BytesRef[] { new BytesRef("a" + d), new BytesRef("b" + d), new BytesRef("c" + d) };
        }
        final long[] expected = expectedAddresses(docs);
        withColumn(docs, 128, (meta, reader) -> {
            for (int rank = 0; rank < docs.length; rank++) {
                assertEquals("address at rank " + rank, expected[rank], reader.firstValueAddress(rank));
                assertEquals(3, reader.valueCount(rank));
                for (int slot = 0; slot < 3; slot++) {
                    assertEquals(docs[rank][slot], reader.valueAt(expected[rank] + slot));
                }
            }
        });
    }

    /**
     * A column whose documents hold one slot each is in step with its documents, so it tables no addressing
     * at all and a rank is its own value address.
     */
    public void testColumnInStepTablesNothing() throws IOException {
        final BytesRef[][] docs = new BytesRef[DOCS][];
        for (int d = 0; d < DOCS; d++) {
            docs[d] = new BytesRef[] { new BytesRef("only" + d) };
        }
        withColumn(docs, 128, (meta, reader) -> {
            assertFalse("the slots are in step with the documents", meta.hasValueAddresses());
            assertSame("nothing to address", SlotAddressing.NONE, meta.addressing());
            for (int rank = 0; rank < docs.length; rank++) {
                assertEquals(rank, reader.firstValueAddress(rank));
                assertEquals(1, reader.valueCount(rank));
            }
        });
    }
}
