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
import org.elasticsearch.columnar.substrate.ChunkCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Where a document's slots begin, over a column of more blocks of counts than one. The addresses inside a
 * block are summed from its base rather than stored, so what matters is that a rank answers the same
 * wherever the read arrives from and whichever block it was last in.
 *
 * <p>The block is fixed small here so a column of a few hundred documents still crosses several of them,
 * and so a test can put its last document exactly on a boundary, one short of one, and one past one.
 */
public class SlotAddressingTests extends ColumnarStringTestCase {

    private static final int COUNTS_BLOCK = 128;

    /** Slot counts that vary, with documents holding nothing, one, and many, so no block is a single run. */
    private static int slotsFor(int doc) {
        return switch (doc % 7) {
            case 0 -> 0;
            case 1, 2, 3 -> 1;
            case 4 -> 2;
            case 5 -> 9;
            default -> 3;
        };
    }

    /** Every document carries the field, so a rank is its position. */
    private static BytesRef[][] dense(int docs) {
        final BytesRef[][] column = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            column[d] = values(d, slotsFor(d));
        }
        return column;
    }

    /** Only some documents carry the field, so a rank counts the carriers rather than the documents. */
    private static BytesRef[][] sparse(int docs) {
        final BytesRef[][] column = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            if (d % 3 != 0) {
                column[d] = values(d, slotsFor(d));
            }
        }
        return column;
    }

    private static BytesRef[] values(int doc, int slots) {
        final BytesRef[] v = new BytesRef[slots];
        for (int s = 0; s < slots; s++) {
            // A vocabulary narrow enough that the column keeps a dictionary when it is offered one.
            v[s] = new BytesRef("term-" + ((doc + s) % 50));
        }
        return v;
    }

    /** The address each carrier's slots begin at, and the rank order they are addressed by. */
    private static long[] expectedAddresses(BytesRef[][] column, List<Integer> carriers) {
        final long[] addresses = new long[carriers.size()];
        long address = 0;
        for (int i = 0; i < carriers.size(); i++) {
            addresses[i] = address;
            address += column[carriers.get(i)].length;
        }
        return addresses;
    }

    private static List<Integer> carriersOf(BytesRef[][] column) {
        final List<Integer> carriers = new ArrayList<>();
        for (int d = 0; d < column.length; d++) {
            if (column[d] != null) {
                carriers.add(d);
            }
        }
        return carriers;
    }

    /**
     * Reads every rank forwards, backwards and shuffled, under both layouts. A dictionary column drives the
     * addressing from its own write loop, so it has to be covered as well as the one that stores its values.
     */
    private void assertAddresses(String shape, BytesRef[][] column) throws IOException {
        final List<Integer> carriers = carriersOf(column);
        final long[] expected = expectedAddresses(column, carriers);
        for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, StringColumnOptions.DEFAULT_DICTIONARY }) {
            final String where = shape + " under " + (policy == DictionaryPolicy.NONE ? "plain" : "dictionary");
            withColumn(column, 128, ChunkCodec.ZSTD, 64 * 1024, policy, 2048, COUNTS_BLOCK, (meta, reader) -> {
                assertEquals(where + ": carriers", carriers.size(), reader.numDocsWithField());
                for (int rank = 0; rank < carriers.size(); rank++) {
                    assertRank(where + " ascending", column, carriers, expected, reader, rank);
                }
                for (int rank = carriers.size() - 1; rank >= 0; rank--) {
                    assertRank(where + " descending", column, carriers, expected, reader, rank);
                }
                final List<Integer> ranks = new ArrayList<>();
                for (int rank = 0; rank < carriers.size(); rank++) {
                    ranks.add(rank);
                }
                Collections.shuffle(ranks, random());
                for (int rank : ranks) {
                    assertRank(where + " shuffled", column, carriers, expected, reader, rank);
                }
            });
        }
    }

    private static void assertRank(
        String where,
        BytesRef[][] column,
        List<Integer> carriers,
        long[] expected,
        StringColumnReader reader,
        int rank
    ) throws IOException {
        final BytesRef[] slots = column[carriers.get(rank)];
        assertEquals(where + ": address at rank " + rank, expected[rank], reader.firstValueAddress(rank));
        assertEquals(where + ": count at rank " + rank, slots.length, reader.valueCount(rank));
        for (int slot = 0; slot < slots.length; slot++) {
            assertEquals(where + ": value at rank " + rank + " slot " + slot, slots[slot], reader.valueAt(expected[rank] + slot));
        }
    }

    /** A column whose carriers land one short of a block, exactly on one, and one past one. */
    public void testDenseColumnAcrossBlockBoundaries() throws IOException {
        for (int carriers : new int[] { COUNTS_BLOCK - 1, COUNTS_BLOCK, COUNTS_BLOCK + 1, 3 * COUNTS_BLOCK, 3 * COUNTS_BLOCK + 1 }) {
            assertAddresses("dense " + carriers, dense(carriers));
        }
    }

    /**
     * The same boundaries on a column only some documents carry, where a rank is no longer a document id.
     * The document count is chosen so the carriers land on the boundary rather than the documents.
     */
    public void testSparseColumnAcrossBlockBoundaries() throws IOException {
        for (int carriers : new int[] { COUNTS_BLOCK - 1, COUNTS_BLOCK, COUNTS_BLOCK + 1, 3 * COUNTS_BLOCK + 1 }) {
            // Two of every three documents carry the field, so this many documents yield that many carriers.
            final int docs = (int) Math.ceil(carriers * 3.0 / 2.0) + 3;
            final BytesRef[][] column = sparse(docs);
            assertAddresses("sparse " + carriersOf(column).size(), column);
        }
    }

    /** Every document holds the same number of slots, which is one run a block and the shape {@code tags} takes. */
    public void testConstantCountsReadBack() throws IOException {
        final int docs = 3 * COUNTS_BLOCK + 37;
        final BytesRef[][] column = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            column[d] = values(d, 3);
        }
        assertAddresses("constant counts", column);
    }

    /**
     * A column whose documents hold one slot each is in step with its documents, so it tables no addressing
     * at all and a rank is its own value address.
     */
    public void testColumnInStepTablesNothing() throws IOException {
        final int docs = 2 * COUNTS_BLOCK + 5;
        final BytesRef[][] column = new BytesRef[docs][];
        for (int d = 0; d < docs; d++) {
            column[d] = values(d, 1);
        }
        withColumn(column, 128, ChunkCodec.ZSTD, 64 * 1024, DictionaryPolicy.NONE, 2048, COUNTS_BLOCK, (meta, reader) -> {
            assertFalse("the slots are in step with the documents", meta.hasValueAddresses());
            assertSame("nothing to address", SlotAddressing.NONE, meta.addressing());
            for (int rank = 0; rank < docs; rank++) {
                assertEquals(rank, reader.firstValueAddress(rank));
                assertEquals(1, reader.valueCount(rank));
            }
        });
    }
}
