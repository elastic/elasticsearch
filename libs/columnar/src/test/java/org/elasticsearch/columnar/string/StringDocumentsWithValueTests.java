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
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * The documents holding a value, which a column answers from what it records about its slots: a document with no
 * slot, or whose every slot is null, is not one of them.
 */
public class StringDocumentsWithValueTests extends ColumnarStringTestCase {

    public void testOneSlotADocument() throws IOException {
        assertDocumentsWithValue(d -> switch (random().nextInt(5)) {
            case 0 -> null;
            case 1 -> new BytesRef[] { null };
            default -> new BytesRef[] { new BytesRef(random().nextBoolean() ? "term-" + (d % 7) : "unique-" + d) };
        });
    }

    /** Values of one length beside null slots, which a column cannot store as a column of one length. */
    public void testValuesOfOneLengthAmongNullSlots() throws IOException {
        assertDocumentsWithValue(d -> random().nextInt(4) == 0 ? new BytesRef[] { null } : new BytesRef[] { new BytesRef("v" + (d % 9)) });
    }

    public void testNoNullSlot() throws IOException {
        assertDocumentsWithValue(d -> random().nextInt(4) == 0 ? null : new BytesRef[] { new BytesRef("term-" + (d % 7)) });
    }

    public void testSeveralSlotsADocument() throws IOException {
        assertDocumentsWithValue(d -> switch (random().nextInt(6)) {
            case 0 -> null;
            case 1 -> new BytesRef[] { null };
            case 2 -> new BytesRef[] { null, null };
            case 3 -> new BytesRef[] { null, new BytesRef("b-" + d) };
            case 4 -> new BytesRef[] { new BytesRef("a"), new BytesRef("term-" + (d % 5)) };
            default -> new BytesRef[] { new BytesRef("term-" + (d % 7)) };
        });
    }

    private interface Slots {
        BytesRef[] of(int doc);
    }

    private void assertDocumentsWithValue(Slots slots) throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(200, 5000)][];
        final FixedBitSet expected = new FixedBitSet(docSlots.length);
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = slots.of(d);
            if (docSlots[d] != null) {
                for (BytesRef slot : docSlots[d]) {
                    if (slot != null) {
                        expected.set(d);
                    }
                }
            }
        }
        for (DictionaryPolicy policy : new DictionaryPolicy[] { DictionaryPolicy.NONE, StringColumnOptions.DEFAULT_DICTIONARY }) {
            withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), policy, (metadata, reader) -> {
                // A document at a time.
                final DocIdSetIterator one = reader.documentsWithValue();
                for (int d = expected.nextSetBit(0); d != DocIdSetIterator.NO_MORE_DOCS; d = next(expected, d + 1)) {
                    assertEquals(d, one.nextDoc());
                    final int runEnd = one.docIDRunEnd();
                    assertTrue("a run holds the document it is asked at", runEnd > d);
                    for (int inRun = d; inRun < runEnd; inRun++) {
                        assertTrue("document " + inRun + " is inside a run", expected.get(inRun));
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, one.nextDoc());

                // A window at a time.
                final DocIdSetIterator bulk = reader.documentsWithValue();
                final FixedBitSet collected = new FixedBitSet(docSlots.length);
                final int window = between(1, 700);
                for (int from = 0; from < docSlots.length; from += window) {
                    if (bulk.docID() < from) {
                        bulk.advance(from);
                    }
                    final int upTo = Math.min(from + window, docSlots.length);
                    final FixedBitSet bits = new FixedBitSet(upTo - from);
                    bulk.intoBitSet(upTo, bits, from);
                    for (int i = bits.nextSetBit(0); i != DocIdSetIterator.NO_MORE_DOCS; i = next(bits, i + 1)) {
                        collected.set(from + i);
                    }
                }
                assertEquals(expected, collected);
            });
        }
    }

    private static int next(FixedBitSet bits, int from) {
        return from >= bits.length() ? DocIdSetIterator.NO_MORE_DOCS : bits.nextSetBit(from);
    }
}
