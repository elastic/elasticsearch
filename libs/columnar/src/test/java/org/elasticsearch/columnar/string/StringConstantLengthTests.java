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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnTestFiles;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * A plain column whose values all have one length and none of them null keeps no lengths: a value begins at its
 * address times that length.
 */
public class StringConstantLengthTests extends ColumnarStringTestCase {

    public void testOneLengthStoresNoLengths() throws IOException {
        final int length = between(0, 40);
        final BytesRef[][] docSlots = slots(between(1, 5000), length, randomBoolean(), false, false);
        withColumn(docSlots, (metadata, reader) -> {
            final PlainValues.Metadata values = plainOf(metadata).values();
            assertEquals("the one length", length, values.constantLength());
            assertNull("no lengths stored", values.lengths());
            assertSame("no starts stored", MonotonicWriter.Table.NONE, values.starts());
            assertEquals(length, metadata.minLength());
            assertEquals(length, metadata.maxLength());
            assertEverySlotReadsBack(docSlots, reader);
        });
    }

    /** Every value empty: a length of zero, so no bytes and no lengths are stored at all. */
    public void testOnlyEmptyValues() throws IOException {
        final BytesRef[][] docSlots = slots(between(1, 3000), 0, randomBoolean(), false, false);
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals(0, plainOf(metadata).values().constantLength());
            assertEquals(0, plainOf(metadata).values().chunks().numChunks());
            assertEverySlotReadsBack(docSlots, reader);
        });
    }

    /** Documents holding empty arrays and no value at all: nothing to take a length from. */
    public void testNoValues() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(1, 500)][];
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = new BytesRef[0];
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals(0, metadata.numValues());
            assertEquals(-1, metadata.minLength());
            assertEquals(-1, metadata.maxLength());
            assertEverySlotReadsBack(docSlots, reader);
        });
    }

    /** One null, or one value of another length, and the lengths are stored as for any column. */
    public void testOneExceptionStoresTheLengths() throws IOException {
        final boolean aNull = randomBoolean();
        final BytesRef[][] docSlots = slots(between(2, 5000), between(1, 40), randomBoolean(), aNull, aNull == false);
        withColumn(docSlots, (metadata, reader) -> {
            final PlainValues.Metadata values = plainOf(metadata).values();
            assertEquals("no one length", -1, values.constantLength());
            assertNotNull("lengths stored", values.lengths());
            assertEverySlotReadsBack(docSlots, reader);
        });
    }

    /** Totals that promise one length the values do not keep fail the write rather than misplacing every value. */
    public void testALengthOffTheCountedOneFailsTheWrite() throws IOException {
        final BytesRef[][] docSlots = slots(between(2, 500), 8, false, false, true);
        final StringColumnValues.Totals counted = totals(docSlots);
        final StringColumnValues.Totals lying = new StringColumnValues.Totals(
            counted.numDocsWithField(),
            counted.numValues(),
            counted.numNullSlots(),
            8,
            8
        );
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory(); ColumnTestFiles.Outputs out = ColumnTestFiles.create(dir, "column", segmentId)) {
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> StringColumnWriter.write(
                    docSlots.length,
                    lying,
                    () -> cursor(docSlots),
                    StringColumnOptions.DEFAULT.withDictionary(DictionaryPolicy.NONE),
                    null,
                    dir,
                    IOContext.DEFAULT,
                    out.outputs()
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("counted at 8"));
        }
    }

    /**
     * Documents of one to three slots, or one each, every value {@code length} bytes; {@code aNull} puts a null
     * in one of them and {@code anOddLength} a value one byte longer.
     */
    private static BytesRef[][] slots(int numDocs, int length, boolean multiValued, boolean aNull, boolean anOddLength) {
        final BytesRef[][] docSlots = new BytesRef[numDocs][];
        for (int d = 0; d < numDocs; d++) {
            docSlots[d] = new BytesRef[multiValued ? between(1, 3) : 1];
            for (int s = 0; s < docSlots[d].length; s++) {
                docSlots[d][s] = new BytesRef(randomAlphaOfLength(length));
            }
        }
        final int odd = between(0, numDocs - 1);
        if (aNull) {
            docSlots[odd][0] = null;
        } else if (anOddLength) {
            docSlots[odd][0] = new BytesRef(randomAlphaOfLength(length + 1));
        }
        return docSlots;
    }

    private static void assertEverySlotReadsBack(BytesRef[][] docSlots, StringColumnReader reader) throws IOException {
        final ColumnIterator iterator = reader.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            final long first = reader.firstValueAddress(iterator.rank());
            assertEquals("slot count of doc " + doc, docSlots[doc].length, reader.valueCount(iterator.rank()));
            for (int slot = 0; slot < docSlots[doc].length; slot++) {
                final long address = first + slot;
                if (docSlots[doc][slot] == null) {
                    assertTrue("doc " + doc + " slot " + slot + " is null", reader.isNullSlot(address));
                } else {
                    assertFalse("doc " + doc + " slot " + slot + " is a value", reader.isNullSlot(address));
                    assertEquals("doc " + doc + " slot " + slot, docSlots[doc][slot], reader.valueAt(address));
                    assertEquals("length of doc " + doc + " slot " + slot, docSlots[doc][slot].length, reader.byteLengthAt(address));
                }
            }
        }
    }
}
