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
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Where a column keeps the table that says which slots a document holds. A document holding none and one
 * holding several put the slots out of step with the documents in opposite directions, so a column holding
 * both can have as many slots as documents while no document's rank is its own value address.
 */
public class StringColumnAddressingTests extends ColumnarStringTestCase {

    /** As many slots as documents, and not one of them where its rank says. */
    public void testDocumentsOutOfStepThatTheCountsHide() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] {
            new BytesRef[0],
            new BytesRef[] { new BytesRef("one") },
            new BytesRef[] { new BytesRef("two"), new BytesRef("three") } };
        withColumn(docSlots, (metadata, reader) -> {
            assertEquals("as many slots as documents", metadata.numDocsWithField(), metadata.numValues());
            assertTrue("the column keeps its addressing", metadata.hasValueAddresses());
            assertEverySlotReadsBack(docSlots, reader);
            assertPageHoldsEverySlot(docSlots, reader);
            assertEquals("the first value", List.of(1), matched(reader.matchTerm(new BytesRef("one"))));
            assertEquals("a value of the last document", List.of(2), matched(reader.matchTerm(new BytesRef("three"))));
        });
    }

    /** The same over a column large enough to cross the blocks its counts and its values are kept in. */
    public void testDocumentsOutOfStepAcrossBlocks() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(2000, 6000)][];
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = switch (d % 3) {
                case 0 -> new BytesRef[0];
                case 1 -> new BytesRef[] { new BytesRef("value-" + d) };
                default -> new BytesRef[] { new BytesRef("value-" + d), new BytesRef("other-" + d) };
            };
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertTrue("the column keeps its addressing", metadata.hasValueAddresses());
            assertEverySlotReadsBack(docSlots, reader);
            assertPageHoldsEverySlot(docSlots, reader);
            final int target = docSlots.length / 2 / 3 * 3 + 2;
            assertEquals("a value of document " + target, List.of(target), matched(reader.matchTerm(new BytesRef("other-" + target))));
        });
    }

    /** A document with no slots among documents that hold one each, which also puts the two out of step. */
    public void testADocumentHoldingNoneAmongOnes() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[between(100, 400)][];
        for (int d = 0; d < docSlots.length; d++) {
            docSlots[d] = d == docSlots.length / 2 ? new BytesRef[0] : new BytesRef[] { new BytesRef("v" + d) };
        }
        withColumn(docSlots, (metadata, reader) -> {
            assertTrue("the column keeps its addressing", metadata.hasValueAddresses());
            assertEverySlotReadsBack(docSlots, reader);
            assertPageHoldsEverySlot(docSlots, reader);
            final int after = docSlots.length / 2 + 1;
            assertEquals("the document after the empty one", List.of(after), matched(reader.matchTerm(new BytesRef("v" + after))));
        });
    }

    /**
     * The page a column of this shape serves, against the slots each document holds. A column that declined
     * would send the caller back to reading a document at a time, and one that served the wrong slots would
     * hand every document after the first its neighbour's values.
     */
    private static void assertPageHoldsEverySlot(BytesRef[][] docSlots, StringColumnReader reader) throws IOException {
        final int[] docs = new int[docSlots.length];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = d;
        }
        final List<BytesRef> paged = new ArrayList<>();
        final int[] counts = new int[1];
        assertTrue("the column serves a page of every document", reader.readBlock(docs, 0, docs.length, new StringBlockSink() {
            @Override
            public void appendOrdinals(int[] ordinals, int count, int[] valueCounts, int docCount, BytesRef[] dictionary, int size) {
                counts[0] = docCount;
                for (int i = 0; i < count; i++) {
                    paged.add(BytesRef.deepCopyOf(dictionary[ordinals[i]]));
                }
            }

            @Override
            public void appendValues(BytesRef[] values, int count, int[] valueCounts, int docCount) {
                counts[0] = docCount;
                for (int i = 0; i < count; i++) {
                    paged.add(BytesRef.deepCopyOf(values[i]));
                }
            }
        }));
        final List<BytesRef> expected = new ArrayList<>();
        for (BytesRef[] slots : docSlots) {
            for (BytesRef slot : slots) {
                if (slot != null) {
                    expected.add(slot);
                }
            }
        }
        assertEquals("the page holds every slot in order", expected, paged);
        assertEquals("a position a document", docSlots.length, counts[0]);
    }

    /** Every slot of every document, read back where the column says it is. */
    private static void assertEverySlotReadsBack(BytesRef[][] docSlots, StringColumnReader reader) throws IOException {
        int seen = 0;
        final ColumnIterator iterator = reader.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            final int rank = iterator.rank();
            final BytesRef[] expected = docSlots[doc];
            assertEquals("slot count at doc " + doc, expected.length, reader.valueCount(rank));
            final long first = reader.firstValueAddress(rank);
            for (int slot = 0; slot < expected.length; slot++) {
                assertEquals("doc " + doc + " slot " + slot, expected[slot], reader.valueAt(first + slot));
            }
            seen++;
        }
        assertEquals("documents carrying the field", docSlots.length, seen);
    }

    private static List<Integer> matched(DocIdSetIterator matches) throws IOException {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }
}
