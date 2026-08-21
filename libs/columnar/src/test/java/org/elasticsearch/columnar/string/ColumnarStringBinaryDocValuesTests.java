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

/**
 * The {@code BinaryDocValues} surface of a string column, beyond the {@code nextDoc} + {@code binaryValue} scan
 * the round-trip tests cover. The positioning methods delegate to {@code ColumnIterator}, whose own behaviour is
 * covered by {@code ColumnIteratorTests}; what these assert is that arriving at a document by seeking rather
 * than by scanning still yields that document's value — the rank to value-address to bytes chain is the part
 * that could be wrong without any of the delegation being wrong.
 */
public class ColumnarStringBinaryDocValuesTests extends ColumnarStringTestCase {

    public void testAdvanceExactOnDenseColumn() throws IOException {
        final BytesRef[] docValues = dense(between(50, 500));
        withSurface(docValues, dv -> {
            // Non-decreasing targets, as the doc-values contract requires.
            for (int doc = 0; doc < docValues.length; doc += between(1, 5)) {
                assertTrue("doc " + doc + " has a value", dv.advanceExact(doc));
                assertEquals("doc " + doc, doc, dv.docID());
                assertEquals("doc " + doc, docValues[doc], single(dv));
            }
        });
    }

    public void testAdvanceExactOnSparseColumn() throws IOException {
        final BytesRef[] docValues = sparse(between(200, 800));
        withSurface(docValues, dv -> {
            for (int doc = 0; doc < docValues.length; doc += between(1, 5)) {
                final boolean present = dv.advanceExact(doc);
                assertEquals("presence at doc " + doc, docValues[doc] != null, present);
                if (present) {
                    assertEquals("doc " + doc, docValues[doc], single(dv));
                }
            }
        });
    }

    /** Re-asking for the current document must not move the cursor or change the value. */
    public void testAdvanceExactIsRepeatable() throws IOException {
        final BytesRef[] docValues = dense(between(20, 100));
        final int target = between(0, docValues.length - 1);
        withSurface(docValues, dv -> {
            assertTrue(dv.advanceExact(target));
            final BytesRef first = single(dv);
            assertTrue("re-asking for the same target", dv.advanceExact(target));
            assertEquals(target, dv.docID());
            assertEquals(first, single(dv));
            assertEquals(docValues[target], single(dv));
        });
    }

    public void testAdvanceLandsOnOrAfterTarget() throws IOException {
        final BytesRef[] docValues = sparse(between(200, 800));
        withSurface(docValues, dv -> {
            int target = 0;
            while (target < docValues.length) {
                final int landed = dv.advance(target);
                final int expected = nextPresent(docValues, target);
                assertEquals("advance(" + target + ")", expected, landed);
                if (landed == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
                // Landing on a present doc, whether target itself or the next one after a gap.
                assertEquals("doc " + landed, docValues[landed], single(dv));
                target = landed + between(1, 10);
            }
        });
    }

    public void testAdvancePastLastValueExhausts() throws IOException {
        final BytesRef[] docValues = dense(between(10, 50));
        withSurface(docValues, dv -> assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(docValues.length)));
    }

    public void testIntoBitSetWithinRange() throws IOException {
        final BytesRef[] docValues = sparse(between(200, 600));
        final int upTo = between(1, docValues.length);
        withSurface(docValues, dv -> {
            assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            final int from = dv.docID();
            final FixedBitSet bitSet = new FixedBitSet(docValues.length);
            dv.intoBitSet(upTo, bitSet, 0);
            for (int doc = 0; doc < docValues.length; doc++) {
                final boolean expected = doc >= from && doc < upTo && docValues[doc] != null;
                assertEquals("bit " + doc, expected, bitSet.get(doc));
            }
        });
    }

    /** Past the last document, so every remaining value is filled in and the iterator is left exhausted. */
    public void testIntoBitSetPastLastDoc() throws IOException {
        final BytesRef[] docValues = sparse(between(100, 400));
        withSurface(docValues, dv -> {
            assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            final int from = dv.docID();
            final FixedBitSet bitSet = new FixedBitSet(docValues.length);
            dv.intoBitSet(docValues.length, bitSet, 0);
            for (int doc = from; doc < docValues.length; doc++) {
                assertEquals("bit " + doc, docValues[doc] != null, bitSet.get(doc));
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        });
    }

    /** {@code upTo} at the current document sets nothing, because the range it covers is empty. */
    public void testIntoBitSetAtCurrentDocIsNoOp() throws IOException {
        final BytesRef[] docValues = dense(between(10, 100));
        withSurface(docValues, dv -> {
            assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            final int current = dv.docID();
            final FixedBitSet bitSet = new FixedBitSet(docValues.length);
            dv.intoBitSet(current, bitSet, 0);
            assertEquals("no bits set", 0, bitSet.cardinality());
            assertEquals("iterator did not move", current, dv.docID());
        });
    }

    /** With an offset, bit {@code d - offset} stands for document {@code d}. */
    public void testIntoBitSetWithOffset() throws IOException {
        final BytesRef[] docValues = sparse(between(200, 600));
        withSurface(docValues, dv -> {
            assertNotEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            final int offset = dv.docID();
            final int upTo = between(offset, docValues.length);
            final FixedBitSet bitSet = new FixedBitSet(docValues.length);
            dv.intoBitSet(upTo, bitSet, offset);
            for (int doc = offset; doc < upTo; doc++) {
                assertEquals("bit for doc " + doc, docValues[doc] != null, bitSet.get(doc - offset));
            }
        });
    }

    public void testCostIsTheNumberOfDocumentsWithAValue() throws IOException {
        final BytesRef[] docValues = sparse(between(100, 500));
        final int numDocsWithField = numDocsWithField(docValues);
        withSurface(docValues, dv -> assertEquals(numDocsWithField, dv.cost()));
    }

    private interface SurfaceCheck {
        void check(ColumnarStringBinaryDocValues dv) throws IOException;
    }

    /** Writes {@code docValues} as a column, opens it at the binary surface, and runs {@code check} over it. */
    private void withSurface(BytesRef[] docValues, SurfaceCheck check) throws IOException {
        withColumn(docValues, (metadata, reader) -> check.check(new ColumnarStringBinaryDocValues(reader, reader.iterator())));
    }

    /** The current document's only value, which the surface hands back as the bytes it was given. */
    private static BytesRef single(ColumnarStringBinaryDocValues dv) throws IOException {
        return BytesRef.deepCopyOf(dv.binaryValue());
    }

    /** The first document at or after {@code target} that has a value, or {@code NO_MORE_DOCS}. */
    private static int nextPresent(BytesRef[] docValues, int target) {
        for (int doc = target; doc < docValues.length; doc++) {
            if (docValues[doc] != null) {
                return doc;
            }
        }
        return DocIdSetIterator.NO_MORE_DOCS;
    }

    private BytesRef[] dense(int maxDoc) {
        final BytesRef[] docValues = new BytesRef[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            docValues[doc] = new BytesRef("value-" + doc);
        }
        return docValues;
    }

    /** Leaves gaps, but guarantees at least one value so the positioning cases have something to land on. */
    private BytesRef[] sparse(int maxDoc) {
        final BytesRef[] docValues = new BytesRef[maxDoc];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (random().nextDouble() < 0.4) {
                docValues[doc] = new BytesRef("value-" + doc);
            }
        }
        int guaranteed = between(0, maxDoc - 1);
        docValues[guaranteed] = new BytesRef("value-" + guaranteed);
        return docValues;
    }
}
