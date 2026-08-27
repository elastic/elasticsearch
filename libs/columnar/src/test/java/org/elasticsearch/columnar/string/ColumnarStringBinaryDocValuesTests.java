/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

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

    /**
     * The ingest cursor over a foreign binary field: it splits each document's payload back into the slots
     * that went into it, and hands every position it is asked for to the field it wraps.
     */
    public void testDecodePayloadsSplitsWhatTheMapperWrote() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(20, 300), 5, true, true);
        final StringColumnValues cursor = ColumnarStringBinaryDocValues.decodePayloads(
            payloadsOver(docSlots),
            StringBinaryPayload.Framing.ARRAY_ORDER
        );
        assertEquals("cost", present(docSlots), cursor.cost());
        int seen = 0;
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            assertEquals("docID follows the field", doc, cursor.docID());
            assertEquals("slot count at doc " + doc, docSlots[doc].length, cursor.valueCount());
            assertEquals("null count at doc " + doc, nulls(docSlots[doc]), cursor.nullCount());
            for (int slot = 0; slot < docSlots[doc].length; slot++) {
                assertEquals("doc " + doc + " slot " + slot, docSlots[doc][slot], cursor.nextValue());
            }
            seen++;
        }
        assertEquals("documents with a value", present(docSlots), seen);
    }

    /** The same cursor driven by {@code advance}, which ingest and merge never use. */
    public void testDecodePayloadsAdvances() throws IOException {
        final BytesRef[][] docSlots = randomDocSlots(between(50, 400), 3, true, true);
        final StringColumnValues cursor = ColumnarStringBinaryDocValues.decodePayloads(
            payloadsOver(docSlots),
            StringBinaryPayload.Framing.ARRAY_ORDER
        );
        int target = 0;
        while (target < docSlots.length) {
            final int expected = nextPresent(docSlots, target);
            final int landed = cursor.advance(target);
            if (expected == DocIdSetIterator.NO_MORE_DOCS) {
                assertEquals("past the last value", DocIdSetIterator.NO_MORE_DOCS, landed);
                break;
            }
            assertEquals("advance(" + target + ")", expected, landed);
            assertEquals("slot count", docSlots[landed].length, cursor.valueCount());
            assertEquals("first slot at doc " + landed, docSlots[landed][0], cursor.nextValue());
            target = landed + 1;
        }
        assertEquals("past the end", DocIdSetIterator.NO_MORE_DOCS, cursor.advance(docSlots.length));
    }

    /**
     * The surface hands back the framing the mapper would have written, not the one the codec was fed, which
     * is what lets a reader that still consults {@code .counts} work against the column unchanged. Compared
     * against a reference encoder written out longhand here rather than against the encoder under test.
     */
    public void testBinaryValueReEncodesIntoTheMappersFraming() throws IOException {
        for (StringBinaryPayload.Framing framing : StringBinaryPayload.Framing.values()) {
            final boolean nulls = framing == StringBinaryPayload.Framing.ARRAY_ORDER;
            // PLAIN has nowhere to put a count, so it only describes a column of one slot per document.
            final int maxSlots = framing.isSelfDescribing() ? 6 : 1;
            final BytesRef[][] docSlots = randomDocSlots(between(20, 300), maxSlots, true, nulls);
            withColumn(docSlots, framing, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), (metadata, reader) -> {
                final ColumnarStringBinaryDocValues dv = new ColumnarStringBinaryDocValues(reader, reader.iterator());
                for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                    assertEquals("doc " + doc + " under " + framing, legacyEncode(docSlots[doc], framing), dv.binaryValue());
                }
            });
        }
    }

    /** A null slot survives the column and comes back distinguishable from the empty string beside it. */
    public void testNullSlotsSurviveTheColumn() throws IOException {
        final BytesRef empty = new BytesRef("");
        final BytesRef[][] docSlots = {
            { null, new BytesRef("a") },
            { empty, null, empty },
            { new BytesRef("b") },
            { null, null, new BytesRef("c"), null },
            { new BytesRef("d"), empty } };
        withColumn(
            docSlots,
            StringBinaryPayload.Framing.ARRAY_ORDER,
            randomValidBlockSize(),
            randomChunkCodec(),
            64,
            (metadata, reader) -> {
                assertEquals("null slots recorded", numNullSlots(docSlots), metadata.numNullSlots());
                assertEquals("null slots counted by hand", 5L, numNullSlots(docSlots));
                final ColumnarStringBinaryDocValues dv = new ColumnarStringBinaryDocValues(reader, reader.iterator());
                for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                    assertEquals("doc " + doc, legacyEncode(docSlots[doc], StringBinaryPayload.Framing.ARRAY_ORDER), dv.binaryValue());
                }
                // Asked out of order, which is what sends the null cursor back through its binary search.
                final ColumnIterator iterator = reader.iterator();
                assertTrue(iterator.advanceExact(3));
                final long first = reader.firstValueAddress(iterator.rank());
                assertTrue("doc 3 slot 0 is null", reader.isNullSlot(first));
                assertFalse("doc 3 slot 2 is a value", reader.isNullSlot(first + 2));
                assertTrue("re-asking behind the cursor", reader.isNullSlot(first + 1));
            }
        );
    }

    /** The merge cursor over a written column, driven by {@code advance} rather than a walk. */
    public void testDirectValuesAdvances() throws IOException {
        final BytesRef[] docValues = new BytesRef[between(50, 400)];
        for (int d = 0; d < docValues.length; d++) {
            docValues[d] = randomBoolean() ? null : new BytesRef("v" + d);
        }
        withSurface(docValues, dv -> {
            final StringColumnValues cursor = dv.directValues();
            int target = 0;
            while (target < docValues.length) {
                final int expected = nextPresent(docValues, target);
                final int landed = cursor.advance(target);
                if (expected == DocIdSetIterator.NO_MORE_DOCS) {
                    assertEquals("past the last value", DocIdSetIterator.NO_MORE_DOCS, landed);
                    return;
                }
                assertEquals("advance(" + target + ")", expected, landed);
                assertEquals("value count", 1, cursor.valueCount());
                assertEquals("value at doc " + landed, docValues[landed], cursor.nextValue());
                target = landed + 1;
            }
        });
    }

    /** Documents that have a value, which is what a cursor over the column walks. */
    private static int present(BytesRef[] docValues) {
        int count = 0;
        for (BytesRef value : docValues) {
            if (value != null) {
                count++;
            }
        }
        return count;
    }

    private static int present(BytesRef[][] docSlots) {
        int count = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                count++;
            }
        }
        return count;
    }

    private static int nulls(BytesRef[] slots) {
        int count = 0;
        for (BytesRef slot : slots) {
            if (slot == null) {
                count++;
            }
        }
        return count;
    }

    /**
     * An in-memory binary field carrying each document's slots as the payload a columnar keyword field
     * writes, standing in for the mapper on the other side of the ingest hop.
     */
    private static BinaryDocValues payloadsOver(BytesRef[][] docSlots) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return StringBinaryPayload.encode(Arrays.asList(docSlots[doc]));
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return docSlots[target] != null;
            }

            @Override
            public int docID() {
                return doc >= docSlots.length ? NO_MORE_DOCS : doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                for (doc = target; doc < docSlots.length; doc++) {
                    if (docSlots[doc] != null) {
                        return doc;
                    }
                }
                doc = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return present(docSlots);
            }
        };
    }

    /**
     * The framing the mapper writes, spelled out longhand: a lone slot raw, otherwise a length per slot,
     * biased by one where the framing can carry a null. Deliberately not the production encoder — this is
     * what {@code binaryValue()} is checked against.
     */
    private static BytesRef legacyEncode(BytesRef[] slots, StringBinaryPayload.Framing framing) throws IOException {
        if (slots.length == 1) {
            return slots[0];
        }
        final int bias = framing == StringBinaryPayload.Framing.ARRAY_ORDER ? 1 : 0;
        final byte[] buffer = new byte[upperBound(slots)];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        for (BytesRef slot : slots) {
            if (slot == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(slot.length + bias);
                out.writeBytes(slot.bytes, slot.offset, slot.length);
            }
        }
        return new BytesRef(buffer, 0, out.getPosition());
    }

    /** Room for every slot's bytes plus the widest a length prefix can be. */
    private static int upperBound(BytesRef[] slots) {
        int length = 0;
        for (BytesRef slot : slots) {
            length += StringBinaryPayload.VINT_MAX_BYTES + (slot == null ? 0 : slot.length);
        }
        return length;
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

    private static int nextPresent(BytesRef[][] docSlots, int target) {
        for (int doc = target; doc < docSlots.length; doc++) {
            if (docSlots[doc] != null) {
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
