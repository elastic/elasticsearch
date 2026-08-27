/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * What the mapper hands the ColumNAR codec for a keyword field: one payload per present document, carrying its own slot count with nulls
 * inline, and no companion count field for any shape.
 */
public class ColumnarBinaryDocValuesFieldTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String COUNTS = FIELD + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

    public void testValuesTravelWithTheirCount() {
        final LuceneDocument doc = new LuceneDocument();
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            values.add(new BytesRef(randomAlphaOfLengthBetween(0, 30)));
        }
        for (BytesRef value : values) {
            ColumnarBinaryDocValuesField.recordValue(doc, FIELD, value, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
        }
        assertSlots(doc, values);
        assertNoCounts(doc);
    }

    /** A null slot keeps its position, which is what array-order reconstruction needs. */
    public void testNullSlotsKeepTheirPosition() {
        final LuceneDocument doc = new LuceneDocument();
        final List<BytesRef> slots = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            final BytesRef slot = randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 30));
            slots.add(slot);
            if (slot == null) {
                ColumnarBinaryDocValuesField.recordNull(doc, FIELD);
            } else {
                ColumnarBinaryDocValuesField.recordValue(doc, FIELD, slot, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
            }
        }
        assertSlots(doc, slots);
        assertNoCounts(doc);
    }

    /**
     * A document with no non-null value still writes a payload — the count describes it — which is what keeps an all-null array
     * distinguishable from a field that is simply absent.
     */
    public void testAllNullDocumentStillWritesAPayload() {
        final LuceneDocument doc = new LuceneDocument();
        final List<BytesRef> slots = new ArrayList<>();
        for (int i = 0; i < between(1, 5); i++) {
            slots.add(null);
            ColumnarBinaryDocValuesField.recordNull(doc, FIELD);
        }
        assertSlots(doc, slots);
        assertNoCounts(doc);
    }

    /** An empty array is a count of zero and nothing after it. */
    public void testEmptyArrayIsACountOfZero() {
        final LuceneDocument doc = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordEmptyArray(doc, FIELD);
        assertSlots(doc, List.of());
        assertEquals("the empty payload", StringBinaryPayload.EMPTY, doc.getField(FIELD).binaryValue());
        assertNoCounts(doc);
    }

    /** The single-value fast path must land on the same shape as the general one. */
    public void testRecordSingleValueMatchesRecordValue() {
        final BytesRef value = new BytesRef(randomAlphaOfLengthBetween(1, 30));

        final LuceneDocument viaSingle = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordSingleValue(viaSingle, FIELD, value, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);

        final LuceneDocument viaGeneral = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordValue(viaGeneral, FIELD, value, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);

        assertEquals(viaGeneral.getField(FIELD).binaryValue(), viaSingle.getField(FIELD).binaryValue());
    }

    /** Even a lone value carries its count, so the blob is never the bare value. */
    public void testALoneValueStillCarriesItsCount() {
        final LuceneDocument doc = new LuceneDocument();
        final BytesRef value = new BytesRef(randomAlphaOfLengthBetween(1, 30));
        ColumnarBinaryDocValuesField.recordSingleValue(doc, FIELD, value, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
        assertNotEquals("not the bare value", value, doc.getField(FIELD).binaryValue());
        assertSlots(doc, List.of(value));
    }

    /** Sorted-unique collection still applies; the payload just records however many survived it. */
    public void testSortedUniqueOrderingDeduplicates() {
        final LuceneDocument doc = new LuceneDocument();
        for (String value : new String[] { "b", "a", "b", "c", "a" }) {
            ColumnarBinaryDocValuesField.recordValue(
                doc,
                FIELD,
                new BytesRef(value),
                MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
            );
        }
        assertSlots(doc, List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c")));
    }

    /** The codec reads its column type off the field's attributes, and readers dispatch on the same one. */
    public void testFieldTypeCarriesTheCodecAttribute() {
        final var field = new ColumnarBinaryDocValuesField(FIELD, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
        assertEquals(ColumnarFieldType.STRING.name(), field.fieldType().getAttributes().get(ColumNARDocValuesFormat.TYPE_ATTRIBUTE));
    }

    /** The blob decodes back to exactly the slots that went in, in order. */
    private static void assertSlots(LuceneDocument doc, List<BytesRef> expected) {
        final IndexableField field = doc.getField(FIELD);
        assertNotNull("binary field", field);
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        assertEquals("slot count", expected.size(), decoder.reset(field.binaryValue()));
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("slot " + i, expected.get(i), decoder.next());
        }
    }

    private static void assertNoCounts(LuceneDocument doc) {
        assertNull("no counts companion", doc.getField(COUNTS));
    }
}
