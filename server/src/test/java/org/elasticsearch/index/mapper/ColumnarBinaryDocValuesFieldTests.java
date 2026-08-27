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
 * What the mapper hands the ColumNAR codec for a keyword field: a payload carrying its own slot count, so the
 * codec can split a document's values apart at flush, alongside the unchanged {@code .counts} companion every
 * reader still consults.
 */
public class ColumnarBinaryDocValuesFieldTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final String COUNTS = FIELD + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

    public void testArrayOrderValuesTravelWithTheirCount() {
        final LuceneDocument doc = new LuceneDocument();
        final List<BytesRef> values = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            values.add(new BytesRef(randomAlphaOfLengthBetween(0, 30)));
        }
        for (BytesRef value : values) {
            ColumnarBinaryDocValuesField.recordValue(
                doc,
                FIELD,
                value,
                StringBinaryPayload.Framing.ARRAY_ORDER,
                MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED
            );
        }
        assertSlots(doc, values);
        assertEquals("counts", (long) values.size(), counts(doc));
    }

    /** A null slot keeps its position and is counted, which is what array-order reconstruction needs. */
    public void testNullSlotsKeepTheirPositionAndCount() {
        final LuceneDocument doc = new LuceneDocument();
        final List<BytesRef> slots = new ArrayList<>();
        for (int i = 0; i < between(2, 20); i++) {
            final BytesRef slot = randomBoolean() ? null : new BytesRef(randomAlphaOfLengthBetween(0, 30));
            slots.add(slot);
            if (slot == null) {
                ColumnarBinaryDocValuesField.recordNull(doc, FIELD, StringBinaryPayload.Framing.ARRAY_ORDER);
            } else {
                ColumnarBinaryDocValuesField.recordValue(
                    doc,
                    FIELD,
                    slot,
                    StringBinaryPayload.Framing.ARRAY_ORDER,
                    MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED
                );
            }
        }
        assertEquals("counts include nulls", (long) slots.size(), counts(doc));
        if (slots.stream().anyMatch(s -> s != null)) {
            assertSlots(doc, slots);
        } else {
            assertNull("an all-null document writes no blob", doc.getField(FIELD));
        }
    }

    /**
     * A document with no non-null value writes the {@code .counts} companion alone, so the codec never sees a
     * document with nothing to store.
     */
    public void testAllNullDocumentWritesNoBlob() {
        final LuceneDocument doc = new LuceneDocument();
        final int nulls = between(1, 5);
        for (int i = 0; i < nulls; i++) {
            ColumnarBinaryDocValuesField.recordNull(doc, FIELD, StringBinaryPayload.Framing.ARRAY_ORDER);
        }
        assertNull("no binary field", doc.getField(FIELD));
        assertEquals("counts", (long) nulls, counts(doc));
    }

    public void testEmptyArrayWritesCountsOnly() {
        final LuceneDocument doc = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordEmptyArray(doc, FIELD, StringBinaryPayload.Framing.ARRAY_ORDER);
        assertNull("no binary field", doc.getField(FIELD));
        assertEquals("counts", 0L, counts(doc));
    }

    /** The single-value fast path must land on the same shape as the general one. */
    public void testRecordSingleValueMatchesRecordValue() {
        final BytesRef value = new BytesRef(randomAlphaOfLengthBetween(1, 30));

        final LuceneDocument viaSingle = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordSingleValue(
            viaSingle,
            FIELD,
            value,
            StringBinaryPayload.Framing.ARRAY_ORDER,
            MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED
        );

        final LuceneDocument viaGeneral = new LuceneDocument();
        ColumnarBinaryDocValuesField.recordValue(
            viaGeneral,
            FIELD,
            value,
            StringBinaryPayload.Framing.ARRAY_ORDER,
            MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED
        );

        assertEquals(viaGeneral.getField(FIELD).binaryValue(), viaSingle.getField(FIELD).binaryValue());
        assertEquals(counts(viaGeneral), counts(viaSingle));
    }

    /** Sorted-unique collection still applies; the payload just records however many survived it. */
    public void testSeparateCountFramingDeduplicates() {
        final LuceneDocument doc = new LuceneDocument();
        for (String value : new String[] { "b", "a", "b", "c", "a" }) {
            ColumnarBinaryDocValuesField.recordValue(
                doc,
                FIELD,
                new BytesRef(value),
                StringBinaryPayload.Framing.SEPARATE_COUNT,
                MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
            );
        }
        assertSlots(doc, List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c")));
        assertEquals("counts", 3L, counts(doc));
    }

    /**
     * A field declared single-valued has nothing for a count to say, so its blob is the value itself and no
     * companion is written — byte for byte what a plain {@code BinaryDocValuesField} would have carried.
     */
    public void testSingleValuedFieldWritesTheValueItself() {
        final LuceneDocument doc = new LuceneDocument();
        final BytesRef value = new BytesRef(randomAlphaOfLengthBetween(0, 30));
        ColumnarBinaryDocValuesField.recordSingleValuedField(doc, FIELD, value);
        assertEquals("the blob is the value", value, doc.getField(FIELD).binaryValue());
        assertNull("no counts companion", doc.getField(COUNTS));
    }

    /** The codec reads its type and framing off the field's attributes, so every shape has to carry them. */
    public void testFieldTypeCarriesTheCodecAttributes() {
        for (StringBinaryPayload.Framing framing : StringBinaryPayload.Framing.values()) {
            final var field = new ColumnarBinaryDocValuesField(FIELD, framing, MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED);
            final var attributes = field.fieldType().getAttributes();
            assertEquals(
                "column type under " + framing,
                ColumnarFieldType.STRING.name(),
                attributes.get(ColumNARDocValuesFormat.TYPE_ATTRIBUTE)
            );
            assertEquals("framing", framing.name(), attributes.get(ColumNARDocValuesFormat.STRING_FRAMING_ATTRIBUTE));
        }
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

    private static long counts(LuceneDocument doc) {
        final IndexableField counts = doc.getField(COUNTS);
        assertNotNull("counts companion", counts);
        return counts.numericValue().longValue();
    }
}
