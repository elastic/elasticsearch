/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;

import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * The binary doc values of a numeric field whose doc values are stored by the ColumNAR codec, which takes numeric columns
 * as {@link NumericBinaryPayload}s rather than as {@code SORTED_NUMERIC}: one payload per document holding every value the
 * document has for the field, each in the same sortable-long encoding the field would write to {@code SORTED_NUMERIC}.
 *
 * <p>The values are written sorted, duplicates kept, so a document's payload holds exactly what {@code SORTED_NUMERIC}
 * would. That keeps the readers of {@code SORTED_NUMERIC} and the {@code .offsets} companion, which indexes into the
 * sorted values, working over the column unchanged. This is interim: the codec can keep the values in document order
 * itself, which will make the companion unnecessary.
 *
 * <p>A document with no value for the field writes no payload, as it writes no {@code SORTED_NUMERIC} value.
 */
public final class ColumnarNumericDocValuesField extends CustomDocValuesField {

    private long[] values = new long[1];
    private int count;
    private final BytesRefBuilder payload = new BytesRefBuilder();

    private ColumnarNumericDocValuesField(String name) {
        super(name);
    }

    private void add(long value) {
        values = ArrayUtil.grow(values, count + 1);
        values[count++] = value;
    }

    /**
     * This document's values as a payload. The bytes are the builder's own, so they are valid until the next call on this
     * field, which is all Lucene needs, since it copies the value into the doc values writer as soon as it is handed over.
     */
    @Override
    public BytesRef binaryValue() {
        // Sorted only to match SORTED_NUMERIC, see the class javadoc.
        Arrays.sort(values, 0, count);
        return NumericBinaryPayload.encode(values, count, payload);
    }

    /**
     * Hands each of this document's values to {@code consumer}, for the readers over an in-memory document, which serve these
     * values as {@code SORTED_NUMERIC} without a codec in between.
     */
    void forEachValue(LongConsumer consumer) {
        for (int i = 0; i < count; i++) {
            consumer.accept(values[i]);
        }
    }

    /**
     * Records {@code value} into the document's accumulator for {@code fieldName}, creating and registering it on first use.
     */
    public static void recordValue(LuceneDocument doc, String fieldName, long value) {
        var field = (ColumnarNumericDocValuesField) doc.getOrAddWithKey(fieldName, key -> {
            var created = new ColumnarNumericDocValuesField(fieldName);
            doc.add(created);
            return created;
        });
        field.add(value);
    }
}
