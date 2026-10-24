/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnTransforms;
import org.elasticsearch.index.mapper.SourceColumn;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The direct {@code columnar_stored} source writer of a {@code flattened} field on the columnar batch path. It renders the same
 * object that {@link FlattenedDocValuesSyntheticFieldLoader} renders from the {@code _keyed} array-order blob the mapper has just
 * written, but reads the group's input columns a second time instead of decoding that blob.
 *
 * <p>The loader decodes each slot back into a key and a value, groups the slots by key in a {@code TreeMap}, and hands the groups to
 * {@link FlattenedFieldSyntheticWriterHelper#writeNested} in key order. Here the keys are known before the first document: they are the
 * group's relative keys, so they are parsed into {@link FlattenedFieldSyntheticWriterHelper.FlattenedKey}s and sorted once, and a
 * document only has to read its values. The order within one key is the order the blob lists them in, which is column order, so a key
 * spelled twice in a document ({@code "a.b"} and {@code "a": {"b": ...}}) merges in the same order on both sides. The rendering itself
 * stays with the writer helper, because the object reconstruction (dotted keys, a scalar and an object sharing a prefix) is its
 * contract and not one to copy.
 */
final class FlattenedArrayOrderSourceLayer implements SourceColumn.Layer {

    /** The group's columns in ascending key order; columns with the same key keep their column order. */
    private final int[] columnsByKey;
    private final FlattenedFieldSyntheticWriterHelper.FlattenedKey[] keys;
    private final List<ObjectTupleCursor<BytesRef>> cursors;
    /** Doc each cursor is positioned on, or {@link DocIdSetIterator#NO_MORE_DOCS} once drained. */
    private final int[] cursorDocs;
    @Nullable
    private final String nullValue;

    /** The fields of the current document, in key order; empty when the document has no slot. */
    private final List<FlattenedFieldSyntheticWriterHelper.OrderedField> fields = new ArrayList<>();

    /**
     * @param columns      the group's columns, as handed to the mapper
     * @param relativeKeys {@code relativeKeys[k]} is the flattened key of {@code columns[k]}
     * @param nullValue    the {@code null_value} of the field, which the mapper writes in place of a null slot, or {@code null}
     */
    FlattenedArrayOrderSourceLayer(EscfColumn[] columns, String[] relativeKeys, @Nullable String nullValue) {
        final int columnCount = columns.length;
        final Integer[] order = new Integer[columnCount];
        for (int k = 0; k < columnCount; k++) {
            order[k] = k;
        }
        // The loader groups by key in a TreeMap<String, ...>, so the order is String's natural order; Arrays.sort is stable.
        Arrays.sort(order, (a, b) -> relativeKeys[a].compareTo(relativeKeys[b]));
        this.columnsByKey = new int[columnCount];
        this.keys = new FlattenedFieldSyntheticWriterHelper.FlattenedKey[columnCount];
        this.cursors = new ArrayList<>(columnCount);
        this.cursorDocs = new int[columnCount];
        for (int k = 0; k < columnCount; k++) {
            columnsByKey[k] = order[k];
            keys[k] = new FlattenedFieldSyntheticWriterHelper.FlattenedKey(relativeKeys[k]);
            // retainValues=false: a value is copied into its String before the cursor moves past it.
            final ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(columns[k], false);
            cursors.add(cursor);
            cursorDocs[k] = cursor.nextDoc();
        }
        this.nullValue = nullValue;
    }

    @Override
    public void advance(int doc) {
        fields.clear();
        int i = 0;
        while (i < columnsByKey.length) {
            final int first = columnsByKey[i];
            final String key = keys[first].fullPath();
            List<String> values = null;
            // All columns with this key, in column order.
            int j = i;
            do {
                final int k = columnsByKey[j];
                final ObjectTupleCursor<BytesRef> cursor = cursors.get(k);
                while (cursorDocs[k] == doc) {
                    if (values == null) {
                        values = new ArrayList<>();
                    }
                    final BytesRef value = cursor.value();
                    values.add(value == null ? nullValue : value.utf8ToString());
                    cursorDocs[k] = cursor.nextDoc();
                }
                j++;
            } while (j < columnsByKey.length && keys[columnsByKey[j]].fullPath().equals(key));
            if (values != null) {
                fields.add(new FlattenedFieldSyntheticWriterHelper.OrderedField(keys[first], values));
            }
            i = j;
        }
    }

    @Override
    public long valueCount() {
        // The whole object is the field's one value, exactly when the mapper wrote a blob for the document (any slot, null included).
        return fields.isEmpty() ? 0 : 1;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        final int[] next = { 0 };
        final FlattenedFieldSyntheticWriterHelper.KeyedValueProducer producer = () -> next[0] < fields.size()
            ? fields.get(next[0]++)
            : null;
        b.startObject();
        new FlattenedFieldSyntheticWriterHelper(producer, List.of()).writeNested(b);
        b.endObject();
    }
}
