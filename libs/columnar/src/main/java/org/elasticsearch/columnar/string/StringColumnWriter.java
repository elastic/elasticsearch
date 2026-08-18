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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Writes a string column. Values are written in the order the {@link StringColumnValues} cursor yields them and
 * are never reordered.
 *
 * <p>{@link StringColumnLayout#PLAIN} appends each value's bytes to the data file and records its byte offset in
 * a {@link MonotonicWriter} table. There is no block, so nothing accumulates: a value is copied straight from
 * the cursor to the output, and the offset table replaces the length prefix a block layout would need inline.
 * Nothing column-proportional is held on the heap — the offset table is built into a temporary file.
 */
public final class StringColumnWriter {

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code data}: iterator metadata, the value bytes, then the offset table;
     * returns the metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value; called
     *                         once for the iterator and once for the values
     * @param directory        directory used for the temporary table file
     * @param context          IO context for the temporary table file
     * @param data             data output (iterator, values, and the offset table are appended)
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(iterator);
        }

        long valuesOffset = data.getFilePointer();

        // One offset per value plus a past-the-end marker, so a value's length is the gap to the next offset.
        try (MonotonicWriter valueOffsets = new MonotonicWriter(directory, context, data.getName(), numValues + 1L)) {
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                int count = values.valueCount();
                for (int i = 0; i < count; i++) {
                    valueOffsets.add(data.getFilePointer() - valuesOffset);
                    BytesRef value = values.nextValue();
                    data.writeBytes(value.bytes, value.offset, value.length);
                }
            }
            valueOffsets.add(data.getFilePointer() - valuesOffset);

            MonotonicWriter.Table offsets = valueOffsets.finish(data);

            return new StringColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                StringColumnLayout.PLAIN,
                valuesOffset,
                offsets.dataOffset(),
                offsets.dataLength(),
                offsets.meta()
            );
        }
    }
}
