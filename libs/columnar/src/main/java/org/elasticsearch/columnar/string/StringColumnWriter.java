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
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;

import java.io.IOException;

/**
 * Writes a string column. Values are written in the order the {@link StringColumnValues} cursor yields them and
 * are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: the values go into a {@link ValueStream}, which streams
 * them a block at a time and writes its offset table to a temporary file. Blocks address a fixed count of
 * values while chunks bound how many bytes are compressed at once, so a block of long urls and a block of
 * single characters are the same count of values and nothing like the same amount of data.
 *
 * <p>Only {@link StringColumnLayout#PLAIN} is written today; the layout is recorded so an ordinal layout can
 * arrive as a new id. See {@code docs/PLAN.md} for how that decision is meant to be reached.
 */
public final class StringColumnWriter {

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code data}: iterator metadata, the block-encoded values, then the block
     * offset table; returns the metadata needed to reconstruct the column at read time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one value
     * @param numValues        total number of values across all documents
     * @param cursors          supplies fresh forward cursors over the documents that have a value; called
     *                         once for the iterator and once for the values
     * @param valuesPerBlock   values behind one offset in the byte stream
     * @param chunkCodec       how a chunk of the byte stream is compressed
     * @param targetChunkBytes bytes a chunk holds before it is closed
     * @param directory        directory used for the temporary table file
     * @param context          IO context for the temporary table file
     * @param data             data output (iterator, value blocks, and the offset table are appended)
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        int valuesPerBlock,
        ChunkCodec chunkCodec,
        int targetChunkBytes,
        Directory directory,
        IOContext context,
        IndexOutput data
    ) throws IOException {
        ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.empty(iterator);
        }

        final ValueStream.Metadata written;
        try (
            ValueStream.Writer stream = new ValueStream.Writer(
                chunkCodec,
                targetChunkBytes,
                valuesPerBlock,
                numValues,
                directory,
                context,
                data.getName(),
                data
            )
        ) {
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    stream.add(values.nextValue());
                }
            }
            written = stream.finish();
        }
        return new StringColumnMetadata(iterator, numDocsWithField, numValues, StringColumnLayout.PLAIN, written);
    }

}
