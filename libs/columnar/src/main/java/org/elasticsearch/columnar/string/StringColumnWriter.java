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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Writes a string column — single- or multi-valued. Slots are written in the order the
 * {@link StringColumnValues} cursor yields them and are never reordered.
 *
 * <p>Nothing column-proportional is held on the heap: the values go into a {@link ValueStream}, which streams
 * them a block at a time and writes its offset table to a temporary file, and both address tables — the
 * per-document value addresses and the null slots — go through {@link MonotonicWriter}, which does the same.
 * Blocks address a fixed count of values while chunks bound how many bytes are compressed at once, so a
 * block of long urls and a block of single characters are the same count of values and nothing like the same
 * amount of data.
 *
 * <p>A null slot takes an address like any other, its bytes stored as a zero-length value, and its address is
 * recorded in the null-slot table. That keeps one address space over the column, so a document's slot count
 * is the difference between consecutive value addresses whether or not any of its slots are null.
 *
 * <p>Only {@link StringColumnLayout#PLAIN} is written today; the layout is recorded so an ordinal layout can
 * arrive as a new id. See {@code docs/PLAN.md} for how that decision is meant to be reached.
 */
public final class StringColumnWriter {

    private StringColumnWriter() {}

    /**
     * Encodes a string column into {@code data}: iterator metadata, the block-encoded values, then the block
     * offset table and the two address tables; returns the metadata needed to reconstruct the column at read
     * time.
     *
     * @param maxDoc           documents in the segment
     * @param numDocsWithField documents that have at least one slot
     * @param numValues        total number of slots across all documents, null slots included
     * @param numNullSlots     how many of those slots are null; the null-slot table is written only when
     *                         this is positive
     * @param cursors          supplies fresh forward cursors over the documents that have a slot; called
     *                         once for the iterator and once for the values
     * @param valuesPerBlock   values behind one offset in the byte stream
     * @param chunkCodec       how a chunk of the byte stream is compressed
     * @param targetChunkBytes bytes a chunk holds before it is closed
     * @param directory        directory used for the temporary table files
     * @param context          IO context for the temporary table files
     * @param data             data output (iterator, value blocks, and the tables are appended)
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        long numNullSlots,
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

        // A document holding several slots and one holding none both put the slots out of step with the
        // documents, and either way a rank stops being its own value address.
        final boolean valueAddresses = numValues != numDocsWithField;
        final boolean hasNullSlots = numNullSlots > 0;

        ValueStream.Writer stream = null;
        MonotonicWriter addresses = null;
        MonotonicWriter nullSlots = null;
        try {
            stream = new ValueStream.Writer(
                chunkCodec,
                targetChunkBytes,
                valuesPerBlock,
                numValues,
                directory,
                context,
                data.getName(),
                data
            );
            if (valueAddresses) {
                addresses = new MonotonicWriter(directory, context, data.getName(), numDocsWithField + 1L);
            }
            if (hasNullSlots) {
                nullSlots = new MonotonicWriter(directory, context, data.getName(), numNullSlots);
            }

            final BytesRef empty = new BytesRef(BytesRef.EMPTY_BYTES);
            long valueAddress = 0;
            StringColumnValues values = cursors.get();
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (valueAddresses) {
                    addresses.add(valueAddress);
                }
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    BytesRef value = values.nextValue();
                    if (value == null) {
                        assert hasNullSlots : "null slot in a column counted as having none";
                        nullSlots.add(valueAddress);
                        value = empty;
                    }
                    stream.add(value);
                    valueAddress++;
                }
            }
            if (valueAddresses) {
                addresses.add(valueAddress);
            }
            assert valueAddress == numValues : "wrote " + valueAddress + " slots, counted " + numValues;

            final ValueStream.Metadata written = stream.finish();
            MonotonicWriter.Table addressTable = valueAddresses ? addresses.finish(data) : MonotonicWriter.Table.NONE;
            MonotonicWriter.Table nulls = hasNullSlots ? nullSlots.finish(data) : MonotonicWriter.Table.NONE;

            return new StringColumnMetadata(
                iterator,
                numDocsWithField,
                numValues,
                numNullSlots,
                StringColumnLayout.PLAIN,
                written,
                addressTable.dataOffset(),
                addressTable.dataLength(),
                addressTable.meta(),
                nulls.dataOffset(),
                nulls.dataLength(),
                nulls.meta()
            );
        } finally {
            IOUtils.close(stream, addresses, nullSlots);
        }
    }

}
