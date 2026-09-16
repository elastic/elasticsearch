/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Reopens a {@code DirectMonotonic} table written by {@link MonotonicWriter}. Read-side counterpart to that
 * class, the same way {@link ColumnIteratorReader} pairs with {@link ColumnIteratorWriter}.
 *
 * <p>The returned {@link LongValues} reads off-heap, straight from a mapped slice of the data file, so
 * reopening a table costs only its small metadata.
 */
public final class MonotonicReader {

    private MonotonicReader() {}

    /**
     * Opens the table stored at {@code [dataOffset, dataOffset + dataLength)} in {@code data}.
     *
     * @param data       the data file the table was written into
     * @param tableMeta  the {@link MonotonicWriter.Table#meta()} bytes held in the column's metadata
     * @param numEntries the number of entries the table was built with; must match what the writer was given
     * @param dataOffset start of the table's region in {@code data}
     * @param dataLength length of the table's region
     */
    public static LongValues open(IndexInput data, byte[] tableMeta, long numEntries, long dataOffset, long dataLength) throws IOException {
        DirectMonotonicReader.Meta meta;
        try (
            IndexInput metaInput = new ByteBuffersIndexInput(
                new ByteBuffersDataInput(List.of(ByteBuffer.wrap(tableMeta))),
                "monotonic-meta"
            )
        ) {
            // The block shift is frozen on the write side; a reader must decode with the same value.
            meta = DirectMonotonicReader.loadMeta(metaInput, numEntries, MonotonicWriter.BLOCK_SHIFT);
        }
        return DirectMonotonicReader.getInstance(meta, data.randomAccessSlice(dataOffset, dataLength));
    }
}
