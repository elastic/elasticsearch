/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds a {@code DirectMonotonic} table (block offsets, per-document value addresses) into a temporary
 * file, so the table never sits on the heap while it is being written. On
 * {@link #finish} the temporary data is copied into the column's data output and its small metadata is
 * returned; {@link #close} removes the temporary file.
 *
 * <p>Type-agnostic: every column type addresses its blocks through one of these tables, so this lives in
 * the substrate rather than beside any one column implementation. {@link MonotonicReader} is the read-side
 * counterpart, reopening a finished table over the data input.
 */
public final class MonotonicWriter implements Closeable {

    /** Monotonic block shift for every ColumNAR offset table. Not persisted, so the reader must use the same value. */
    static final int BLOCK_SHIFT = 16;

    /** Location of a finished table in the data file, plus its {@code DirectMonotonic} metadata. */
    public record Table(long dataOffset, long dataLength, byte[] meta) {
        public static final Table NONE = new Table(0, 0, new byte[0]);
    }

    private final Directory directory;
    private final IOContext context;
    private final ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
    private final IndexOutput metaOut;
    private final IndexOutput dataTemp;
    private final String tempName;
    private final DirectMonotonicWriter writer;
    private boolean dataClosed = false;

    public MonotonicWriter(Directory directory, IOContext context, String prefix, long numValues) throws IOException {
        this.directory = directory;
        this.context = context;
        this.metaOut = new ByteBuffersIndexOutput(metaBuffer, "monotonic-meta", "monotonic-meta");
        this.dataTemp = directory.createTempOutput(prefix, "columnar-monotonic", context);
        this.tempName = dataTemp.getName();
        this.writer = DirectMonotonicWriter.getInstance(metaOut, dataTemp, numValues, BLOCK_SHIFT);
    }

    public void add(long value) throws IOException {
        writer.add(value);
    }

    /** Flushes the table, copies its data into {@code data}, and returns where it landed. */
    public Table finish(IndexOutput data) throws IOException {
        writer.finish();
        metaOut.close();
        dataTemp.close();
        dataClosed = true;
        long dataOffset = data.getFilePointer();
        try (IndexInput in = directory.openInput(tempName, context)) {
            data.copyBytes(in, in.length());
        }
        return new Table(dataOffset, data.getFilePointer() - dataOffset, metaBuffer.toArrayCopy());
    }

    @Override
    public void close() throws IOException {
        try {
            if (dataClosed == false) {
                IOUtils.closeWhileHandlingException(metaOut, dataTemp);
            }
        } finally {
            IOUtils.deleteFilesIgnoringExceptions(directory, tempName);
        }
    }
}
