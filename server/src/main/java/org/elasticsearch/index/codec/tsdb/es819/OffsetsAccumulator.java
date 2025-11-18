/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;

/**
 * Builds the doc values address offset table iteratively, one document at a time. Useful to avoid a separate docvalues iteration
 * to build the address offset table.
 */
final class OffsetsAccumulator implements Closeable {
    private final Directory dir;
    private final IOContext context;

    private final ByteBuffersDataOutput addressMetaBuffer;
    private final ByteBuffersIndexOutput addressMetaOutput;
    private final IndexOutput addressDataOutput;
    private final DirectMonotonicWriter addressesWriter;

    private final String addressOffsetsTempFileName;

    private long addr = 0;

    OffsetsAccumulator(Directory dir, IOContext context, IndexOutput data, long numDocsWithField) throws IOException {
        this.dir = dir;
        this.context = context;

        addressMetaBuffer = new ByteBuffersDataOutput();
        addressMetaOutput = new ByteBuffersIndexOutput(addressMetaBuffer, "meta-temp", "meta-temp");
        addressDataOutput = dir.createTempOutput(data.getName(), "address-data", context);
        addressOffsetsTempFileName = addressDataOutput.getName();
        addressesWriter = DirectMonotonicWriter.getInstance(
            addressMetaOutput,
            addressDataOutput,
            numDocsWithField + 1L,
            ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
        );
    }

    public void addDoc(int docValueCount) throws IOException {
        addressesWriter.add(addr);
        addr += docValueCount;
    }

    public void build(IndexOutput meta, IndexOutput data) throws IOException {
        addressesWriter.add(addr);
        addressesWriter.finish();
        long start = data.getFilePointer();
        meta.writeLong(start);
        meta.writeVInt(ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
        addressMetaBuffer.copyTo(meta);
        addressDataOutput.close();
        try (var addressDataInput = dir.openInput(addressOffsetsTempFileName, context)) {
            data.copyBytes(addressDataInput, addressDataInput.length());
            meta.writeLong(data.getFilePointer() - start);
        }
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    public void close() throws IOException {
        IOUtils.close(addressMetaOutput, addressDataOutput);
        if (addressOffsetsTempFileName != null) {
            IOUtils.deleteFilesIgnoringExceptions(dir, addressOffsetsTempFileName);
        }
    }
}
