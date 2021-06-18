/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class ChunkedBlobOutputStream<T> extends OutputStream {

    /**
     * We buffer 8MB before flushing to storage.
     */
    public static final long FLUSH_BUFFER_BYTES = new ByteSizeValue(8, ByteSizeUnit.MB).getBytes();

    protected final List<T> parts = new ArrayList<>();

    private final BigArrays bigArrays;

    protected ReleasableBytesStreamOutput buffer;

    protected boolean successful = false;

    protected long written = 0L;

    protected ChunkedBlobOutputStream(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        buffer = new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void write(int b) throws IOException {
        buffer.write(b);
        maybeFlushBuffer();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buffer.write(b, off, len);
        maybeFlushBuffer();
    }

    @Override
    public void close() throws IOException {
        try {
            doClose();
        } finally {
            Releasables.close(buffer);
        }
    }

    public void markSuccess() {
        this.successful = true;
    }

    protected final void finishPart(T partId) {
        written += buffer.size();
        parts.add(partId);
        buffer.close();
        // only need a new buffer if we're not done yet
        if (successful) {
            buffer = null;
        } else {
            buffer = new ReleasableBytesStreamOutput(bigArrays);
        }
    }

    protected abstract void maybeFlushBuffer() throws IOException;

    protected abstract void doClose() throws IOException;
}
