/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class ChunkedBlobOutputStream<T> extends OutputStream {

    protected final List<T> parts = new ArrayList<>();

    protected ReleasableBytesStreamOutput buffer;

    private final BigArrays bigArrays;

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

    protected final void finishPart(T partId) {
        written += buffer.size();
        parts.add(partId);
        buffer.close();
        buffer = new ReleasableBytesStreamOutput(bigArrays);
    }

    protected abstract void maybeFlushBuffer() throws IOException;
}
