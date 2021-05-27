/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} that writes into underlying IndexOutput
 */
public class IndexOutputOutputStream extends OutputStream {

    private final IndexOutput out;

    public IndexOutputOutputStream(IndexOutput out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.writeByte((byte) b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.writeBytes(b, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.writeBytes(b, off, len);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
