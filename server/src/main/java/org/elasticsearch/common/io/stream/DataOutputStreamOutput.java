/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;

public class DataOutputStreamOutput extends StreamOutput {

    private final DataOutput out;

    public DataOutputStreamOutput(DataOutput out) {
        this.out = out;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.write(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        // nothing to do there...
    }

    @Override
    public void reset() throws IOException {
        // nothing to do there...
    }

    @Override
    public void close() throws IOException {
        if (out instanceof Closeable) {
            ((Closeable) out).close();
        }
    }
}
