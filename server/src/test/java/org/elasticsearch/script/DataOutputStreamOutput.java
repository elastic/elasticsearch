/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.StreamOutputHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

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
    public long position() {
        return ESTestCase.fail(new UnsupportedOperationException("not implemented"));
    }

    @Override
    public void writeString(String str) throws IOException {
        StreamOutputHelper.writeString(str, this);
    }

    @Override
    public void writeOptionalString(@Nullable String str) throws IOException {
        StreamOutputHelper.writeOptionalString(str, this);
    }

    @Override
    public void writeGenericString(String value) throws IOException {
        StreamOutputHelper.writeGenericString(value, this);
    }

    @Override
    public void flush() throws IOException {
        // nothing to do there...
    }

    @Override
    public void close() throws IOException {
        if (out instanceof Closeable) {
            ((Closeable) out).close();
        }
    }
}
