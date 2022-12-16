/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A @link {@link StreamOutput} that is backed by a {@link BytesRef}.
 * This is useful for small data, for larger or unknown sizes use {@link BytesStreamOutput} instead.
 *
 * Compared to {@link BytesStreamOutput} this class avoids copying the bytes ref.
 * Compared to {@link BytesRefBuilder} this class supports writing all the rich data types that {@link StreamOutput} supports.
 */
public class BytesRefStreamOutput extends StreamOutput implements Accountable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefStreamOutput.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRefBuilder.class);
    private final BytesRefBuilder builder = new BytesRefBuilder();

    public BytesRef get() {
        return builder.get();
    }

    @Override
    public long position() {
        return builder.length();
    }

    @Override
    public void writeByte(byte b) {
        builder.append(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        builder.append(b, offset, length);
    }

    @Override
    public void flush() {}

    /**
     * Closes this stream to further operations.
     *
     * This is a no-op, as the underlying BytesRefBuilder has no IO resources.
     */
    @Override
    public void close() {}

    public void reset() {
        builder.clear();
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + builder.bytes().length;
    }

    // for tests
    byte[] bytes() {
        return builder.bytes();
    }
}
