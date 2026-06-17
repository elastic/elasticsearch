/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A test utility that wraps an {@link IndexInput} and implements {@link DirectAccessInput},
 * serving direct {@link ByteBuffer} slices backed by {@link CloseableByteBuffer} for
 * deterministic native memory management. The buffers are allocated via {@link NativeAccess}
 * and freed eagerly when the action completes.
 */
public class DirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

    private final byte[] data;
    private final NativeAccess nativeAccess;

    public DirectAccessIndexInput(String resourceDescription, IndexInput delegate, byte[] data, NativeAccess nativeAccess) {
        super(resourceDescription, delegate);
        this.data = data;
        this.nativeAccess = nativeAccess;
    }

    @Override
    public boolean withByteBufferSlice(long offset, long length, CheckedConsumer<ByteBuffer, IOException> action) throws IOException {
        try (CloseableByteBuffer cbuf = nativeAccess.newConfinedBuffer((int) length)) {
            cbuf.buffer().put(0, data, (int) offset, (int) length);
            action.accept(cbuf.buffer().asReadOnlyBuffer());
        }
        return true;
    }

    @Override
    public boolean withByteBufferSlices(long[] offsets, int length, int count, CheckedConsumer<ByteBuffer[], IOException> action)
        throws IOException {
        if (DirectAccessInput.checkSlicesArgs(offsets, count)) {
            return true;
        }
        CloseableByteBuffer[] cbufs = new CloseableByteBuffer[count];
        try {
            ByteBuffer[] views = new ByteBuffer[count];
            for (int i = 0; i < count; i++) {
                cbufs[i] = nativeAccess.newConfinedBuffer(length);
                cbufs[i].buffer().put(0, data, (int) offsets[i], length);
                views[i] = cbufs[i].buffer().asReadOnlyBuffer();
            }
            action.accept(views);
        } finally {
            for (CloseableByteBuffer cbuf : cbufs) {
                if (cbuf != null) {
                    cbuf.close();
                }
            }
        }
        return true;
    }

    @Override
    public IndexInput clone() {
        return new DirectAccessIndexInput("clone(" + toString() + ")", in.clone(), data, nativeAccess);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new DirectAccessIndexInput(sliceDescription, in.slice(sliceDescription, offset, length), data, nativeAccess);
    }
}
