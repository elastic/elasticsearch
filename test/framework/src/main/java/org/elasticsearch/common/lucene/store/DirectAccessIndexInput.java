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
import org.elasticsearch.foreign.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A test utility that wraps an {@link IndexInput} and implements {@link DirectAccessInput},
 * serving direct {@link MemorySegment} slices backed by {@link CloseableByteBuffer} for
 * deterministic native memory management. The buffers are allocated via {@link NativeAccess}
 * and freed eagerly when the action completes.
 */
public class DirectAccessIndexInput extends FilterIndexInput implements DirectAccessInput {

    private final byte[] data;

    public DirectAccessIndexInput(String resourceDescription, IndexInput delegate, byte[] data) {
        super(resourceDescription, delegate);
        this.data = data;
    }

    @Override
    public boolean withMemorySegmentSlice(long offset, long length, CheckedConsumer<MemorySegment, IOException> action) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            var seg = arena.allocate((int) length);
            MemorySegment.copy(data, (int) offset, seg, ValueLayout.JAVA_BYTE, 0L, (int) length);
            action.accept(seg);
        }
        return true;
    }

    @Override
    public boolean withSliceAddresses(
        long[] offsets,
        int length,
        int count,
        MemorySegment addressesScratch,
        CheckedConsumer<MemorySegment, IOException> action
    ) throws IOException {
        if (DirectAccessInput.checkSlicesArgs(offsets, count, addressesScratch)) {
            return true;
        }
        // Test impl: allocate each slice into a confined arena that lives for the action, write the addresses.
        try (Arena arena = Arena.ofConfined()) {
            for (int i = 0; i < count; i++) {
                MemorySegment seg = arena.allocate(length);
                MemorySegment.copy(data, (int) offsets[i], seg, ValueLayout.JAVA_BYTE, 0L, length);
                addressesScratch.setAtIndex(ValueLayout.JAVA_LONG, i, seg.address());
            }
            action.accept(addressesScratch);
        }
        return true;
    }

    @Override
    public IndexInput clone() {
        return new DirectAccessIndexInput("clone(" + toString() + ")", in.clone(), data);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new DirectAccessIndexInput(sliceDescription, in.slice(sliceDescription, offset, length), data);
    }
}
