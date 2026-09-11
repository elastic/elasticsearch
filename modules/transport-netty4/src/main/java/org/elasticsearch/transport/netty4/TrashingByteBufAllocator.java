/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

import java.util.Arrays;

/**
 * A plain heap buffer is handed out wrapped in a {@link CompositeByteBuf} holding it as its only component, not for
 * composition, but because that is the only public netty buffer which owns a reference count while taking its storage
 * from a buffer we supply. It gives us {@code deallocate()}, which netty calls exactly once, on the CAS that drops the
 * reference count to zero, and which is the only safe point at which to zero the content.
 */
class TrashingByteBufAllocator extends NettyAllocator.NoDirectBuffers {

    static final int DEFAULT_MAX_COMPONENTS = 16;

    TrashingByteBufAllocator(ByteBufAllocator delegate) {
        super(delegate);
    }

    static void trashBuffer(ByteBuf buf) {
        for (var nioBuf : buf.nioBuffers(0, buf.writerIndex())) {
            if (nioBuf.hasArray()) {
                var from = nioBuf.arrayOffset() + nioBuf.position();
                var to = from + nioBuf.remaining();
                Arrays.fill(nioBuf.array(), from, to, (byte) 0);
            }
        }
    }

    @Override
    public ByteBuf heapBuffer() {
        return new TrashingByteBuf(this, super.heapBuffer());
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return new TrashingByteBuf(this, super.heapBuffer(initialCapacity));
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return new TrashingByteBuf(this, super.heapBuffer(initialCapacity, maxCapacity));
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return new TrashingCompositeByteBuf(this, DEFAULT_MAX_COMPONENTS);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return new TrashingCompositeByteBuf(this, maxNumComponents);
    }

    abstract static class SlicingCompositeByteBuf extends CompositeByteBuf {

        SlicingCompositeByteBuf(ByteBufAllocator alloc, int maxNumComponents) {
            super(alloc, false, maxNumComponents);
        }

        protected final void wrapSingleComponent(ByteBuf buf) {
            maxCapacity(buf.maxCapacity());
            var readerIndex = buf.readerIndex();
            var writerIndex = buf.writerIndex();
            addComponent(false, buf.setIndex(0, buf.capacity()));
            setIndex(readerIndex, writerIndex);
        }

        @Override
        public ByteBuf retainedSlice() {
            return retainedSlice(readerIndex(), readableBytes());
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return new RetainedSlice(slice(index, length).retain());
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return new RetainedSlice(duplicate().retain());
        }
    }

    static final class RetainedSlice extends SlicingCompositeByteBuf {

        RetainedSlice(ByteBuf derived) {
            super(derived.alloc(), 1);
            wrapSingleComponent(derived);
        }
    }

    static class TrashingCompositeByteBuf extends SlicingCompositeByteBuf {

        TrashingCompositeByteBuf(ByteBufAllocator alloc, int maxNumComponents) {
            super(alloc, maxNumComponents);
        }

        @Override
        protected final void deallocate() {
            trashBuffer(this);
            super.deallocate();
        }
    }

    static final class TrashingByteBuf extends TrashingCompositeByteBuf {

        TrashingByteBuf(ByteBufAllocator alloc, ByteBuf buf) {
            super(alloc, 1);
            wrapSingleComponent(buf);
        }
    }
}
