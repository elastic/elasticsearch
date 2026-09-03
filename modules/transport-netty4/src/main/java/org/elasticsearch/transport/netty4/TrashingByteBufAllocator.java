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

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

class TrashingByteBufAllocator extends NettyAllocator.NoDirectBuffers {

    static int DEFAULT_MAX_COMPONENTS = 16;

    TrashingByteBufAllocator(ByteBufAllocator delegate) {
        super(delegate);
    }

    static void trashBuffer(ByteBuf buf) {
        for (var nioBuf : buf.nioBuffers(0, buf.capacity())) {
            if (nioBuf.hasArray()) {
                var from = nioBuf.arrayOffset() + nioBuf.position();
                var to = from + nioBuf.remaining();
                Arrays.fill(nioBuf.array(), from, to, (byte) 0);
            }
        }
    }

    @Override
    public ByteBuf heapBuffer() {
        return new TrashingByteBuf(super.heapBuffer());
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return new TrashingByteBuf(super.heapBuffer(initialCapacity));
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return new TrashingByteBuf(super.heapBuffer(initialCapacity, maxCapacity));
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return new TrashingCompositeByteBuf(this, false, DEFAULT_MAX_COMPONENTS);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return new TrashingCompositeByteBuf(this, false, maxNumComponents);
    }

    interface Trashable {

        void maybeTrash();
    }

    static class TrashingByteBuf extends WrappedByteBuf implements Trashable {

        private final AtomicBoolean trashed = new AtomicBoolean();

        protected TrashingByteBuf(ByteBuf buf) {
            super(buf);
        }

        static TrashingByteBuf newBuf(ByteBuf buf) {
            return new TrashingByteBuf(buf);
        }

        @Override
        public void maybeTrash() {
            maybeTrash(1);
        }

        private void maybeTrash(int decrement) {
            if (refCnt() == decrement && decrement > 0 && trashed.compareAndSet(false, true)) {
                // see [NOTE on racy trashContent() calls]
                trashContent();
            }
        }

        @Override
        public boolean release() {
            maybeTrash(1);
            return super.release();
        }

        @Override
        public boolean release(int decrement) {
            maybeTrash(decrement);
            return super.release(decrement);
        }

        // [NOTE on racy trashContent() calls]: We trash the buffer content _before_ reducing the ref
        // count to zero, which looks racy because in principle a concurrent caller could come along
        // and successfully retain() this buffer to keep it alive after it's been trashed. Such a
        // caller would sometimes get an IllegalReferenceCountException ofc but that's something it
        // could handle - see for instance org.elasticsearch.transport.netty4.Netty4Utils.ByteBufRefCounted.tryIncRef.
        // Yet in practice this should never happen, we only ever retain() these buffers while we
        // know them to be alive (i.e. via RefCounted#mustIncRef or its moral equivalents) so it'd
        // be a bug for a caller to retain() a buffer whose ref count is heading to zero and whose
        // contents we've already decided to trash.
        private void trashContent() {
            trashBuffer(buf);
        }

        @Override
        public ByteBuf order(ByteOrder endianness) {
            return newBuf(super.order(endianness));
        }

        @Override
        public ByteBuf asReadOnly() {
            return newBuf(super.asReadOnly());
        }

        @Override
        public ByteBuf readBytes(int length) {
            return newBuf(super.readBytes(length));
        }

        @Override
        public ByteBuf readSlice(int length) {
            return newBuf(super.readSlice(length));
        }

        @Override
        public ByteBuf readRetainedSlice(int length) {
            return new TrashingByteSlice(super.readRetainedSlice(length), this);
        }

        @Override
        public ByteBuf copy() {
            return newBuf(super.copy());
        }

        @Override
        public ByteBuf copy(int index, int length) {
            return newBuf(super.copy(index, length));
        }

        @Override
        public ByteBuf slice() {
            return newBuf(super.slice());
        }

        @Override
        public ByteBuf slice(int index, int length) {
            return newBuf(super.slice(index, length));
        }

        @Override
        public ByteBuf duplicate() {
            return newBuf(super.duplicate());
        }

        @Override
        public ByteBuf retainedSlice() {
            return new TrashingByteSlice(super.retainedSlice(), this);
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return new TrashingByteSlice(super.retainedSlice(index, length), this);
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return new TrashingByteSlice(super.retainedDuplicate(), this);
        }
    }

    static class TrashingByteSlice extends WrappedByteBuf implements Trashable {

        private final Trashable parent;

        TrashingByteSlice(ByteBuf buf, Trashable parent) {
            super(buf);
            this.parent = parent;
        }

        @Override
        public void maybeTrash() {
            maybeTrash(1);
        }

        private void maybeTrash(int decrement) {
            if (refCnt() == decrement && decrement > 0) {
                parent.maybeTrash();
            }
        }

        @Override
        public boolean release() {
            maybeTrash(1);
            return super.release();
        }

        @Override
        public boolean release(int decrement) {
            maybeTrash(decrement);
            return super.release(decrement);
        }

        @Override
        public ByteBuf readRetainedSlice(int length) {
            return new TrashingByteSlice(super.readRetainedSlice(length), this);
        }

        @Override
        public ByteBuf retainedSlice() {
            return new TrashingByteSlice(super.retainedSlice(), this);
        }

        @Override
        public ByteBuf retainedSlice(int index, int length) {
            return new TrashingByteSlice(super.retainedSlice(index, length), this);
        }

        @Override
        public ByteBuf retainedDuplicate() {
            return new TrashingByteSlice(super.retainedDuplicate(), this);
        }
    }

    static class TrashingCompositeByteBuf extends CompositeByteBuf {

        TrashingCompositeByteBuf(ByteBufAllocator alloc, boolean direct, int maxNumComponents) {
            super(alloc, direct, maxNumComponents);
        }

        @Override
        protected void deallocate() {
            trashBuffer(this);
            super.deallocate();
        }
    }
}
