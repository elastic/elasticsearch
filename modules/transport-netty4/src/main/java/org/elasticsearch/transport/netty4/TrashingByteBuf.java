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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class TrashingByteBuf extends WrappedByteBuf {

    private boolean trashed = false;

    protected TrashingByteBuf(ByteBuf buf) {
        super(buf);
    }

    static TrashingByteBuf newBuf(ByteBuf buf) {
        return new TrashingByteBuf(buf);
    }

    @Override
    public boolean release() {
        if (refCnt() == 1) {
            // see [NOTE on racy trashContent() calls]
            trashContent();
        }
        return super.release();
    }

    @Override
    public boolean release(int decrement) {
        if (refCnt() == decrement && refCnt() > 0) {
            // see [NOTE on racy trashContent() calls]
            trashContent();
        }
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
        if (trashed == false) {
            trashed = true;
            NettyAllocator.TrashingByteBufAllocator.trashBuffer(buf);
        }
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        super.capacity(newCapacity);
        return this;
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
    public ByteBuf setIndex(int readerIndex, int writerIndex) {
        super.setIndex(readerIndex, writerIndex);
        return this;
    }

    @Override
    public ByteBuf discardReadBytes() {
        super.discardReadBytes();
        return this;
    }

    @Override
    public ByteBuf discardSomeReadBytes() {
        super.discardSomeReadBytes();
        return this;
    }

    @Override
    public ByteBuf ensureWritable(int minWritableBytes) {
        super.ensureWritable(minWritableBytes);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst) {
        super.getBytes(index, dst);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int length) {
        super.getBytes(index, dst, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        super.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst) {
        super.getBytes(index, dst);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        super.getBytes(index, dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        super.getBytes(index, dst);
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        super.getBytes(index, out, length);
        return this;
    }

    @Override
    public ByteBuf setBoolean(int index, boolean value) {
        super.setBoolean(index, value);
        return this;
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        super.setByte(index, value);
        return this;
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        super.setShort(index, value);
        return this;
    }

    @Override
    public ByteBuf setShortLE(int index, int value) {
        super.setShortLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        super.setMedium(index, value);
        return this;
    }

    @Override
    public ByteBuf setMediumLE(int index, int value) {
        super.setMediumLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setInt(int index, int value) {
        super.setInt(index, value);
        return this;
    }

    @Override
    public ByteBuf setIntLE(int index, int value) {
        super.setIntLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        super.setLong(index, value);
        return this;
    }

    @Override
    public ByteBuf setLongLE(int index, long value) {
        super.setLongLE(index, value);
        return this;
    }

    @Override
    public ByteBuf setChar(int index, int value) {
        super.setChar(index, value);
        return this;
    }

    @Override
    public ByteBuf setFloat(int index, float value) {
        super.setFloat(index, value);
        return this;
    }

    @Override
    public ByteBuf setDouble(int index, double value) {
        super.setDouble(index, value);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src) {
        super.setBytes(index, src);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int length) {
        super.setBytes(index, src, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        super.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src) {
        super.setBytes(index, src);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        super.setBytes(index, src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        super.setBytes(index, src);
        return this;
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
        return newBuf(super.readRetainedSlice(length));
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst) {
        super.readBytes(dst);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int length) {
        super.readBytes(dst, length);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
        super.readBytes(dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf readBytes(byte[] dst) {
        super.readBytes(dst);
        return this;
    }

    @Override
    public ByteBuf readBytes(ByteBuffer dst) {
        super.readBytes(dst);
        return this;
    }

    @Override
    public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
        super.readBytes(dst, dstIndex, length);
        return this;
    }

    @Override
    public ByteBuf readBytes(OutputStream out, int length) throws IOException {
        super.readBytes(out, length);
        return this;
    }

    @Override
    public ByteBuf skipBytes(int length) {
        super.skipBytes(length);
        return this;
    }

    @Override
    public ByteBuf writeBoolean(boolean value) {
        super.writeBoolean(value);
        return this;
    }

    @Override
    public ByteBuf writeByte(int value) {
        super.writeByte(value);
        return this;
    }

    @Override
    public ByteBuf writeShort(int value) {
        super.writeShort(value);
        return this;
    }

    @Override
    public ByteBuf writeShortLE(int value) {
        super.writeShortLE(value);
        return this;
    }

    @Override
    public ByteBuf writeMedium(int value) {
        super.writeMedium(value);
        return this;
    }

    @Override
    public ByteBuf writeMediumLE(int value) {
        super.writeMediumLE(value);
        return this;
    }

    @Override
    public ByteBuf writeInt(int value) {
        super.writeInt(value);
        return this;

    }

    @Override
    public ByteBuf writeIntLE(int value) {
        super.writeIntLE(value);
        return this;
    }

    @Override
    public ByteBuf writeLong(long value) {
        super.writeLong(value);
        return this;
    }

    @Override
    public ByteBuf writeLongLE(long value) {
        super.writeLongLE(value);
        return this;
    }

    @Override
    public ByteBuf writeChar(int value) {
        super.writeChar(value);
        return this;
    }

    @Override
    public ByteBuf writeFloat(float value) {
        super.writeFloat(value);
        return this;
    }

    @Override
    public ByteBuf writeDouble(double value) {
        super.writeDouble(value);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src) {
        super.writeBytes(src);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int length) {
        super.writeBytes(src, length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
        super.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src) {
        super.writeBytes(src);
        return this;
    }

    @Override
    public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
        super.writeBytes(src, srcIndex, length);
        return this;
    }

    @Override
    public ByteBuf writeBytes(ByteBuffer src) {
        super.writeBytes(src);
        return this;
    }

    @Override
    public ByteBuf writeZero(int length) {
        super.writeZero(length);
        return this;
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
    public ByteBuf retainedSlice() {
        return newBuf(super.retainedSlice());
    }

    @Override
    public ByteBuf slice(int index, int length) {
        return newBuf(super.slice(index, length));
    }

    @Override
    public ByteBuf retainedSlice(int index, int length) {
        return newBuf(super.retainedSlice(index, length));
    }

    @Override
    public ByteBuf duplicate() {
        return newBuf(super.duplicate());
    }

    @Override
    public ByteBuf retainedDuplicate() {
        return newBuf(super.retainedDuplicate());
    }

    @Override
    public ByteBuf retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public ByteBuf touch(Object hint) {
        super.touch(hint);
        return this;
    }

    @Override
    public ByteBuf retain() {
        super.retain();
        return this;
    }

    @Override
    public ByteBuf touch() {
        super.touch();
        return this;
    }

    @Override
    public ByteBuf setFloatLE(int index, float value) {
        return super.setFloatLE(index, value);
    }

    @Override
    public ByteBuf setDoubleLE(int index, double value) {
        super.setDoubleLE(index, value);
        return this;
    }

    @Override
    public ByteBuf writeFloatLE(float value) {
        super.writeFloatLE(value);
        return this;
    }

    @Override
    public ByteBuf writeDoubleLE(double value) {
        super.writeDoubleLE(value);
        return this;
    }

    @Override
    public ByteBuf asByteBuf() {
        return this;
    }
}
