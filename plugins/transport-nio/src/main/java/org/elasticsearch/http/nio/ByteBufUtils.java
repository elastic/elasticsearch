/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ByteBufUtils {

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        final BytesRefIterator iterator = reference.iterator();
        // usually we have one, two, or three components from the header, the message, and a buffer
        final List<ByteBuf> buffers = new ArrayList<>(3);
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
            }

            if (buffers.size() == 1) {
                return buffers.get(0);
            } else {
                CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                composite.addComponents(true, buffers);
                return composite;
            }
        } catch (IOException ex) {
            throw new AssertionError("no IO happens here", ex);
        }
    }

    static BytesReference toBytesReference(final ByteBuf buffer) {
        final int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
            return BytesArray.EMPTY;
        } else if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), readableBytes);
        } else {
            final ByteBuffer[] byteBuffers = buffer.nioBuffers();
            return BytesReference.fromByteBuffers(byteBuffers);
        }
    }
}
