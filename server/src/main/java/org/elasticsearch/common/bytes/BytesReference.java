/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;


/**
 * A reference to bytes.
 */
public interface BytesReference extends Comparable<BytesReference>, ToXContentFragment {

    /**
     * Convert an {@link XContentBuilder} into a BytesReference. This method closes the builder,
     * so no further fields may be added.
     */
    static BytesReference bytes(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream) {
            return new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            return ((BytesStream) stream).bytes();
        }
    }

    /**
     * Returns a compact array from the given BytesReference. The returned array won't be copied unless necessary. If you need
     * to modify the returned array use {@code BytesRef.deepCopyOf(reference.toBytesRef()} instead
     */
    static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }

    /**
     * Returns an array of byte buffers from the given BytesReference.
     */
    static ByteBuffer[] toByteBuffers(BytesReference reference) {
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        try {
            ArrayList<ByteBuffer> buffers = new ArrayList<>();
            while ((r = byteRefIterator.next()) != null) {
                buffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
            }
            return buffers.toArray(new ByteBuffer[buffers.size()]);

        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffers.
     */
    static BytesReference fromByteBuffers(ByteBuffer[] buffers) {
        int bufferCount = buffers.length;
        if (bufferCount == 0) {
            return BytesArray.EMPTY;
        } else if (bufferCount == 1) {
            return fromByteBuffer(buffers[0]);
        } else {
            BytesReference[] references = new BytesReference[bufferCount];
            for (int i = 0; i < bufferCount; ++i) {
                references[i] = fromByteBuffer(buffers[i]);
            }

            return CompositeBytesReference.of(references);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffer.
     */
    static BytesReference fromByteBuffer(ByteBuffer buffer) {
        assert buffer.hasArray();
        return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    }

    /**
     * Returns BytesReference either wrapping the provided {@link ByteArray} or in case the has a backing raw byte array one that wraps
     * that backing array directly.
     */
    static BytesReference fromByteArray(ByteArray byteArray, int length) {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        if (byteArray.hasArray()) {
            return new BytesArray(byteArray.array(), 0, length);
        }
        return new PagedBytesReference(byteArray, 0, length);
    }

    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     */
    byte get(int index);

    /**
     * Returns the integer read from the 4 bytes (BE) starting at the given index.
     */
    int getInt(int index);

    /**
     * Finds the index of the first occurrence of the given marker between within the given bounds.
     * @param marker marker byte to search
     * @param from lower bound for the index to check (inclusive)
     * @return first index of the marker or {@code -1} if not found
     */
    int indexOf(byte marker, int from);

    /**
     * The length.
     */
    int length();

    /**
     * Slice the bytes from the {@code from} index up to {@code length}.
     */
    BytesReference slice(int from, int length);

    /**
     * The amount of memory used by this BytesReference
     */
    long ramBytesUsed();

    /**
     * A stream input of the bytes.
     */
    StreamInput streamInput() throws IOException;

    /**
     * Writes the bytes directly to the output stream.
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * Interprets the referenced bytes as UTF8 bytes, returning the resulting string
     */
    String utf8ToString();

    /**
     * Converts to Lucene BytesRef.
     */
    BytesRef toBytesRef();

    /**
     * Returns a BytesRefIterator for this BytesReference. This method allows
     * access to the internal pages of this reference without copying them. Use with care!
     * @see BytesRefIterator
     */
    BytesRefIterator iterator();

    /**
     * @return {@code true} if this instance is backed by a byte array
     */
    default boolean hasArray() {
        return false;
    }

    /**
     * @return backing byte array for this instance
     */
    default byte[] array() {
        throw new UnsupportedOperationException();
    }

    /**
     * @return offset of the first byte of this instance in the backing byte array
     */
    default int arrayOffset() {
        throw new UnsupportedOperationException();
    }
}
