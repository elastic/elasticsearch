/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Taken from Elasticsearch - copy of org.elasticsearch.common.io.FastByteArrayOutputStream with some enhancements, mainly in allowing access to the underlying byte[].
 *
 * Similar to {@link java.io.ByteArrayOutputStream} just not synced.
 */
public class FastByteArrayOutputStream extends OutputStream {

    private BytesArray data;

    /**
     * Creates a new byte array output stream. The buffer capacity is
     * initially 1024 bytes, though its size increases if necessary.
     * <p>
     * ES: We use 1024 bytes since we mainly use this to build json/smile
     * content in memory, and rarely does the 32 byte default in ByteArrayOutputStream fits...
     */
    public FastByteArrayOutputStream() {
        this(1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of
     * the specified size, in bytes.
     *
     * @param size the initial size.
     */
    public FastByteArrayOutputStream(int size) {
        Assert.isTrue(size >= 0, "Negative initial size: " + size);
        data = new BytesArray(size);
    }

    public FastByteArrayOutputStream(BytesArray data) {
        this.data = data;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     */
    public void write(int b) {
        data.add(b);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this byte array output stream.
     * <p>
     * <b>NO checks for bounds, parameters must be ok!</b>
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    public void write(byte b[], int off, int len) {
        data.add(b, off, len);
    }

    /**
     * Writes the complete contents of this byte array output stream to
     * the specified output stream argument, as if by calling the output
     * stream's write method using <code>out.write(buf, 0, count)</code>.
     *
     * @param out the output stream to which to write the data.
     * @throws IOException if an I/O error occurs.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(data.bytes, 0, data.size);
    }

    public BytesArray bytes() {
        return data;
    }

    public void setBytes(byte[] data, int size) {
        this.data.bytes(data, size);
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number
     *         of valid bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public long size() {
        return data.length();
    }

    /**
     * Resets the <code>count</code> field of this byte array output
     * stream to zero, so that all currently accumulated output in the
     * output stream is discarded. The output stream can be used again,
     * reusing the already capacity buffer space.
     *
     * @see java.io.ByteArrayInputStream#count
     */
    public void reset() {
        data.reset();
    }

    public String toString() {
        return data.toString();
    }

    /**
     * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods in
     * this class can be called after the stream has been closed without
     * generating an <tt>IOException</tt>.
     */
    public void close() throws IOException {}
}