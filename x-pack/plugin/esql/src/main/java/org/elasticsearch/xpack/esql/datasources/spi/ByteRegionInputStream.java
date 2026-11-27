/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.ByteArrayInputStream;

/**
 * {@link ByteArrayInputStream} over a heap region. Format readers may parse
 * {@code array()[position(), limit())} in place instead of copying with
 * {@link java.io.InputStream#readAllBytes()}.
 * <p>
 * {@link #array()} is the whole backing array, often a pooled chunk. Only
 * {@code [position(), limit())} is this stream's data; bytes outside that
 * window belong to slack or adjacent regions and must not be read or written.
 * Callers must not mutate the array. {@link #close()} does not invalidate it;
 * lifetime follows the owning {@link StorageObject} / pooled chunk, not this
 * stream. Do not retain the array after that owner is released.
 * <p>
 * Inspect {@code instanceof ByteRegionInputStream} on the stream returned by
 * {@link StorageObject#newStream()} <em>before</em> wrapping it. Filter wrappers
 * hide the type and force a copy. Any {@code read}/{@code skip} advances
 * {@link #position()} to the first unread byte (e.g. after skipping a leading
 * partial line).
 */
public final class ByteRegionInputStream extends ByteArrayInputStream {

    public ByteRegionInputStream(byte[] buf, int offset, int length) {
        super(validated(buf, offset, length), offset, length);
    }

    private static byte[] validated(byte[] buf, int offset, int length) {
        if (buf == null) {
            throw new IllegalArgumentException("buf must not be null");
        }
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("Invalid region: offset=" + offset + ", length=" + length);
        }
        int end;
        try {
            end = Math.addExact(offset, length);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("offset + length overflows int: offset=" + offset + ", length=" + length, e);
        }
        if (end > buf.length) {
            throw new IllegalArgumentException("Invalid region: offset=" + offset + ", length=" + length + ", data.length=" + buf.length);
        }
        return buf;
    }

    /** Backing array; not a copy. Valid only for this stream's lifetime. */
    public byte[] array() {
        return buf;
    }

    /** Current read cursor (absolute index into {@link #array()}). */
    public int position() {
        return pos;
    }

    /** Exclusive end of the readable region in {@link #array()}. */
    public int limit() {
        return count;
    }
}
