/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * A sequential-read cursor over a {@link PagedBytes}. Tracks the current page
 * index and offset within that page. Callers advance the cursor by calling the
 * {@code read*} methods.
 */
public class PagedBytesCursor {
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private byte[][] pages;
    private int pageIndex;
    private int pageOffset;
    private int remaining;

    /**
     * Scratch space for encoders that need to copy-and-mutate bytes. Access via {@link #init(BytesRef)}.
     */
    public final BytesRef scratchBytes = new BytesRef(); // NOCOMMIT remove me
    /**
     * Used by {@link #init(byte[], int, int, int)} to hold the single page so
     * we only have to allocate one time.
     */
    private byte[][] singlePageHolder;

    /**
     * Make an empty cursor, pointing at nothing.
     */
    public PagedBytesCursor() {}

    /**
     * Reset this cursor to point at the start of {@code ref}.
     */
    public void init(PagedBytes ref) {
        init(ref.pages(), 0, 0, ref.length());
    }

    /**
     * Reset this cursor to point at {@code remaining} bytes starting at
     * {@code pageIndex}/{@code pageOffset} within {@code pages}.
     */
    public void init(byte[][] pages, int pageIndex, int pageOffset, int remaining) {
        this.pages = pages;
        this.pageIndex = pageIndex;
        this.pageOffset = pageOffset;
        this.remaining = remaining;
    }

    /**
     * Reset this cursor to point at {@code remaining} bytes within a single {@code byte[]},
     * starting at {@code pageOffset}. Lazily allocates the single-page holder on first use.
     */
    public void init(byte[] bytes, int pageIndex, int pageOffset, int remaining) {
        if (singlePageHolder == null) {
            singlePageHolder = new byte[1][];
        }
        singlePageHolder[0] = bytes;
        this.pages = singlePageHolder;
        this.pageIndex = pageIndex;
        this.pageOffset = pageOffset;
        this.remaining = remaining;
    }

    /**
     * Reset this cursor to point at all bytes in {@code ref}. Used by encoders
     * that copy-and-mutate via {@link #scratchBytes} and then make the result
     * readable through the cursor.
     */
    public void init(BytesRef ref) { // NOCOMMIT all calls to this are leftovers
        init(ref.bytes, 0, ref.offset, ref.length);
    }

    /**
     * Wrap a flat {@link BytesRef} as a single-page cursor. No copy is made.
     */
    public static PagedBytesCursor fromBytesRef(BytesRef ref) {
        return new PagedBytesCursor(ref);
    }
    // NOCOMMIT all calls to this are leftovers? maybe. same for private ctor below.

    private PagedBytesCursor(BytesRef ref) {
        this.pages = new byte[][] { ref.bytes };
        this.pageIndex = 0;
        this.pageOffset = ref.offset;
        this.remaining = ref.length;
    }

    /**
     * Point {@code scratch} at the next {@code len} bytes and advance this cursor past them.
     * The returned cursor shares the same backing pages — no copy is made.
     */
    public PagedBytesCursor slice(int len, PagedBytesCursor scratch) {
        if (remaining < len) {
            throw new IllegalArgumentException("not enough bytes");
        }
        scratch.pages = pages;
        scratch.pageIndex = pageIndex;
        scratch.pageOffset = pageOffset;
        scratch.remaining = len;
        // All pages except the last are exactly BYTE_PAGE_SIZE, so advance arithmetically.
        int abs = pageOffset + len;
        pageIndex += abs / BYTE_PAGE_SIZE;
        pageOffset = abs % BYTE_PAGE_SIZE;
        remaining -= len;
        return scratch;
    }

    /**
     * Number of bytes not yet read.
     */
    public int remaining() {
        return remaining;
    }

    /**
     * Read one byte and advance.
     */
    public byte readByte() {
        if (remaining <= 0) {
            throw new IllegalArgumentException("no bytes remaining");
        }
        byte b = pages[pageIndex][pageOffset++];
        remaining--;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return b;
    }

    /**
     * Read an {@code int} and advance.
     */
    public int readInt() {
        if (remaining < Integer.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Integer.BYTES) {
            int v = (int) INT.get(pages[pageIndex], pageOffset);
            pageOffset += Integer.BYTES;
            remaining -= Integer.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16) | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Read a {@code long} and advance.
     */
    public long readLong() {
        if (remaining < Long.BYTES) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= Long.BYTES) {
            long v = (long) LONG.get(pages[pageIndex], pageOffset);
            pageOffset += Long.BYTES;
            remaining -= Long.BYTES;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return v;
        }
        return ((long) (readByte() & 0xFF) << 56) | ((long) (readByte() & 0xFF) << 48) | ((long) (readByte() & 0xFF) << 40)
            | ((long) (readByte() & 0xFF) << 32) | ((long) (readByte() & 0xFF) << 24) | ((long) (readByte() & 0xFF) << 16)
            | ((long) (readByte() & 0xFF) << 8) | (long) (readByte() & 0xFF);
    }

    /**
     * Read an int stored in variable-length format. Reads between one and five bytes.
     * Smaller values take fewer bytes. Negative numbers always use all 5 bytes.
     */
    public int readVInt() {
        byte b = readByte();
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = readByte();
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) != 0) {
            throw new IllegalStateException("Invalid last byte for a vint [" + Integer.toHexString(b) + "]");
        }
        return i;
    }

    /**
     * Read {@code len} bytes and advance. Returns a {@link BytesRef} pointing directly
     * into the current page when the bytes fit within it (zero-copy), or copies into
     * {@code scratch} when they span a page boundary.
     * <p>
     *     NOCOMMIT swap this to returning PagedBytesRef - and have that have an offset.
     *     NOCOMMIT rewrite javadocs after
     * </p>
     */
    public BytesRef readBytesRef(int len, BytesRef scratch) {
        if (remaining < len) {
            throw new IllegalArgumentException("not enough bytes");
        }
        if (pages[pageIndex].length - pageOffset >= len) {
            scratch.bytes = pages[pageIndex];
            scratch.offset = pageOffset;
            scratch.length = len;
            pageOffset += len;
            remaining -= len;
            if (pageOffset >= pages[pageIndex].length && remaining > 0) {
                pageIndex++;
                pageOffset = 0;
            }
            return scratch;
        }
        scratch.bytes = ArrayUtil.grow(scratch.bytes, len);
        scratch.offset = 0;
        scratch.length = len;
        for (int i = 0; i < len; i++) {
            scratch.bytes[i] = readByte();
        }
        return scratch;
    }

    /**
     * Read bytes up to the end of the current page without crossing a page boundary,
     * returning a zero-copy {@link BytesRef} into the backing page. Advances the cursor.
     */
    public BytesRef readPageChunk(BytesRef scratch) {
        int len = Math.min(pages[pageIndex].length - pageOffset, remaining);
        scratch.bytes = pages[pageIndex];
        scratch.offset = pageOffset;
        scratch.length = len;
        pageOffset += len;
        remaining -= len;
        if (pageOffset >= pages[pageIndex].length && remaining > 0) {
            pageIndex++;
            pageOffset = 0;
        }
        return scratch;
    }

    /**
     * Flip all bits of the remaining unread bytes in the underlying pages in-place.
     * The cursor position is not advanced.
     */
    public void bitwiseNot() {
        int rem = remaining;
        int pi = pageIndex;
        int po = pageOffset;
        while (rem > 0) {
            int len = Math.min(pages[pi].length - po, rem);
            for (int i = po; i < po + len; i++) {
                pages[pi][i] = (byte) ~pages[pi][i];
            }
            rem -= len;
            pi++;
            po = 0;
        }
    }

    /**
     * Read bytes up to (and consuming) {@code terminator}, copying them into {@code scratch}.
     * Always copies — callers are expected to mutate the result in place.
     * <p>
     * TODO: callers that do not mutate the result may be able to use
     * {@link #readBytesRef(int, BytesRef)} with a pre-scanned length to get the
     * zero-copy fast path.
     * </p>
     */
    public BytesRef readTerminatedBytesRef(byte terminator, BytesRef scratch) {
        int len = findTerminator(terminator);
        scratch.bytes = ArrayUtil.grow(scratch.bytes, len);
        scratch.offset = 0;
        scratch.length = len;
        for (int i = 0; i < len; i++) {
            scratch.bytes[i] = readByte();
        }
        readByte(); // consume the terminator
        return scratch;
    }

    private int findTerminator(byte terminator) {
        int scanPageIndex = pageIndex;
        int scanOffset = pageOffset;
        int scanRemaining = remaining;
        int len = 0;
        while (scanRemaining > 0) {
            if (pages[scanPageIndex][scanOffset] == terminator) {
                return len;
            }
            len++;
            scanOffset++;
            scanRemaining--;
            if (scanOffset >= pages[scanPageIndex].length && scanRemaining > 0) {
                scanPageIndex++;
                scanOffset = 0;
            }
        }
        throw new IllegalArgumentException("terminator not found");
    }
}
