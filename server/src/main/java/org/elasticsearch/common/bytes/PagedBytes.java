/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * A sequence of bytes spread across multiple {@code byte[]} called "pages".
 * All pages except the last have exactly {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
 * The last page has {@code <=} {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
 * {@link #length} tracks how many of those bytes are valid.
 * <p>
 *     This is quite like {@link PagedBytesReference} but with stronger constraints
 *     that should make it faster. It's also accessed directly using
 *     {@link PagedBytesCursor} or {@link #pages()}, avoiding lots of
 *     {@code invokevirtual}.
 * </p>
 */
public final class PagedBytes implements Comparable<PagedBytes>, Releasable {
    /**
     * The actual data. All pages except the last have exactly
     * {@link PageCacheRecycler#BYTE_PAGE_SIZE}. The last page has
     * {@code <=} {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
     * {@link #length} tracks how many of those bytes are valid.
     */
    private final byte[][] pages;
    /**
     * Number of valid bytes across all {@link #pages}.
     */
    private final int length;
    /**
     * Called on {@link #close()}.
     */
    private final Releasable onClose;

    public static final PagedBytes EMPTY = new PagedBytes(new byte[0][], 0, () -> {});

    PagedBytes(byte[][] pages, int length, Releasable onClose) {
        if (Assertions.ENABLED) {
            for (int i = 0; i < pages.length - 1; i++) {
                if (pages[i].length != BYTE_PAGE_SIZE) {
                    throw new IllegalStateException("page " + i + " has length " + pages[i].length + " but expected " + BYTE_PAGE_SIZE);
                }
            }
            if (pages.length > 0 && pages[pages.length - 1].length > BYTE_PAGE_SIZE) {
                throw new IllegalStateException(
                    "last page has length " + pages[pages.length - 1].length + " but expected at most " + BYTE_PAGE_SIZE
                );
            }
        }
        this.pages = pages;
        this.length = length;
        this.onClose = onClose;
    }

    /**
     * The actual data. All pages except the last have exactly
     * {@link PageCacheRecycler#BYTE_PAGE_SIZE}. The last page has
     * {@code <=} {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
     * {@link #length} tracks how many of those bytes are valid.
     * Generally this will be in one of two shapes:
     * <ul>
     *     <li>A single page containing a short value (e.g. {@code length=3}): {@snippet lang=java :
     *     new byte[][] { page }  // page.length <= BYTE_PAGE_SIZE, only first 3 bytes are valid
     *     }</li>
     *     <li>Many full pages (e.g. {@code length = 5 * BYTE_PAGE_SIZE}): {@snippet lang=java :
     *     new byte[][] { page1, page2, page3, page4, page5 }
     *     }</li>
     * </ul>
     */
    public byte[][] pages() {
        return pages;
    }

    /**
     * Number of valid bytes across all {@link #pages}.
     */
    public int length() {
        return length;
    }

    /**
     * Returns {@code true} if this paged byte sequence is equal to {@code other}.
     * Compatible with {@link BytesRef#bytesEquals(BytesRef)}.
     */
    public boolean bytesEquals(BytesRef other) {
        if (length != other.length) {
            return false;
        }
        int otherOffset = other.offset;
        int remaining = length;
        for (byte[] page : pages) {
            int len = Math.min(page.length, remaining);
            for (int i = 0; i < len; i++) {
                if (page[i] != other.bytes[otherOffset++]) {
                    return false;
                }
            }
            remaining -= len;
            if (remaining == 0) {
                break;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != PagedBytes.class) {
            return false;
        }
        PagedBytes rhs = (PagedBytes) obj;
        if (this.length != rhs.length) {
            return false;
        }
        return compareTo(rhs) == 0;
    }

    @Override
    public int hashCode() {
        MurmurHash3x86_32 hasher = new MurmurHash3x86_32();
        for (int i = 0; i < pages.length - 1; i++) {
            hasher.fullPage(pages[i]);
        }
        return pages.length == 0
            ? hasher.lastPage(new byte[0], 0)
            : hasher.lastPage(pages[pages.length - 1], length - (pages.length - 1) * BYTE_PAGE_SIZE);
    }

    @Override
    public int compareTo(PagedBytes rhs) {
        int remaining = Math.min(this.length, rhs.length);
        int fullPages = remaining / BYTE_PAGE_SIZE;
        int tail = remaining % BYTE_PAGE_SIZE;

        for (int page = 0; page < fullPages; page++) {
            int diff = Arrays.compareUnsigned(this.pages[page], rhs.pages[page]);
            if (diff != 0) {
                return diff;
            }
        }

        if (tail > 0) {
            int diff = Arrays.compareUnsigned(this.pages[fullPages], 0, tail, rhs.pages[fullPages], 0, tail);
            if (diff != 0) {
                return diff;
            }
        }

        // All shared pages are the same.
        return Integer.compare(this.length, rhs.length);
    }

    public int compareTo(BytesRef rhs) {
        int remaining = Math.min(this.length, rhs.length);
        int rhsOffset = rhs.offset;
        for (int i = 0; remaining > 0; i++) {
            int pageLen = Math.min(remaining, BYTE_PAGE_SIZE);
            int diff = Arrays.compareUnsigned(this.pages[i], 0, pageLen, rhs.bytes, rhsOffset, rhsOffset + pageLen);
            if (diff != 0) {
                return diff;
            }
            remaining -= pageLen;
            rhsOffset += pageLen;
        }
        return Integer.compare(this.length, rhs.length);
    }

    @Override
    public String toString() {
        return limitedToString(pages, 0, 0, length);
    }

    /**
     * Renders up to 100 bytes starting at {@code pageIndex}/{@code pageOffset} within {@code pages}
     * as bracketed space-separated uppercase hex, e.g. {@code [0A FF 00]}, optionally followed by a
     * count of remaining unrendered bytes, e.g. {@code [0A FF ...2.1kb more...]}.
     */
    static String limitedToString(byte[][] pages, int pageIndex, int pageOffset, int remaining) {
        int show = Math.min(remaining, 100);
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int pi = pageIndex;
        int po = pageOffset;
        for (int i = 0; i < show; i++) {
            int b = pages[pi][po] & 0xFF;
            if (i > 0) {
                sb.append(' ');
            }
            sb.append(Character.toUpperCase(Character.forDigit(b >> 4, 16)));
            sb.append(Character.toUpperCase(Character.forDigit(b & 0xF, 16)));
            po++;
            if (po >= pages[pi].length && i < show - 1) {
                pi++;
                po = 0;
            }
        }
        if (remaining > 100) {
            sb.append(" ...").append(ByteSizeValue.ofBytes(remaining - 100)).append(" more...");
        }
        return sb.append(']').toString();
    }

    /**
     * Reset {@code scratch} to point at the start of this ref and return it.
     */
    public PagedBytesCursor cursor(PagedBytesCursor scratch) {
        scratch.init(pages(), 0, 0, length(), true);
        return scratch;
    }

    @Override
    public void close() {
        onClose.close();
    }
}
