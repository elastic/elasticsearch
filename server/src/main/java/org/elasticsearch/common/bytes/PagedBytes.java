/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;

/**
 * Represents a slice of pages spread across multiple {@code byte[]}. All pages
 * except the last have exactly {@link PageCacheRecycler#BYTE_PAGE_SIZE}.
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
     * The actual data. All entries have {@link PageCacheRecycler#BYTE_PAGE_SIZE} bytes;
     * only the first {@link #length} bytes across all pages are valid.
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
     * The actual data. All entries have exactly {@link PageCacheRecycler#BYTE_PAGE_SIZE} bytes;
     * only the first {@link #length} bytes across all pages are valid. Generally this will be
     * in one of two shapes:
     * <ul>
     *     <li>A single page containing a short value (e.g. {@code length=3}): {@snippet lang=java :
     *     new byte[][] { page }  // page.length == BYTE_PAGE_SIZE, only first 3 bytes are valid
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
        if (obj.getClass() != PagedBytes.class) {
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
        // Must match BytesRef.hashCode() for the same byte sequence so that paged and
        // flat keys are interchangeable in BytesRefHashTable.
        // BytesRef.hashCode() delegates to StringHelper.murmurhash3_x86_32.
        var hasher = new MurmurHash3x86_32(StringHelper.GOOD_FAST_HASH_SEED);
        for (int i = 0; i < pages.length - 1; i++) {
            hasher.fullPage(pages[i]);
        }
        return pages.length == 0
            ? hasher.lastPage(new byte[0], 0)
            : hasher.lastPage(pages[pages.length - 1], length - (pages.length - 1) * BYTE_PAGE_SIZE);
    }

    /**
     * Computes the same hash code used by {@link BytesRef} without materializing into a
     * contiguous array. It's {@code MurmurHash3_x86_32}! Feed full pages via {@link #fullPage},
     * then call {@link #lastPage} once to mix in the final (possibly partial) page and get the hash.
     * <p>
     * Because {@link PageCacheRecycler#BYTE_PAGE_SIZE} is a multiple of 4, the 4-byte blocks
     * that murmur3 processes never straddle page boundaries. Only the last page can have a tail
     * of 0–3 bytes.
     */
    static class MurmurHash3x86_32 {
        private static final int C1 = 0xcc9e2d51;
        private static final int C2 = 0x1b873593;

        private int h1;
        private int totalLength;

        MurmurHash3x86_32(int seed) {
            this.h1 = seed;
        }

        /** Mix in one full {@link PageCacheRecycler#BYTE_PAGE_SIZE}-byte page. */
        void fullPage(byte[] page) {
            // BYTE_PAGE_SIZE is a multiple of 4, so there is no tail.
            for (int i = 0; i < page.length; i += 4) {
                int k1 = (page[i] & 0xff) | ((page[i + 1] & 0xff) << 8) | ((page[i + 2] & 0xff) << 16) | ((page[i + 3] & 0xff) << 24);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
                h1 = Integer.rotateLeft(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }
            totalLength += page.length;
        }

        /** Mix in the last page (using only the first {@code length} bytes) and return the hash. */
        int lastPage(byte[] page, int length) {
            totalLength += length;
            int roundedEnd = length & ~3;

            for (int i = 0; i < roundedEnd; i += 4) {
                int k1 = (page[i] & 0xff) | ((page[i + 1] & 0xff) << 8) | ((page[i + 2] & 0xff) << 16) | ((page[i + 3] & 0xff) << 24);
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
                h1 = Integer.rotateLeft(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
            }

            int tailLen = length & 3;
            if (tailLen > 0) {
                int k1 = switch (tailLen) {
                    case 3 -> ((page[roundedEnd + 2] & 0xff) << 16) | ((page[roundedEnd + 1] & 0xff) << 8) | (page[roundedEnd] & 0xff);
                    case 2 -> ((page[roundedEnd + 1] & 0xff) << 8) | (page[roundedEnd] & 0xff);
                    default -> page[roundedEnd] & 0xff;
                };
                k1 *= C1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= C2;
                h1 ^= k1;
            }

            // Finalization — force all bits to avalanche.
            h1 ^= totalLength;
            h1 ^= h1 >>> 16;
            h1 *= 0x85ebca6b;
            h1 ^= h1 >>> 13;
            h1 *= 0xc2b2ae35;
            h1 ^= h1 >>> 16;

            return h1;
        }
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
        return this.length - rhs.length;
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
        return this.length - rhs.length;
    }

    @Override
    public String toString() {
        // Truncate to valid bytes; if there are multiple pages limitedToString will add "..." since
        // pages[0].length == BYTE_PAGE_SIZE > 100.
        return limitedToString(Arrays.copyOf(pages[0], Math.min(pages[0].length, length)));
    }

    /**
     * Renders up to 100 bytes from {@code head} as space-separated hex.
     */
    static String limitedToString(byte[] head) {
        StringBuilder b = new StringBuilder();
        int length = Math.min(100, head.length);
        for (int i = 0; i < length; i++) {
            if (i != 0) {
                b.append(' ');
            }
            int v = head[i] & 0xff;
            if (v < 0x10) {
                b.append('0');
            }
            b.append(Integer.toHexString(v));
        }
        if (length != head.length) {
            b.append("...");
        }
        return b.toString();
    }

    /**
     * Reset {@code scratch} to point at the start of this ref and return it.
     */
    public PagedBytesCursor cursor(PagedBytesCursor scratch) {
        scratch.init(this);
        return scratch;
    }

    @Override
    public void close() {
        onClose.close();
    }
}
