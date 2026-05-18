/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.WeakReference;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class BytesRefArray extends AbstractRefCounted implements Accountable, Releasable, Writeable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefArray.class);
    private static final int HALF_PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES / 2;
    static long MAX_INT_OFFSET = Integer.MAX_VALUE - 1024; // package level for testing

    private final BigArrays bigArrays;
    private final Bytes bytes;
    private final long maxIntOffset;
    private final long initialCapacity;
    private long size;

    private int fixedLength = -1;
    private long lastOffset;
    private IntOffsets intOffsets;
    private LongOffsets longOffsets;

    public BytesRefArray(long initialCapacity, BigArrays bigArrays) {
        this(initialCapacity, bigArrays, 0L);
    }

    public BytesRefArray(long initialCapacity, BigArrays bigArrays, long byteHint) {
        this.bigArrays = bigArrays;
        this.maxIntOffset = MAX_INT_OFFSET;
        this.initialCapacity = initialCapacity;
        boolean success = false;
        try {
            final long initialBytes = byteHint > 0 ? byteHint : initialCapacity * 3;
            bytes = new Bytes(bigArrays, initialBytes);
            success = true;
        } finally {
            if (success == false) {
                closeInternal();
            }
        }
    }

    public BytesRefArray(StreamInput in, BigArrays bigArrays) throws IOException {
        this.bigArrays = bigArrays;
        this.maxIntOffset = MAX_INT_OFFSET;
        boolean success = false;
        try {
            size = in.readVLong();
            this.initialCapacity = size;
            long numOffsets = size + 1;
            intOffsets = new IntOffsets(bigArrays, (int) Math.min(numOffsets, Integer.MAX_VALUE));
            long offset = 0;
            for (long i = 0; i < numOffsets; i++) {
                long next = in.readVLong();
                appendOffset(next, Math.toIntExact(next - offset));
                offset = next;
            }
            lastOffset = getOffset(size);
            long sizeOfBytes = in.readVLong();
            bytes = new Bytes(bigArrays, sizeOfBytes);
            bytes.readFrom(in, sizeOfBytes);
            success = true;
        } finally {
            if (success == false) {
                closeInternal();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: new serialization
        out.writeVLong(size);
        if (intOffsets == null) {
            final long len = Math.max(fixedLength, 0);
            for (long i = 0; i <= size; i++) {
                out.writeVLong(len * i);
            }
            out.writeVLong(lastOffset);
            bytes.writeTo(out);
            return;
        }
        for (long i = 0; i < intOffsets.size; i++) {
            out.writeVLong(intOffsets.get(i));
        }
        if (longOffsets != null) {
            for (long i = 0; i < longOffsets.size; i++) {
                out.writeVLong(longOffsets.get(i));
            }
        }
        out.writeVLong(lastOffset);
        bytes.writeTo(out);
    }

    private long getOffset(long index) {
        if (fixedLength >= 0) {
            return (long) fixedLength * index;
        }
        if (index < intOffsets.size) {
            return intOffsets.get(index);
        }
        return longOffsets.get(index - intOffsets.size);
    }

    private void appendOffset(long newOffset, int length) {
        if (intOffsets != null) {
            if (newOffset <= maxIntOffset) {
                intOffsets.append((int) newOffset);
                return;
            }
            if (longOffsets == null) {
                longOffsets = new LongOffsets(bigArrays, 16);
            }
            longOffsets.append(newOffset);
            return;
        }
        if (fixedLength == -1 || fixedLength == length) {
            fixedLength = length;
            return;
        }
        transitionFixedLengthToOffsets(newOffset);
    }

    private void transitionFixedLengthToOffsets(long newOffset) {
        intOffsets = new IntOffsets(bigArrays, (int) Math.min(initialCapacity + 1, Integer.MAX_VALUE));
        for (long i = 0; i <= size; i++) {
            long off = (long) fixedLength * i;
            if (off <= maxIntOffset) {
                intOffsets.append((int) off);
            } else {
                if (longOffsets == null) {
                    longOffsets = new LongOffsets(bigArrays, 16);
                }
                longOffsets.append(off);
            }
        }
        fixedLength = -1;
        if (newOffset <= maxIntOffset) {
            intOffsets.append((int) newOffset);
        } else {
            if (longOffsets == null) {
                longOffsets = new LongOffsets(bigArrays, 16);
            }
            longOffsets.append(newOffset);
        }
    }

    public void append(BytesRef value) {
        lastOffset += value.length;
        appendOffset(lastOffset, value.length);
        bytes.append(value.bytes, value.offset, value.length);
        ++size;
    }

    public void append(PagedBytesCursor cursor) {
        final int length = cursor.remaining();
        lastOffset += length;
        appendOffset(lastOffset, length);
        bytes.append(cursor);
        ++size;
    }

    public BytesRef get(long id, BytesRef dest) {
        final long startOffset = getOffset(id);
        final int length = Math.toIntExact(getOffset(id + 1) - startOffset);
        return bytes.get(startOffset, length, dest);
    }

    public PagedBytesCursor get(long id, PagedBytesCursor scratch) {
        final long startOffset = getOffset(id);
        final int length = Math.toIntExact(getOffset(id + 1) - startOffset);
        return bytes.get(startOffset, length, scratch);
    }

    /**
     * {@return the maximum byte length of any entry in this array, or {@code 0} if empty}
     */
    public int valueMaxByteSize() {
        if (fixedLength >= 0) {
            return fixedLength;
        }
        if (size == 0) {
            return 0;
        }
        int max = 0;
        long prev = getOffset(0);
        for (long i = 1; i <= size; i++) {
            long curr = getOffset(i);
            max = Math.max(max, Math.toIntExact(curr - prev));
            prev = curr;
        }
        return max;
    }

    public boolean bytesEqual(long id, BytesRef other) {
        final long startOffset = getOffset(id);
        final long length = getOffset(id + 1) - startOffset;
        if (length != other.length) {
            return false;
        }
        if (length == 0) {
            return true;
        }
        return bytes.bytesEqual(startOffset, other);
    }

    public long size() {
        return size;
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        Releasables.close(bytes, intOffsets, longOffsets);
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + bigArraysRamBytesUsed();
    }

    public long bigArraysRamBytesUsed() {
        long used = bytes.ramBytesUsed();
        if (intOffsets != null) {
            used += intOffsets.ramBytesUsed();
        }
        if (longOffsets != null) {
            used += longOffsets.ramBytesUsed();
        }
        return used;
    }

    static final class IntOffsets implements BigArray {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntOffsets.class);
        private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
        private static final int PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        private static final int INTS_PER_PAGE = PAGE_SIZE / Integer.BYTES;
        private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(INTS_PER_PAGE);
        private static final int PAGE_MASK = INTS_PER_PAGE - 1;

        private final BigArrays bigArrays;
        private final PageCacheRecycler recycler;
        private Recycler.V<?>[] caches;
        byte[][] pages;
        int pageCount;
        byte[] currentPage;
        int posInPage;
        long size;

        IntOffsets(BigArrays bigArrays, long initialCapacity) {
            this.bigArrays = bigArrays;
            PageCacheRecycler r = bigArrays.recycler();
            this.recycler = r != null ? r : PageCacheRecycler.NON_RECYCLING_INSTANCE;
            final int numPages = Math.toIntExact(Math.max(1, (initialCapacity + INTS_PER_PAGE - 1) / INTS_PER_PAGE));
            bigArrays.adjustBreaker(SHALLOW_SIZE + estimateUseByPagesArray(numPages), false);
            this.pages = new byte[numPages][];
            this.caches = new Recycler.V[numPages];
            boolean success = false;
            try {
                if (initialCapacity < INTS_PER_PAGE) {
                    final int initialBytes = Math.max(1, (int) initialCapacity) * Integer.BYTES;
                    bigArrays.adjustBreaker(initialBytes, false);
                    pages[0] = currentPage = new byte[initialBytes];
                    pageCount = 1;
                } else {
                    this.currentPage = grabNextPage();
                }
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private void growFirstPage() {
            byte[] oldPage = pages[0];
            final int newSize = ArrayUtil.oversize((int) size + 1, Integer.BYTES) * Integer.BYTES;
            if (newSize >= PageCacheRecycler.INT_PAGE_SIZE / 2) {
                bigArrays.adjustBreaker(PAGE_SIZE, false);
                Recycler.V<byte[]> newPage = recycler.bytePage(false);
                caches[0] = newPage;
                currentPage = pages[0] = newPage.v();
            } else {
                bigArrays.adjustBreaker(newSize, false);
                currentPage = pages[0] = new byte[newSize];
            }
            System.arraycopy(oldPage, 0, currentPage, 0, posInPage);
            bigArrays.adjustBreaker(-oldPage.length, true);
        }

        private byte[] grabNextPage() {
            if (pageCount >= pages.length) {
                final int oldSize = pages.length;
                int newSize = ArrayUtil.oversize(pageCount + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                bigArrays.adjustBreaker(estimateUseByPagesArray(newSize), false);
                pages = Arrays.copyOf(pages, newSize);
                caches = Arrays.copyOf(caches, newSize);
                bigArrays.adjustBreaker(-estimateUseByPagesArray(oldSize), true);
            }
            bigArrays.adjustBreaker(PAGE_SIZE, false);
            Recycler.V<byte[]> page = recycler.bytePage(false);
            caches[pageCount] = page;
            byte[] v = page.v();
            pages[pageCount++] = v;
            return v;
        }

        void append(int value) {
            if (posInPage + Integer.BYTES > currentPage.length) {
                if (currentPage.length < PAGE_SIZE) {
                    growFirstPage();
                } else {
                    currentPage = grabNextPage();
                    posInPage = 0;
                }
            }
            INT_HANDLE.set(currentPage, posInPage, value);
            posInPage += Integer.BYTES;
            size++;
        }

        int get(long index) {
            int page = (int) (index >> PAGE_SHIFT);
            int off = (int) (index & PAGE_MASK) * Integer.BYTES;
            return (int) INT_HANDLE.get(pages[page], off);
        }

        @Override
        public long ramBytesUsed() {
            long used = SHALLOW_SIZE + estimateUseByPagesArray(pages.length);
            if (pageCount > 0) {
                used += pages[0].length;
                used += (long) (pageCount - 1) * PAGE_SIZE;
            }
            return used;
        }

        @Override
        public void close() {
            for (int i = 0; i < pageCount; i++) {
                Releasables.close(caches[i]);
            }
            bigArrays.adjustBreaker(-ramBytesUsed(), true);
        }

        @Override
        public long size() {
            return size;
        }
    }

    static final class LongOffsets implements BigArray {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(LongOffsets.class);
        private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
        private static final int PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        private static final int LONGS_PER_PAGE = PAGE_SIZE / Long.BYTES;
        private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(LONGS_PER_PAGE);
        private static final int PAGE_MASK = LONGS_PER_PAGE - 1;

        private final BigArrays bigArrays;
        private final PageCacheRecycler recycler;
        private Recycler.V<?>[] caches;
        byte[][] pages;
        int pageCount;
        byte[] currentPage;
        int posInPage;
        long size;

        LongOffsets(BigArrays bigArrays, long initialCapacity) {
            this.bigArrays = bigArrays;
            PageCacheRecycler r = bigArrays.recycler();
            this.recycler = r != null ? r : PageCacheRecycler.NON_RECYCLING_INSTANCE;
            int numPages = Math.toIntExact(Math.max(1, (initialCapacity + LONGS_PER_PAGE - 1) / LONGS_PER_PAGE));
            bigArrays.adjustBreaker(SHALLOW_SIZE + estimateUseByPagesArray(numPages), true);
            this.pages = new byte[numPages][];
            this.caches = new Recycler.V[numPages];
            boolean success = false;
            try {
                this.currentPage = grabNextPage();
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private byte[] grabNextPage() {
            if (pageCount >= pages.length) {
                final int oldSize = pages.length;
                final int newSize = ArrayUtil.oversize(pageCount + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                bigArrays.adjustBreaker(estimateUseByPagesArray(newSize), false);
                pages = Arrays.copyOf(pages, newSize);
                caches = Arrays.copyOf(caches, newSize);
                bigArrays.adjustBreaker(-estimateUseByPagesArray(oldSize), true);
            }
            bigArrays.adjustBreaker(PAGE_SIZE, false);
            Recycler.V<byte[]> page = recycler.bytePage(false);
            caches[pageCount] = page;
            byte[] v = page.v();
            pages[pageCount++] = v;
            return v;
        }

        void append(long value) {
            if (posInPage == PAGE_SIZE) {
                currentPage = grabNextPage();
                posInPage = 0;
            }
            LONG_HANDLE.set(currentPage, posInPage, value);
            posInPage += Long.BYTES;
            size++;
        }

        long get(long index) {
            int page = (int) (index >> PAGE_SHIFT);
            int off = (int) (index & PAGE_MASK) * Long.BYTES;
            return (long) LONG_HANDLE.get(pages[page], off);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + estimateUseByPagesArray(pages.length) + (long) pageCount * PAGE_SIZE;
        }

        @Override
        public void close() {
            for (int i = 0; i < pageCount; i++) {
                Releasables.close(caches[i]);
            }
            bigArrays.adjustBreaker(-ramBytesUsed(), true);
        }
    }

    public static final class Bytes implements BigArray {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Bytes.class);
        private static final int PAGE_SIZE = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_SIZE);
        private static final int PAGE_MASK = PAGE_SIZE - 1;

        private final BigArrays bigArrays;
        private final PageCacheRecycler recycler;

        private Recycler.V<?>[] caches;
        byte[][] pages;
        int pageCount;
        byte[] currentPage;
        int currentPagePos;
        private WeakReference<byte[]> cachedScratch = new WeakReference<>(null);
        private final BytesRef scratch = new BytesRef();

        public Bytes(BigArrays bigArrays, long initialSize) {
            this.bigArrays = bigArrays;
            PageCacheRecycler r = bigArrays.recycler();
            this.recycler = r != null ? r : PageCacheRecycler.NON_RECYCLING_INSTANCE;
            int numPages = Math.max(1, (int) ((initialSize + PAGE_SIZE - 1) >> PAGE_SHIFT));
            bigArrays.adjustBreaker(SHALLOW_SIZE + estimateUseByPagesArray(numPages), false);
            this.pages = new byte[numPages][];
            this.caches = new Recycler.V[numPages];
            boolean success = false;
            try {
                if (initialSize < HALF_PAGE_SIZE) {
                    final int bytesLength = Math.max(1, (int) initialSize);
                    bigArrays.adjustBreaker(bytesLength, false);
                    pages[0] = currentPage = new byte[bytesLength];
                    pageCount = 1;
                } else {
                    currentPage = grabNextPage();
                }
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        private void growFirstPage(long minSize) {
            byte[] oldPage = pages[0];
            final int newSize = ArrayUtil.oversize((int) Math.min(minSize, PAGE_SIZE), Integer.BYTES) * Integer.BYTES;
            if (newSize >= PageCacheRecycler.INT_PAGE_SIZE / 2) {
                bigArrays.adjustBreaker(PAGE_SIZE, false);
                Recycler.V<byte[]> newPage = recycler.bytePage(false);
                caches[0] = newPage;
                currentPage = pages[0] = newPage.v();
            } else {
                bigArrays.adjustBreaker(newSize, false);
                currentPage = pages[0] = new byte[newSize];
            }
            System.arraycopy(oldPage, 0, currentPage, 0, currentPagePos);
            bigArrays.adjustBreaker(-oldPage.length, true);
        }

        private void grow(int minSize) {
            final int oldSize = pages.length;
            int newSize = ArrayUtil.oversize(minSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            bigArrays.adjustBreaker(estimateUseByPagesArray(newSize), false);
            pages = Arrays.copyOf(pages, newSize);
            caches = Arrays.copyOf(caches, newSize);
            bigArrays.adjustBreaker(-estimateUseByPagesArray(oldSize), true);
        }

        private byte[] grabNextPage() {
            if (pageCount >= pages.length) {
                grow(pageCount + 1);
            }
            bigArrays.adjustBreaker(PAGE_SIZE, false);
            Recycler.V<byte[]> page = recycler.bytePage(false);
            caches[pageCount] = page;
            byte[] v = page.v();
            pages[pageCount++] = v;
            return v;
        }

        void readFrom(StreamInput in, long length) throws IOException {
            long bytesRead = 0;
            if (currentPage.length < PAGE_SIZE) {
                final long newLength = bytesRead + length;
                if (newLength <= currentPage.length) {
                    in.readBytes(currentPage, (int) bytesRead, (int) length);
                    bytesRead += length;
                    currentPagePos = (int) bytesRead;
                    return;
                }
                growFirstPage(newLength);
            }
            long remaining = length;
            while (remaining > 0) {
                if (currentPagePos == PAGE_SIZE) {
                    currentPage = grabNextPage();
                    currentPagePos = 0;
                }
                int toRead = (int) Math.min(remaining, PAGE_SIZE - currentPagePos);
                in.readBytes(currentPage, currentPagePos, toRead);
                currentPagePos += toRead;
                remaining -= toRead;
            }
        }

        void writeTo(StreamOutput out) throws IOException {
            long remaining = size();
            for (int p = 0; p < pageCount && remaining > 0; p++) {
                int chunk = (int) Math.min(remaining, pages[p].length);
                out.writeBytes(pages[p], 0, chunk);
                remaining -= chunk;
            }
        }

        public void append(byte[] src, int offset, int len) {
            if (currentPagePos + len <= currentPage.length) {
                System.arraycopy(src, offset, currentPage, currentPagePos, len);
                currentPagePos += len;
            } else if (currentPage.length < PAGE_SIZE) {
                promoteThenAdd(src, offset, len);
            } else {
                appendCrossPage(src, offset, len);
            }
        }

        private void promoteThenAdd(byte[] src, int offset, int len) {
            final int newSize = currentPagePos + len;
            growFirstPage(newSize);
            if (newSize <= PAGE_SIZE) {
                System.arraycopy(src, offset, currentPage, currentPagePos, len);
                currentPagePos += len;
                return;
            }
            appendCrossPage(src, offset, len);
        }

        private void appendCrossPage(byte[] src, int offset, int len) {
            int copyLen = PAGE_SIZE - currentPagePos;
            System.arraycopy(src, offset, currentPage, currentPagePos, copyLen);
            do {
                currentPage = grabNextPage();
                offset += copyLen;
                len -= copyLen;
                copyLen = Math.min(len, PAGE_SIZE);
                System.arraycopy(src, offset, currentPage, 0, copyLen);
            } while (len > copyLen);
            currentPagePos = copyLen;
        }

        void append(PagedBytesCursor cursor) {
            while (cursor.remaining() > 0) {
                cursor.readPageChunk(scratch);
                append(scratch.bytes, scratch.offset, scratch.length);
            }
        }

        private byte[] allocateScratchBuffer(byte[] scratch, int requiredLength) {
            if (requiredLength < HALF_PAGE_SIZE) {
                return new byte[requiredLength];
            }
            byte[] cached = cachedScratch.get();
            if (cached == scratch && scratch.length >= requiredLength) {
                return cached;
            }
            cached = new byte[requiredLength];
            cachedScratch = new WeakReference<>(cached);
            return cached;
        }

        BytesRef get(long byteStart, int length, BytesRef dst) {
            if (length == 0) {
                dst.length = 0;
                return dst;
            }
            final int pageIndex = (int) (byteStart >> PAGE_SHIFT);
            final int offset = (int) (byteStart & PAGE_MASK);
            if (offset + length <= PAGE_SIZE) {
                dst.bytes = pages[pageIndex];
                dst.offset = offset;
                dst.length = length;
            } else {
                dst.offset = 0;
                dst.length = length;
                dst.bytes = allocateScratchBuffer(dst.bytes, length);
                int copiedBytes = PAGE_SIZE - offset;
                System.arraycopy(pages[pageIndex], offset, dst.bytes, 0, copiedBytes);
                int p = pageIndex;
                do {
                    int chunkLen = Math.min(PAGE_SIZE, length - copiedBytes);
                    System.arraycopy(pages[++p], 0, dst.bytes, copiedBytes, chunkLen);
                    copiedBytes += chunkLen;
                } while (copiedBytes < length);
            }
            return dst;
        }

        boolean bytesEqual(long byteStart, BytesRef other) {
            int length = other.length;
            int pageIndex = (int) (byteStart >> PAGE_SHIFT);
            int offset = (int) (byteStart & PAGE_MASK);
            if (offset + length <= PAGE_SIZE) {
                return Arrays.mismatch(pages[pageIndex], offset, offset + length, other.bytes, other.offset, other.offset + length) < 0;
            }
            int firstChunk = PAGE_SIZE - offset;
            if (Arrays.mismatch(pages[pageIndex], offset, PAGE_SIZE, other.bytes, other.offset, other.offset + firstChunk) >= 0) {
                return false;
            }
            int compared = firstChunk;
            do {
                ++pageIndex;
                int cmpLen = Math.min(PAGE_SIZE, length - compared);
                if (Arrays.mismatch(
                    pages[pageIndex],
                    0,
                    cmpLen,
                    other.bytes,
                    other.offset + compared,
                    other.offset + compared + cmpLen
                ) >= 0) {
                    return false;
                }
                compared += cmpLen;
            } while (compared < length);
            return true;
        }

        PagedBytesCursor get(long byteStart, int length, PagedBytesCursor scratch) {
            scratch.init(pages, (int) (byteStart >> PAGE_SHIFT), (int) (byteStart & PAGE_MASK), length, true);
            return scratch;
        }

        @Override
        public long size() {
            if (pageCount > 0) {
                return (long) (pageCount - 1) * PAGE_SIZE + currentPagePos;
            }
            return 0;
        }

        @Override
        public long ramBytesUsed() {
            long used = SHALLOW_SIZE + estimateUseByPagesArray(pages.length);
            if (pageCount > 0) {
                used += pages[0].length;
                used += (long) (pageCount - 1) * PAGE_SIZE;
            }
            return used;
        }

        @Override
        public void close() {
            for (int i = 0; i < pageCount; i++) {
                Releasables.close(caches[i]);
            }
            bigArrays.adjustBreaker(-ramBytesUsed(), true);
        }
    }

    static long estimateUseByPagesArray(int arrayLength) {
        return 2L * RamUsageEstimator.alignObjectSize(
            (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * arrayLength
        );
    }
}
