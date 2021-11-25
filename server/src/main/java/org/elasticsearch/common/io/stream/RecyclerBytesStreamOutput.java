/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A @link {@link StreamOutput} that uses {@link Recycler.V<BytesRef>} to acquire pages of bytes, which
 * avoids frequent reallocation &amp; copying of the internal data. When {@link #close()} is called,
 * the bytes will be released.
 */
public class RecyclerBytesStreamOutput extends BytesStream implements Releasable {

    static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final Recycler<BytesRef> recycler;
    private final int pageSize;
    private ArrayList<Page> pages = new ArrayList<>();
    private int pageIndex = -1;
    private int firstPageOffset = 0;
    private int currentPageOffset;

    public RecyclerBytesStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        try (Recycler.V<BytesRef> obtain = recycler.obtain()) {
            pageSize = obtain.v().length;
        }
        this.currentPageOffset = pageSize;
    }

    @Override
    public long position() {
        return internalPosition() - firstPageOffset;
    }

    private long internalPosition() {
        return ((long) pageSize * pageIndex) + currentPageOffset;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(1);
        BytesRef currentPage = pages.get(pageIndex).bytes();
        currentPage.bytes[currentPage.offset + currentPageOffset] = b;
        currentPageOffset++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        // illegal args: offset and/or length exceed array size
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }

        // get enough pages for new size
        ensureCapacity(length);

        // bulk copy
        int bytesToCopy = length;
        int srcOff = offset;
        int j = 0;
        while (true) {
            BytesRef currentPage = pages.get(pageIndex + j).bytes();
            int toCopyThisLoop = Math.min(pageSize - currentPageOffset, bytesToCopy);
            System.arraycopy(b, srcOff, currentPage.bytes, currentPage.offset + currentPageOffset, toCopyThisLoop);
            srcOff += toCopyThisLoop;
            bytesToCopy -= toCopyThisLoop;
            if (bytesToCopy > 0) {
                currentPageOffset = 0;
            } else {
                currentPageOffset += toCopyThisLoop;
                break;
            }
            j++;
        }

        // advance
        pageIndex += j;
    }

    @Override
    public void writeInt(int i) throws IOException {
        if (4 > (pageSize - currentPageOffset)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).bytes();
            VH_BE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).bytes();
            VH_BE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 8;
        }
    }

    @Override
    public void reset() {
        closePages();
        pageIndex = -1;
        currentPageOffset = pageSize;
        firstPageOffset = 0;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        long internalPosition = position + firstPageOffset;
        ensureCapacityFromPosition(internalPosition);
        this.pageIndex = (int) internalPosition / pageSize;
        this.currentPageOffset = (int) internalPosition % pageSize;
    }

    public void skip(int length) {
        seek(position() + length);
    }

    @Override
    public void close() {
        closePages();
    }

    private void closePages() {
        try {
            Releasables.close(pages.stream().map(c -> (Releasable) c::decRef).collect(Collectors.toList()));
        } finally {
            pages.clear();
        }
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see ByteArrayOutputStream#size()
     */
    public int size() {
        return Math.toIntExact(position());
    }

    @Override
    public BytesReference bytes() {
        return bytes(false);
    }

    private ReleasableBytesReference bytes(boolean retainAndTruncate) {
        int position = (int) internalPosition();
        final ReleasableBytesReference result;
        final int newFirstPageOffset;
        final int pagesToDrop;
        final boolean retainFirstPage;
        if (position == 0) {
            ReleasableBytesReference empty = ReleasableBytesReference.empty();
            if (retainAndTruncate == false) {
                empty.decRef();
            }
            newFirstPageOffset = 0;
            retainFirstPage = false;
            pagesToDrop = 0;
            result = empty;
        } else {
            final int adjustment;
            final int remainder = position % pageSize;
            final int internalBytesInLastPage;
            if (remainder != 0) {
                adjustment = 1;
                internalBytesInLastPage = remainder;
                newFirstPageOffset = remainder;
            } else {
                adjustment = 0;
                newFirstPageOffset = 0;
                internalBytesInLastPage = pageSize;
            }
            final int pageCount = (position / pageSize) + adjustment;
            retainFirstPage = adjustment == 1;
            pagesToDrop = pageCount - adjustment;
            if (pageCount == 1) {
                Page page = pages.get(0);
                BytesRef bytePage = page.bytes();
                BytesArray bytesArray = new BytesArray(
                    bytePage.bytes,
                    bytePage.offset + firstPageOffset,
                    internalBytesInLastPage - firstPageOffset
                );
                result = new ReleasableBytesReference(bytesArray, page);
            } else {
                ReleasableBytesReference[] references = new ReleasableBytesReference[pageCount];
                for (int i = 0; i < pageCount - 1; ++i) {
                    Page page = this.pages.get(i);
                    if (retainAndTruncate) {
                        page.incRef();
                    }
                    int offsetAdjustment = 0;
                    if (i == 0) {
                        offsetAdjustment = firstPageOffset;
                    }
                    BytesRef bytePage = page.bytes();
                    BytesArray bytesArray = new BytesArray(
                        bytePage.bytes,
                        bytePage.offset + offsetAdjustment,
                        bytePage.length - offsetAdjustment
                    );
                    references[i] = new ReleasableBytesReference(bytesArray, page);
                }
                Page page = this.pages.get(pageCount - 1);
                BytesRef last = page.bytes();
                references[pageCount - 1] = new ReleasableBytesReference(
                    new BytesArray(last.bytes, last.offset, internalBytesInLastPage),
                    page
                );
                result = new ReleasableBytesReference(CompositeBytesReference.of(references), () -> Releasables.close(references));
            }
        }
        if (retainAndTruncate) {
            ArrayList<Page> newPages = new ArrayList<>();
            for (int i = pagesToDrop; i < pages.size(); ++i) {
                newPages.add(pages.get(i));
            }
            pages = newPages;
            if (retainFirstPage) {
                pages.get(0).incRef();
            }
            firstPageOffset = newFirstPageOffset;
            if (pages.isEmpty()) {
                currentPageOffset = pageSize;
                pageIndex = -1;
            } else {
                currentPageOffset = firstPageOffset;
                pageIndex = 0;
            }
        }
        return result;
    }

    public ReleasableBytesReference retainBytesAndTruncateStream() {
        ReleasableBytesReference bytes = bytes(true);

        return bytes;
    }

    public List<ReleasableBytesReference> retainPages() {
        return new ArrayList<>(pages.size());
    }

    private void ensureCapacity(int bytesNeeded) {
        if (bytesNeeded > pageSize - currentPageOffset) {
            ensureCapacityFromPosition(internalPosition() + bytesNeeded);
        }
    }

    private void ensureCapacityFromPosition(long newInternalPosition) {
        while (newInternalPosition > (long) pageSize * pages.size()) {
            if (newInternalPosition > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
            }
            Recycler.V<BytesRef> newPage = recycler.obtain();
            assert pageSize == newPage.v().length;
            pages.add(new Page(newPage));
            // We are at the end of the current page, increment page index
            if (currentPageOffset == pageSize) {
                pageIndex++;
                currentPageOffset = 0;
            }
        }
    }

    private static class Page extends AbstractRefCounted {

        private final Recycler.V<BytesRef> v;

        private Page(Recycler.V<BytesRef> v) {
            this.v = v;
        }

        private BytesRef bytes() {
            return v.v();
        }

        @Override
        protected void closeInternal() {
            v.close();
        }
    }
}
