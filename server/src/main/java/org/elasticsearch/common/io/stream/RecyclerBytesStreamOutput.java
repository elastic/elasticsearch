/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Objects;

/**
 * A @link {@link StreamOutput} that uses {@link Recycler.V<BytesRef>} to acquire pages of bytes, which
 * avoids frequent reallocation &amp; copying of the internal data. When {@link #close()} is called,
 * the bytes will be released.
 */
public class RecyclerBytesStreamOutput extends BytesStream implements Releasable {

    static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    static final VarHandle VH_LE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    static final VarHandle VH_BE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
    static final VarHandle VH_LE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>();
    private final Recycler<BytesRef> recycler;
    private final int pageSize;
    private int pageIndex = -1;
    private int currentCapacity = 0;
    private int currentPageOffset;

    public RecyclerBytesStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.pageSize = recycler.pageSize();
        this.currentPageOffset = pageSize;
    }

    @Override
    public long position() {
        return ((long) pageSize * pageIndex) + currentPageOffset;
    }

    @Override
    public void writeByte(byte b) {
        int currentPageOffset = this.currentPageOffset;
        if (1 > (pageSize - currentPageOffset)) {
            ensureCapacity(1);
            currentPageOffset = 0;
        }
        BytesRef currentPage = pages.get(pageIndex).v();
        currentPage.bytes[currentPage.offset + currentPageOffset] = b;
        this.currentPageOffset = currentPageOffset + 1;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        Objects.checkFromIndexSize(offset, length, b.length);

        // get enough pages for new size
        final int pageSize = this.pageSize;
        int currentPageOffset = this.currentPageOffset;
        if (length > pageSize - currentPageOffset) {
            ensureCapacity(length);
            currentPageOffset = this.currentPageOffset;
        }

        // bulk copy
        int bytesToCopy = length;
        int srcOff = offset;
        int j = 0;
        while (true) {
            BytesRef currentPage = pages.get(pageIndex + j).v();
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
        this.currentPageOffset = currentPageOffset;

        // advance
        pageIndex += j;
    }

    @Override
    public void writeInt(int i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (4 > (pageSize - currentPageOffset)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            this.currentPageOffset = currentPageOffset + 4;
        }
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        if (4 > (pageSize - currentPageOffset)) {
            super.writeIntLE(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_LE_INT.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_BE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            this.currentPageOffset = currentPageOffset + 8;
        }
    }

    @Override
    public void writeLongLE(long i) throws IOException {
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLongLE(i);
        } else {
            BytesRef currentPage = pages.get(pageIndex).v();
            VH_LE_LONG.set(currentPage.bytes, currentPage.offset + currentPageOffset, i);
            currentPageOffset += 8;
        }
    }

    @Override
    public void legacyWriteWithSizePrefix(Writeable writeable) throws IOException {
        // TODO: do this without copying the bytes from tmp by calling writeBytes and just use the pages in tmp directly through
        // manipulation of the offsets on the pages after writing to tmp. This will require adjustments to the places in this class
        // that make assumptions about the page size
        try (RecyclerBytesStreamOutput tmp = new RecyclerBytesStreamOutput(recycler)) {
            tmp.setTransportVersion(getTransportVersion());
            writeable.writeTo(tmp);
            int size = tmp.size();
            writeVInt(size);
            int tmpPage = 0;
            while (size > 0) {
                final Recycler.V<BytesRef> p = tmp.pages.get(tmpPage);
                final BytesRef b = p.v();
                final int writeSize = Math.min(size, b.length);
                writeBytes(b.bytes, b.offset, writeSize);
                tmp.pages.set(tmpPage, null).close();
                size -= writeSize;
                tmpPage++;
            }
        }
    }

    // overridden with some code duplication the same way other write methods in this class are overridden to bypass StreamOutput's
    // intermediary buffers
    @Override
    public void writeString(String str) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        final int charCount = str.length();
        // maximum serialized length is 3 bytes per char + 5 bytes for the longest possible vint
        if (charCount * 3 + 5 > (pageSize - currentPageOffset)) {
            super.writeString(str);
            return;
        }
        BytesRef currentPage = pages.get(pageIndex).v();
        int off = currentPage.offset + currentPageOffset;
        byte[] buffer = currentPage.bytes;
        // mostly duplicated from StreamOutput.writeString to to get more reliable compilation of this very hot loop
        int offset = off + putVInt(buffer, charCount, off);
        for (int i = 0; i < charCount; i++) {
            final int c = str.charAt(i);
            if (c <= 0x007F) {
                buffer[offset++] = ((byte) c);
            } else if (c > 0x07FF) {
                buffer[offset++] = ((byte) (0xE0 | c >> 12 & 0x0F));
                buffer[offset++] = ((byte) (0x80 | c >> 6 & 0x3F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                buffer[offset++] = ((byte) (0xC0 | c >> 6 & 0x1F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            }
        }
        this.currentPageOffset = offset - currentPage.offset;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacityFromPosition(position);
        int offsetInPage = (int) (position % pageSize);
        int pageIndex = (int) position / pageSize;
        // Special handling for seeking to the first index in a new page, which is handled as a seeking to one-after the last index
        // in the previous case. This is done so that seeking to the first index of a new page does not cause a page allocation while
        // still allowing a fast check via (pageSize - currentPageOffset) on the remaining size in the current page in all other methods.
        if (offsetInPage == 0) {
            this.pageIndex = pageIndex - 1;
            this.currentPageOffset = pageSize;
        } else {
            this.pageIndex = pageIndex;
            this.currentPageOffset = offsetInPage;
        }
    }

    public void skip(int length) {
        seek(position() + length);
    }

    @Override
    public void close() {
        var pages = this.pages;
        if (pages != null) {
            this.pages = null;
            Releasables.close(pages);
        }
    }

    /**
     * Move the contents written to this stream to a {@link ReleasableBytesReference}. Closing this instance becomes a noop after
     * this method returns successfully and its buffers need to be released by releasing the returned bytes reference.
     *
     * @return a {@link ReleasableBytesReference} that must be released once no longer needed
     */
    public ReleasableBytesReference moveToBytesReference() {
        var bytes = bytes();
        var pages = this.pages;
        this.pages = null;
        return new ReleasableBytesReference(bytes, () -> Releasables.close(pages));
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
        int position = (int) position();
        if (position == 0) {
            return BytesArray.EMPTY;
        } else {
            final int adjustment;
            final int bytesInLastPage;
            final int remainder = position % pageSize;
            if (remainder != 0) {
                adjustment = 1;
                bytesInLastPage = remainder;
            } else {
                adjustment = 0;
                bytesInLastPage = pageSize;
            }
            final int pageCount = (position / pageSize) + adjustment;
            if (pageCount == 1) {
                BytesRef page = pages.get(0).v();
                return new BytesArray(page.bytes, page.offset, bytesInLastPage);
            } else {
                BytesReference[] references = new BytesReference[pageCount];
                for (int i = 0; i < pageCount - 1; ++i) {
                    references[i] = new BytesArray(this.pages.get(i).v());
                }
                BytesRef last = this.pages.get(pageCount - 1).v();
                references[pageCount - 1] = new BytesArray(last.bytes, last.offset, bytesInLastPage);
                return CompositeBytesReference.of(references);
            }
        }
    }

    private void ensureCapacity(int bytesNeeded) {
        assert bytesNeeded > pageSize - currentPageOffset;
        ensureCapacityFromPosition(position() + bytesNeeded);
    }

    private void ensureCapacityFromPosition(long newPosition) {
        // Integer.MAX_VALUE is not a multiple of the page size so we can only allocate the largest multiple of the pagesize that is less
        // than Integer.MAX_VALUE
        if (newPosition > Integer.MAX_VALUE - (Integer.MAX_VALUE % pageSize)) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
        }
        while (newPosition > currentCapacity) {
            Recycler.V<BytesRef> newPage = recycler.obtain();
            assert pageSize == newPage.v().length;
            pages.add(newPage);
            currentCapacity += pageSize;
        }
        // We are at the end of the current page, increment page index
        if (currentPageOffset == pageSize) {
            pageIndex++;
            currentPageOffset = 0;
        }
    }
}
