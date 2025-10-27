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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * A @link {@link StreamOutput} that uses {@link Recycler.V<BytesRef>} to acquire pages of bytes, which
 * avoids frequent reallocation &amp; copying of the internal data. When {@link #close()} is called,
 * the bytes will be released.
 */
public class RecyclerBytesStreamOutput extends BytesStream implements Releasable {

    private ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>(8);
    private final Recycler<BytesRef> recycler;
    private final int pageSize;
    private int pageIndex = -1;
    private int currentCapacity = 0;

    private BytesRef currentBytesRef;
    private int currentPageOffset;

    public RecyclerBytesStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.pageSize = recycler.pageSize();
        this.currentPageOffset = pageSize;
        // Always start with a page. This is because if we don't have a page, one of the hot write paths would be forced to go through
        // a slow path. We prefer to only execute that path if we need to expand.
        ensureCapacityFromPosition(1);
        nextPage();
    }

    @Override
    public long position() {
        return ((long) pageSize * pageIndex) + currentPageOffset;
    }

    @Override
    public void writeByte(byte b) {
        int currentPageOffset = this.currentPageOffset;
        if (1 > pageSize - currentPageOffset) {
            ensureCapacity(1);
            nextPage();
            currentPageOffset = 0;
        }
        final BytesRef currentPage = currentBytesRef;
        final int destOffset = currentPage.offset + currentPageOffset;
        currentPage.bytes[destOffset] = b;
        this.currentPageOffset = currentPageOffset + 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writeBytes(b, off, len);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        Objects.checkFromIndexSize(offset, length, b.length);

        int currentPageOffset = this.currentPageOffset;
        BytesRef currentPage = currentBytesRef;
        if (length > pageSize - currentPageOffset) {
            ensureCapacity(length);
        }

        int bytesToCopy = length;
        int srcOff = offset;
        while (true) {
            final int toCopyThisLoop = Math.min(pageSize - currentPageOffset, bytesToCopy);
            final int destOffset = currentPage.offset + currentPageOffset;
            System.arraycopy(b, srcOff, currentPage.bytes, destOffset, toCopyThisLoop);
            srcOff += toCopyThisLoop;
            bytesToCopy -= toCopyThisLoop;
            if (bytesToCopy > 0) {
                currentPageOffset = 0;
                currentPage = pages.get(++pageIndex).v();
            } else {
                currentPageOffset += toCopyThisLoop;
                break;
            }
        }
        this.currentPageOffset = currentPageOffset;
        this.currentBytesRef = currentPage;
    }

    @Override
    public void writeVInt(int i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        final int remainingBytesInPage = pageSize - currentPageOffset;

        // Single byte values (most common)
        if ((i & 0xFFFFFF80) == 0) {
            if (1 > remainingBytesInPage) {
                super.writeVInt(i);
            } else {
                BytesRef currentPage = currentBytesRef;
                currentPage.bytes[currentPage.offset + currentPageOffset] = (byte) i;
                this.currentPageOffset = currentPageOffset + 1;
            }
            return;
        }

        int bytesNeeded = vIntLength(i);
        if (bytesNeeded > remainingBytesInPage) {
            super.writeVInt(i);
        } else {
            BytesRef currentPage = currentBytesRef;
            putVInt(i, bytesNeeded, currentPage.bytes, currentPage.offset + currentPageOffset);
            this.currentPageOffset = currentPageOffset + bytesNeeded;
        }
    }

    public static int vIntLength(int value) {
        int leadingZeros = Integer.numberOfLeadingZeros(value);
        if (leadingZeros >= 25) {
            return 1;
        } else if (leadingZeros >= 18) {
            return 2;
        } else if (leadingZeros >= 11) {
            return 3;
        } else if (leadingZeros >= 4) {
            return 4;
        }
        return 5;
    }

    private void putVInt(int i, int bytesNeeded, byte[] page, int offset) {
        if (bytesNeeded == 1) {
            page[offset] = (byte) i;
        } else {
            putMultiByteVInt(page, i, offset);
        }
    }

    @Override
    public void writeInt(int i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (4 > (pageSize - currentPageOffset)) {
            super.writeInt(i);
        } else {
            BytesRef currentPage = currentBytesRef;
            ByteUtils.writeIntBE(i, currentPage.bytes, currentPage.offset + currentPageOffset);
            this.currentPageOffset = currentPageOffset + 4;
        }
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (4 > (pageSize - currentPageOffset)) {
            super.writeIntLE(i);
        } else {
            BytesRef currentPage = currentBytesRef;
            ByteUtils.writeIntLE(i, currentPage.bytes, currentPage.offset + currentPageOffset);
            this.currentPageOffset = currentPageOffset + 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLong(i);
        } else {
            BytesRef currentPage = currentBytesRef;
            ByteUtils.writeLongBE(i, currentPage.bytes, currentPage.offset + currentPageOffset);
            this.currentPageOffset = currentPageOffset + 8;
        }
    }

    @Override
    public void writeLongLE(long i) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        if (8 > (pageSize - currentPageOffset)) {
            super.writeLongLE(i);
        } else {
            BytesRef currentPage = currentBytesRef;
            ByteUtils.writeLongLE(i, currentPage.bytes, currentPage.offset + currentPageOffset);
            this.currentPageOffset = currentPageOffset + 8;
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

    /**
     * Attempt to get one page to perform a write directly into the page. The page will only be returned if the requested bytes can fit.
     * If requested bytes cannot fit, null will be returned. This will advance the current position in the stream.
     *
     * @param bytes the number of bytes for the single write
     * @return a direct page if there is enough space in current page, otherwise null
     */
    public BytesRef tryGetPageForWrite(int bytes) {
        final int beforePageOffset = this.currentPageOffset;
        if (bytes <= (pageSize - beforePageOffset)) {
            BytesRef currentPage = currentBytesRef;
            BytesRef bytesRef = new BytesRef(currentPage.bytes, currentPage.offset + beforePageOffset, bytes);
            this.currentPageOffset = beforePageOffset + bytes;
            return bytesRef;
        } else {
            return null;
        }
    }

    // overridden with some code duplication the same way other write methods in this class are overridden to bypass StreamOutput's
    // intermediary buffers
    @Override
    public void writeString(String str) throws IOException {
        final int currentPageOffset = this.currentPageOffset;
        final int charCount = str.length();
        int bytesNeededForVInt = vIntLength(charCount);
        // maximum serialized length is 3 bytes per char + n bytes for the vint
        if (charCount * 3 + bytesNeededForVInt > (pageSize - currentPageOffset)) {
            super.writeString(str);
            return;
        }

        BytesRef currentPage = currentBytesRef;
        int offset = currentPage.offset + currentPageOffset;
        byte[] buffer = currentPage.bytes;
        // mostly duplicated from StreamOutput.writeString to to get more reliable compilation of this very hot loop
        putVInt(charCount, bytesNeededForVInt, currentPage.bytes, offset);
        offset += bytesNeededForVInt;

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
        if (position > 0) {
            int offsetInPage = (int) (position % pageSize);
            int pageIndex = (int) position / pageSize;

            // Special handling for seeking to the first index in a new page, which is handled as a seeking to one-after the last index
            // in the previous case. This is done so that seeking to the first index of a new page does not cause a page allocation while
            // still allowing a fast check via (pageSize - currentPageOffset) on the remaining size in the current page in all other
            // methods.
            if (offsetInPage == 0) {
                this.pageIndex = pageIndex - 1;
                this.currentPageOffset = pageSize;
            } else {
                this.pageIndex = pageIndex;
                this.currentPageOffset = offsetInPage;
            }
        } else {
            // We always have an initial page so special handling for seeking to 0.
            assert position == 0;
            this.pageIndex = 0;
            this.currentPageOffset = 0;
        }
        this.currentBytesRef = pages.get(pageIndex).v();
    }

    public void skip(int length) {
        seek(position() + length);
    }

    @Override
    public void close() {
        var pages = this.pages;
        if (pages != null) {
            closeFields();
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
        closeFields();

        return new ReleasableBytesReference(bytes, () -> Releasables.close(pages));
    }

    private void closeFields() {
        this.pages = null;
        this.currentBytesRef = null;
        this.pageIndex = -1;
        this.currentPageOffset = pageSize;
        this.currentCapacity = 0;
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
        } else if (pages == null) {
            throw new IllegalStateException("Cannot use " + getClass().getSimpleName() + " after it has been closed");
        }

        long additionalCapacityNeeded = newPosition - currentCapacity;
        if (additionalCapacityNeeded > 0) {
            // Calculate number of additional pages needed
            int additionalPagesNeeded = (int) ((additionalCapacityNeeded + pageSize - 1) / pageSize);
            pages.ensureCapacity(pages.size() + additionalPagesNeeded);
            for (int i = 0; i < additionalPagesNeeded; i++) {
                Recycler.V<BytesRef> newPage = recycler.obtain();
                assert pageSize == newPage.v().length;
                pages.add(newPage);
            }
            currentCapacity += additionalPagesNeeded * pageSize;
        }
    }

    private void nextPage() {
        pageIndex++;
        currentPageOffset = 0;
        currentBytesRef = pages.get(pageIndex).v();
    }
}
