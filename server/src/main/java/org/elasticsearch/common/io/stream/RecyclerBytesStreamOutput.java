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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Objects;

/**
 * A @link {@link StreamOutput} that uses a {@link Recycler<BytesRef>} to acquire pages of bytes, which avoids frequent reallocation &amp;
 * copying of the internal data. When {@link #close()} is called, the bytes will be released.
 * <p>
 * Best only used for outputs which are either short-lived or large, because the resulting {@link ReleasableBytesReference} retains whole
 * pages and this overhead may be significant on small and long-lived objects.
 *
 * A {@link RecyclerBytesStreamOutput} obtains pages (16kiB slices of a larger {@code byte[]}) from a {@code Recycler<BytesRef>} rather than
 * using the {@link BigArrays} abstraction that {@link BytesStreamOutput} and {@link ReleasableBytesStreamOutput} both use. This means it
 * can access the underlying {@code byte[]} directly and therefore avoids the intermediate buffer and the copy for almost all writes (the
 * exception being occasional writes that get too close to the end of a page).
 * <p>
 * It does not attempt to grow its collector slowly in the same way that {@link BigArrays#resize} does. Instead, it always obtains from the
 * recycler a whole new 16kiB page when the need arises. This works best when the serialized data has a short lifespan (e.g. it is an
 * outbound network message) so the overhead has limited impact and the savings on allocations and copying (due to the absence of resize
 * operations) are significant.
 * <p>
 * The resulting {@link ReleasableBytesReference} is a view over the underlying {@code byte[]} pages and involves no significant extra
 * allocation to obtain. It is oversized: The worst case for overhead is when the data is a single byte, since this takes up a whole 16kiB
 * page almost all of which is overhead. Nonetheless, if recycled pages are available then it may still be preferable to use them via a
 * {@link RecyclerBytesStreamOutput}. If the result is large then the overhead is less important, and if the result will only be used for
 * a short time, for instance soon being written to the network or to disk, then the imminent recycling of these pages may mean it is ok to
 * keep it as-is. For results which are both small and long-lived it may be better to copy them into a freshly-allocated {@code byte[]}.
 * <p>
 * Any memory allocated in this way is not tracked by the {@link org.elasticsearch.common.breaker} subsystem, even if the
 * {@code Recycler<BytesRef>} was obtained from {@link BigArrays#bytesRefRecycler()}, unless the caller takes steps to add this tracking
 * themselves.
 */
public class RecyclerBytesStreamOutput extends BytesStream implements Releasable {

    private ArrayList<Recycler.V<BytesRef>> pages = new ArrayList<>(8);
    private final Recycler<BytesRef> recycler;
    private final int pageSize;
    private int pageIndex = -1;
    private int currentCapacity = 0;

    /**
     * Pool from which current buffer is sliced.
     */
    private byte[] currentBufferPool;

    /**
     * Current absolute offset within currentBufferPool.
     */
    private int currentOffset;

    /**
     * Max permitted offset within currentBufferPool.
     */
    private int maxOffset;

    /**
     * Position in stream corresponding (conceptually at least) with start of currentBufferPool.
     */
    private long positionOffset;

    public RecyclerBytesStreamOutput(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.pageSize = recycler.pageSize();
        this.currentOffset = this.maxOffset = pageSize;
        // Always start with a page. This is because if we don't have a page, one of the hot write paths would be forced to go through
        // a slow path. We prefer to only execute that path if we need to expand.
        ensureCapacityFromPosition(1);
        nextPage();
    }

    @Override
    public long position() {
        return positionOffset + currentOffset;
    }

    @Override
    public void writeByte(byte b) {
        int currentOffset = this.currentOffset;
        if (currentOffset >= maxOffset) {
            ensureCapacity(1);
            currentOffset = nextPage();
        }
        this.currentBufferPool[currentOffset] = b;
        this.currentOffset = currentOffset + 1;
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

        int currentOffset = this.currentOffset;
        int maxOffset = this.maxOffset;
        if (length <= maxOffset - currentOffset) {
            System.arraycopy(b, offset, this.currentBufferPool, currentOffset, length);
            this.currentOffset = currentOffset + length;
        } else {
            writeBytesMultiPage(b, offset, length, this.currentBufferPool, currentOffset, maxOffset);
        }
    }

    private void writeBytesMultiPage(
        byte[] sourceBufferPool,
        int sourceOffset,
        int lengthToCopy,
        byte[] targetBufferPool,
        int targetOffset,
        int maxTargetOffset
    ) {
        Objects.checkFromIndexSize(sourceOffset, lengthToCopy, sourceBufferPool.length);
        ensureCapacity(lengthToCopy);

        int pageIndex = this.pageIndex;
        int pageStart = 0;
        while (true) {
            final int toCopyThisLoop = Math.min(maxTargetOffset - targetOffset, lengthToCopy);
            System.arraycopy(sourceBufferPool, sourceOffset, targetBufferPool, targetOffset, toCopyThisLoop);
            sourceOffset += toCopyThisLoop;
            lengthToCopy -= toCopyThisLoop;
            if (lengthToCopy > 0) {
                final var nextPage = pages.get(++pageIndex).v();
                targetBufferPool = nextPage.bytes;
                targetOffset = pageStart = nextPage.offset;
                maxTargetOffset = nextPage.offset + nextPage.length;
            } else {
                targetOffset += toCopyThisLoop;
                break;
            }
        }
        this.pageIndex = pageIndex;
        this.currentBufferPool = targetBufferPool;
        this.currentOffset = targetOffset;
        this.maxOffset = maxTargetOffset;
        this.positionOffset = ((long) pageIndex) * pageSize - pageStart;
    }

    @Override
    public void writeVInt(int i) {
        if ((i & 0xFFFF_FF80) != 0) {
            // The cold-path multi-byte case is extracted to its own method so the hotter-path single-byte case can inline.
            writeMultiByteVInt(i);
            return;
        }

        final var maxOffset = this.maxOffset;
        var currentOffset = this.currentOffset;
        if (currentOffset == maxOffset) {
            ensureCapacityFromPosition(positionOffset + currentOffset + 1);
            currentOffset = nextPage();
        }

        this.currentBufferPool[currentOffset] = (byte) i;
        this.currentOffset = currentOffset + 1;
    }

    private void writeMultiByteVInt(int i) {
        final int currentOffset = this.currentOffset;
        final int remainingBytesInPage = maxOffset - currentOffset;
        if (5 > remainingBytesInPage && vIntLength(i) > remainingBytesInPage) {
            while ((i & 0xFFFF_FF80) != 0) {
                writeByte((byte) ((i & 0x7F) | 0x80));
                i >>>= 7;
            }
            writeByte((byte) (i & 0x7F));
        } else {
            this.currentOffset = StreamOutputHelper.putMultiByteVInt(this.currentBufferPool, i, currentOffset);
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
            StreamOutputHelper.putMultiByteVInt(page, i, offset);
        }
    }

    @Override
    public void writeInt(int i) throws IOException {
        int currentOffset = this.currentOffset;
        if (4 > (maxOffset - currentOffset)) {
            super.writeInt(i);
        } else {
            ByteUtils.writeIntBE(i, currentBufferPool, currentOffset);
            this.currentOffset = currentOffset + 4;
        }
    }

    @Override
    public void writeIntLE(int i) throws IOException {
        int currentOffset = this.currentOffset;
        if (4 > (maxOffset - currentOffset)) {
            super.writeIntLE(i);
        } else {
            ByteUtils.writeIntLE(i, currentBufferPool, currentOffset);
            this.currentOffset = currentOffset + 4;
        }
    }

    @Override
    public void writeLong(long i) throws IOException {
        int currentOffset = this.currentOffset;
        if (8 > (maxOffset - currentOffset)) {
            super.writeLong(i);
        } else {
            ByteUtils.writeLongBE(i, currentBufferPool, currentOffset);
            this.currentOffset = currentOffset + 8;
        }
    }

    @Override
    public void writeLongLE(long i) throws IOException {
        int currentOffset = this.currentOffset;
        if (8 > (maxOffset - currentOffset)) {
            super.writeLongLE(i);
        } else {
            ByteUtils.writeLongLE(i, currentBufferPool, currentOffset);
            this.currentOffset = currentOffset + 8;
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
        final int currentOffset = this.currentOffset;
        if (bytes <= maxOffset - currentOffset) {
            this.currentOffset = currentOffset + bytes;
            return new BytesRef(this.currentBufferPool, currentOffset, bytes);
        } else {
            return null;
        }
    }

    // overridden with some code duplication the same way other write methods in this class are overridden to bypass StreamOutput's
    // intermediary buffers
    @Override
    public void writeString(String str) throws IOException {
        int currentOffset = this.currentOffset;
        final int charCount = str.length();
        int bytesNeededForVInt = vIntLength(charCount);
        // maximum serialized length is 3 bytes per char + n bytes for the vint
        if (charCount * 3 + bytesNeededForVInt > maxOffset - currentOffset) {
            // Technically no need for scratch buffer here, we can do the same thing directly on the pages just with bounds checks -- TODO
            StreamOutputHelper.writeString(str, this);
            return;
        }

        int offset = currentOffset;
        byte[] currentBufferPool = this.currentBufferPool;
        // mostly duplicated from StreamOutput.writeString to to get more reliable compilation of this very hot loop
        putVInt(charCount, bytesNeededForVInt, currentBufferPool, offset);
        offset += bytesNeededForVInt;

        for (int i = 0; i < charCount; i++) {
            final int c = str.charAt(i);
            if (c <= 0x007F) {
                currentBufferPool[offset++] = ((byte) c);
            } else if (c > 0x07FF) {
                currentBufferPool[offset++] = ((byte) (0xE0 | c >> 12 & 0x0F));
                currentBufferPool[offset++] = ((byte) (0x80 | c >> 6 & 0x3F));
                currentBufferPool[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                currentBufferPool[offset++] = ((byte) (0xC0 | c >> 6 & 0x1F));
                currentBufferPool[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            }
        }
        this.currentOffset = offset;
    }

    @Override
    public void writeOptionalString(@Nullable String str) throws IOException {
        if (str == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeString(str);
        }
    }

    @Override
    public void writeGenericString(String value) throws IOException {
        writeByte((byte) 0);
        writeString(value);
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacityFromPosition(position);
        if (position > 0) {
            // Special handling for seeking to the first index in a new page, which is handled as a seeking to one-after the last index
            // in the previous case. This is done so that seeking to the first index of a new page does not cause a page allocation while
            // still allowing a fast check via (pageSize - currentPageOffset) on the remaining size in the current page in all other
            // methods.
            long prevPosition = position - 1;
            int offsetInPage = (int) (prevPosition % pageSize);
            int pageIndex = (int) prevPosition / pageSize;
            innerSeek(pageIndex, offsetInPage + 1, position);
        } else {
            // We always have an initial page so special handling for seeking to 0.
            assert position == 0;
            innerSeek(0, 0, 0);
        }
    }

    private void innerSeek(int pageIndex, int offsetInPage, long position) {
        if (this.pageIndex == pageIndex) {
            this.currentOffset = (int) (position - this.positionOffset);
        } else {
            this.pageIndex = pageIndex;
            final var page = pages.get(pageIndex).v();
            this.currentBufferPool = page.bytes;
            final var pageOffset = page.offset;
            this.currentOffset = pageOffset + offsetInPage;
            this.maxOffset = pageOffset + page.length;
            this.positionOffset = ((long) pageIndex) * pageSize - pageOffset;
        }
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

        return new ReleasableBytesReference(bytes, pages.size() == 1 ? pages.getFirst() : Releasables.wrap(pages));
    }

    /**
     * Base64-encode the contents of the stream and convert to a {@link String}, avoiding unnecessary allocation and copying as much as
     * possible.
     *
     * @param encoder Encoder to use. Must not insert line-breaks.
     * @return Base64-encoded copy of the contents of the stream.
     */
    public String toBase64String(Base64.Encoder encoder) {
        assert encoder.encode(new byte[120]).length == 160 : "Line breaks not supported";
        if (pageIndex == 0) {
            // common case: small object that fits into one page, can be encoded directly
            final var rawLength = pageSize - (maxOffset - currentOffset);

            // allocates a new array for the output
            final var encodedBuffer = encoder.encode(ByteBuffer.wrap(currentBufferPool, currentOffset - rawLength, rawLength));
            assert encodedBuffer.hasArray();

            // copies the buffer to a fresh array to ensure immutability
            return new String(encodedBuffer.array(), encodedBuffer.arrayOffset(), encodedBuffer.remaining(), StandardCharsets.ISO_8859_1);
        } else {
            return toBase64StringMultiPage(encoder);
        }
    }

    private String toBase64StringMultiPage(Base64.Encoder encoder) {
        // probably a mistake to want such a massive string, but let's do our best

        class ToAsciiStringStream extends OutputStream {
            // possibly slightly oversized but NB no space for line-breaks
            final byte[] encodedBytes = new byte[Math.multiplyExact(4, Math.addExact(Math.toIntExact(position()), 2) / 3)];
            int position;

            @Override
            public void write(int b) {
                encodedBytes[position++] = (byte) b;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                System.arraycopy(b, off, encodedBytes, position, len);
                position += len;
            }

            @Override
            public String toString() {
                return new String(encodedBytes, 0, position, StandardCharsets.ISO_8859_1);
            }
        }

        try (var toAsciiStringStream = new ToAsciiStringStream()) {
            try (var encoderStream = encoder.wrap(toAsciiStringStream)) {
                int copyPageIndex = 0;
                for (final var page : pages) {
                    final var pageContents = page.v();
                    if (copyPageIndex++ < pageIndex) {
                        encoderStream.write(pageContents.bytes, pageContents.offset, pageContents.length);
                    } else {
                        encoderStream.write(pageContents.bytes, pageContents.offset, currentOffset - pageContents.offset);
                        break;
                    }
                }
            }
            return toAsciiStringStream.toString();
        } catch (IOException e) {
            assert false : e; // no actual IO happens here
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Copies the entire remaining contents of the given {@link InputStream} directly into this output, avoiding the need for any
     * intermediate buffers.
     */
    public void writeAllBytesFrom(InputStream in) throws IOException {
        while (true) {
            final var currentBufferPool = this.currentBufferPool;
            final var maxOffset = this.maxOffset;
            var currentOffset = this.currentOffset;
            while (currentOffset < maxOffset) {
                int readSize = in.read(currentBufferPool, currentOffset, maxOffset - currentOffset);
                if (readSize == -1) {
                    this.currentOffset = currentOffset;
                    return;
                }
                currentOffset += readSize;
            }
            this.currentOffset = maxOffset;
            ensureCapacity(1);
            nextPage();
        }
    }

    private void closeFields() {
        this.pages = null;
        this.currentBufferPool = null;
        this.pageIndex = -1;
        this.currentOffset = 0;
        this.maxOffset = 0;
        this.positionOffset = 0L;
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
        final int position = (int) position();
        if (position == 0) {
            return BytesArray.EMPTY;
        } else if (position <= pageSize) {
            final var page = pages.getFirst().v();
            return new BytesArray(page.bytes, page.offset, position);
        } else {
            return bytesMultiPage(position);
        }
    }

    private BytesReference bytesMultiPage(int position) {
        final int pageCount = (position + pageSize - 1) / pageSize;
        assert pageCount > 1;
        final BytesReference[] references = new BytesReference[pageCount];
        int pageIndex = 0;
        for (var page : pages) {
            if (pageIndex < pageCount - 1) {
                references[pageIndex++] = new BytesArray(page.v());
            } else {
                final var pageBytes = page.v();
                references[pageIndex] = new BytesArray(pageBytes.bytes, pageBytes.offset, position - pageIndex * pageSize);
                break;
            }
        }
        return CompositeBytesReference.of(references);
    }

    private void ensureCapacity(int bytesNeeded) {
        assert bytesNeeded > maxOffset - currentOffset;
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

    private int nextPage() {
        pageIndex++;
        final var page = pages.get(pageIndex).v();
        this.currentBufferPool = page.bytes;
        final var pageOffset = page.offset;
        this.currentOffset = pageOffset;
        this.maxOffset = pageOffset + page.length;
        this.positionOffset = ((long) pageIndex) * pageSize - pageOffset;
        return pageOffset;
    }
}
