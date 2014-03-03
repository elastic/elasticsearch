/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.cache.recycler.NonePageCacheRecyclerService;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BigArrays;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A @link {@link StreamOutput} that uses a {@link PageCacheRecycler} to acquire pages of
 * bytes, which avoids frequent reallocation & copying of the internal data. Pages are
 * returned to the recycler on {@link #close()}.
 */
public class BytesStreamOutput extends StreamOutput implements BytesStream {

    /**
     * PageCacheRecycler for acquiring/releasing individual memory pages
     */
    private final NonePageCacheRecyclerService pageRecycler;

    /**
     * The buffer where data is stored.
     */
    private final List<Recycler.V<byte[]>> pages;

    /**
     * The number of valid bytes in the buffer.
     */
    private int count;

    /**
     * Size of a page taken from the PageCacheRecycler. We assume a constant page size for
     * all requests.
     */
    private final int pageSize;

    /**
     * Create a nonrecycling {@link BytesStreamOutput} with 1 initial page acquired.
     */
    public BytesStreamOutput() {
        this(NonePageCacheRecyclerService.INSTANCE, BigArrays.BYTE_PAGE_SIZE);
    }

    /**
     * Create a nonrecycling {@link BytesStreamOutput} with enough initial pages acquired
     * to satisfy the capacity given by {@link expectedSize}.
     * 
     * @param expectedSize the expected maximum size of the stream in bytes.
     */
    public BytesStreamOutput(int expectedSize) {
        this(NonePageCacheRecyclerService.INSTANCE, expectedSize);
    }

    /**
     * Create a {@link BytesStreamOutput} with 1 initial page acquired.
     * 
     * @param pageCacheRecycler the {@link PageCacheRecycler} from which to obtain
     *            bytes[]s.
     */
    private BytesStreamOutput(NonePageCacheRecyclerService pageCacheRecycler) {
        // expected size does not matter as long as it's >0
        this(pageCacheRecycler, 1);
    }

    /**
     * Create a {@link BytesStreamOutput} with enough initial pages acquired to satisfy
     * the capacity given by {@link expectedSize}.
     * 
     * @param pageCacheRecycler the {@link PageCacheRecycler} from which to obtain
     *            bytes[]s.
     * @param expectedSize the expected maximum size of the stream in bytes.
     */
    private BytesStreamOutput(NonePageCacheRecyclerService pageCacheRecycler, int expectedSize) {
        this.pageRecycler = pageCacheRecycler;
        // there is no good way to figure out the pageSize used by a PCR, so
        // get one page and use its size.
        Recycler.V<byte[]> vpage = pageRecycler.bytePage(true);
        this.pageSize = vpage.v().length;
        // expect 16 pages by default, more if specified
        this.pages = new ArrayList<Recycler.V<byte[]>>(Math.max(16, expectedSize / pageSize));
        // keep already acquired page
        this.pages.add(vpage);
        // acquire all other requested pages up front if expectedSize > pageSize
        if (expectedSize > pageSize) {
            ensureCapacity(expectedSize);
        }
    }

    @Override
    public boolean seekPositionSupported() {
        return true;
    }

    @Override
    public long position() throws IOException {
        return count;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        ensureOpen();
        ensureCapacity(count);
        byte[] page = pages.get(count / pageSize).v();
        int offset = count % pageSize;
        page[offset] = b;
        count++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        ensureOpen();

        // nothing to copy
        if (length == 0) {
            return;
        }

        // illegal args: offset and/or length exceed array size
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }

        // get enough pages for new size
        ensureCapacity(count + length);

        // where are we?
        int currentPageIndex = count / pageSize;
        byte[] currentPage = pages.get(currentPageIndex).v();
        int initialOffset = count % pageSize;
        int remainingPageCapacity = pageSize - initialOffset;
        int remainingLength = length;

        // try to fill the current page pointed to by #count in one copy if possible
        if (remainingLength <= remainingPageCapacity) {
            // simply copy all of length
            System.arraycopy(b, offset, currentPage, initialOffset, remainingLength);
            count += remainingLength;
        }
        else {
            // first fill remainder of first page
            System.arraycopy(b, offset, currentPage, initialOffset, remainingPageCapacity);
            count += remainingPageCapacity;
            remainingLength -= remainingPageCapacity;
            int copied = remainingPageCapacity;

            // if there is enough to copy try to fill adjacent pages directly
            while (remainingLength > pageSize) {
                // advance & fill next page
                currentPage = pages.get(++currentPageIndex).v();
                System.arraycopy(b, offset + copied, currentPage, 0, pageSize);
                copied += pageSize;
                remainingLength -= pageSize;
                count += pageSize;
            }

            // finally take care of remaining bytes: tricky since the above loop may not
            // have run, and #count could point to the middle of a page. So figure out
            // again where we are.
            currentPageIndex = count / pageSize;
            currentPage = pages.get(currentPageIndex).v();
            initialOffset = count % pageSize;
            System.arraycopy(b, offset + copied, currentPage, initialOffset, remainingLength);
            count += remainingLength;
        }
    }

    public void reset() {
        ensureOpen();
        // reset the count but keep all acquired pages
        count = 0;
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        // nothing to do there
    }

    @Override
    public void seek(long position) throws IOException {
        if (position > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("position " + position + " > Integer.MAX_VALUE");
        }
        count = (int)position;
        ensureCapacity(count);
    }

    public void skip(int length) {
        count += length;
        ensureCapacity(count);
    }

    @Override
    public void close() throws IOException {
        // empty for now.
    }

    /**
     * Returns the current size of the buffer.
     * 
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return count;
    }

    @Override
    public BytesReference bytes() {
        // for now just create a copy; later we might create a page-aware BytesReference
        // and just transfer ownership.
        return new BytesArray(toByteArray(), 0, count);
    }

    private byte[] toByteArray() {
        // stricly speaking this is undefined. :/
        // ensureOpen();

        if (count <= pageSize) {
            // simply return the first page
            return pages.get(0).v();
        }

        // create result array
        byte[] result = new byte[count];
        int resultOffset = 0;

        // copy all full pages
        int toCopy = Math.min(count, pageSize);
        int numPages = count / pageSize;
        for (int i = 0; i < numPages; i++) {
            byte[] page = pages.get(i).v();
            System.arraycopy(page, 0, result, resultOffset, toCopy);
            resultOffset += toCopy;
        }

        // copy any remaining bytes from the last page
        int remainder = count % pageSize;
        if (remainder > 0) {
            byte[] lastPage = pages.get(numPages).v();
            System.arraycopy(lastPage, 0, result, resultOffset, remainder);
        }

        return result;
    }

    private void ensureOpen() {
        if (count < 0) {
            throw new IllegalStateException("Stream is already closed.");
        }
    }

    private void ensureCapacity(int offset) {
        int capacity = pageSize * pages.size();
        while (offset >= capacity) {
            Recycler.V<byte[]> vpage = pageRecycler.bytePage(true);
            pages.add(vpage);
            capacity += pageSize;
        }
    }

}
