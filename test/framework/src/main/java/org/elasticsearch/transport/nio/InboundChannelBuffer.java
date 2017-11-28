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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.lease.Releasable;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.function.Supplier;

public class InboundChannelBuffer {

    public static final int PAGE_SIZE = 1 << 14;

    private final int pageMask;
    private final int pageShift;

    private final ArrayDeque<Page> pages;
    private final Supplier<Page> pageSupplier;

    private int capacity = 0;
    private int internalIndex = 0;
    private int offset = 0;

    public InboundChannelBuffer() {
        this(() -> new Page(ByteBuffer.wrap(new byte[PAGE_SIZE]), () -> {}));
    }

    public InboundChannelBuffer(Supplier<Page> pageSupplier) {
        this.pageSupplier = pageSupplier;
        this.pages = new ArrayDeque<>();
        this.pageMask = PAGE_SIZE - 1;
        this.pageShift = Integer.numberOfTrailingZeros(PAGE_SIZE);
        this.capacity = PAGE_SIZE * pages.size();
        expandCapacity(PAGE_SIZE);
    }

    public void expandCapacity(int newCapacity) {
        if (capacity < newCapacity) {
            int numPages = numPages(newCapacity + offset);
            int pagesToAdd = numPages - pages.size();
            for (int i = 0; i < pagesToAdd; ++i) {
                pages.addLast(pageSupplier.get());
            }
            capacity += pagesToAdd * PAGE_SIZE;
        }
    }

    public void releasePagesFromHead(int bytesToRelease) {
        if (bytesToRelease > capacity) {
            throw new IllegalArgumentException("Releasing more bytes than allocated.");
        }

        int pagesToRelease = pageIndex(offset + bytesToRelease);
        for (int i = 0; i < pagesToRelease; ++i) {
            Page page = pages.removeFirst();
            page.close();
        }
        capacity -= bytesToRelease;
        internalIndex = Math.max(internalIndex - bytesToRelease, 0);
        offset = indexInPage(bytesToRelease + offset);
    }

    public ByteBuffer[] getPreIndexBuffers() {
        if (internalIndex == 0) {
            return new ByteBuffer[0];
        }
        int indexWithOffset = internalIndex + offset;
        int pageCount = pageIndex(indexWithOffset);
        int finalLimit = indexInPage(indexWithOffset);
        if (finalLimit != 0) {
            pageCount += 1;
        }

        ByteBuffer[] buffers = new ByteBuffer[pageCount];
        Iterator<Page> pageIterator = pages.iterator();
        ByteBuffer firstBuffer = pageIterator.next().buffer.duplicate();
        firstBuffer.position(firstBuffer.position() + offset);
        buffers[0] = firstBuffer;
        for (int i = 1; i < buffers.length; ++i) {
            buffers[i] = pageIterator.next().buffer.duplicate();
        }
        if (finalLimit != 0) {
            buffers[buffers.length - 1].limit(finalLimit);
        }

        return buffers;
    }

    public ByteBuffer[] getPostIndexBuffers() {
        if (internalIndex == capacity) {
            return new ByteBuffer[0];
        }
        int indexWithOffset = offset + internalIndex;

        int pageIndex = pageIndex(indexWithOffset);
        int indexInPage = indexInPage(indexWithOffset);

        ByteBuffer[] buffers = new ByteBuffer[pages.size() - pageIndex];
        Iterator<Page> pageIterator = pages.descendingIterator();
        for (int i = buffers.length - 1; i > 0; --i) {
            buffers[i] = pageIterator.next().buffer.duplicate();
        }
        ByteBuffer firstPostIndexBuffer = pageIterator.next().buffer.duplicate();
        firstPostIndexBuffer.position(firstPostIndexBuffer.position() + indexInPage);
        buffers[0] = firstPostIndexBuffer;

        return buffers;
    }

    public void incrementIndex(int delta) {
        internalIndex += delta;
    }

    public int getIndex() {
        return internalIndex;
    }

    public int getCapacity() {
        return capacity;
    }

    private int numPages(int capacity) {
        return (capacity + pageMask) >>> pageShift;
    }

    private int pageIndex(int index) {
        return index >>> pageShift;
    }

    private int indexInPage(int index) {
        return index & pageMask;
    }

    public static class Page implements Releasable {

        private final ByteBuffer buffer;
        private final Releasable releasable;


        public Page(ByteBuffer buffer, Releasable releasable) {
            this.buffer = buffer;
            this.releasable = releasable;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
