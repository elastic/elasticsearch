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

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.netty.NettyUtils;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;

/**
 * A page based bytes reference, internally holding the bytes in a paged
 * data structure.
 */
public class PagedBytesReference implements BytesReference {

    private static final int PAGE_SIZE = BigArrays.BYTE_PAGE_SIZE;

    private final BigArrays bigarrays;
    protected final ByteArray bytearray;
    private final int offset;
    private final int length;
    private int hash = 0;

    public PagedBytesReference(BigArrays bigarrays, ByteArray bytearray, int length) {
        this(bigarrays, bytearray, 0, length);
    }

    public PagedBytesReference(BigArrays bigarrays, ByteArray bytearray, int from, int length) {
        this.bigarrays = bigarrays;
        this.bytearray = bytearray;
        this.offset = from;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return bytearray.get(offset + index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from < 0 || (from + length) > length()) {
            throw new IllegalArgumentException("can't slice a buffer with length [" + length() + "], with slice parameters from [" + from + "], length [" + length + "]");
        }

        return new PagedBytesReference(bigarrays, bytearray, offset + from, length);
    }

    @Override
    public StreamInput streamInput() {
        return new PagedBytesReferenceStreamInput(bytearray, offset, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        // nothing to do
        if (length == 0) {
            return;
        }

        BytesRef ref = new BytesRef();
        int written = 0;

        // are we a slice?
        if (offset != 0) {
            // remaining size of page fragment at offset
            int fragmentSize = Math.min(length, PAGE_SIZE - (offset % PAGE_SIZE));
            bytearray.get(offset, fragmentSize, ref);
            os.write(ref.bytes, ref.offset, fragmentSize);
            written += fragmentSize;
        }

        // handle remainder of pages + trailing fragment
        while (written < length) {
            int remaining = length - written;
            int bulkSize = (remaining > PAGE_SIZE) ? PAGE_SIZE : remaining;
            bytearray.get(offset + written, bulkSize, ref);
            os.write(ref.bytes, ref.offset, bulkSize);
            written += bulkSize;
        }
    }

    @Override
    public void writeTo(GatheringByteChannel channel) throws IOException {
        // nothing to do
        if (length == 0) {
            return;
        }

        int currentLength = length;
        int currentOffset = offset;
        BytesRef ref = new BytesRef();

        while (currentLength > 0) {
            // try to align to the underlying pages while writing, so no new arrays will be created.
            int fragmentSize = Math.min(currentLength, PAGE_SIZE - (currentOffset % PAGE_SIZE));
            boolean newArray = bytearray.get(currentOffset, fragmentSize, ref);
            assert !newArray : "PagedBytesReference failed to align with underlying bytearray. offset [" + currentOffset + "], size [" + fragmentSize + "]";
            Channels.writeToChannel(ref.bytes, ref.offset, ref.length, channel);
            currentLength -= ref.length;
            currentOffset += ref.length;
        }

        assert currentLength == 0;
    }

    @Override
    public byte[] toBytes() {
        if (length == 0) {
            return BytesRef.EMPTY_BYTES;
        }

        BytesRef ref = new BytesRef();
        bytearray.get(offset, length, ref);

        // undo the single-page optimization by ByteArray.get(), otherwise
        // a materialized stream will contain trailing garbage/zeros
        byte[] result = ref.bytes;
        if (result.length != length || ref.offset != 0) {
            result = Arrays.copyOfRange(result, ref.offset, ref.offset + length);
        }

        return result;
    }

    @Override
    public BytesArray toBytesArray() {
        BytesRef ref = new BytesRef();
        bytearray.get(offset, length, ref);
        return new BytesArray(ref);
    }

    @Override
    public BytesArray copyBytesArray() {
        BytesRef ref = new BytesRef();
        boolean copied = bytearray.get(offset, length, ref);

        if (copied) {
            // BigArray has materialized for us, no need to do it again
            return new BytesArray(ref.bytes, ref.offset, ref.length);
        } else {
            // here we need to copy the bytes even when shared
            byte[] copy = Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
            return new BytesArray(copy);
        }
    }

    @Override
    public ChannelBuffer toChannelBuffer() {
        // nothing to do
        if (length == 0) {
            return ChannelBuffers.EMPTY_BUFFER;
        }

        ChannelBuffer[] buffers;
        ChannelBuffer currentBuffer = null;
        BytesRef ref = new BytesRef();
        int pos = 0;

        // are we a slice?
        if (offset != 0) {
            // remaining size of page fragment at offset
            int fragmentSize = Math.min(length, PAGE_SIZE - (offset % PAGE_SIZE));
            bytearray.get(offset, fragmentSize, ref);
            currentBuffer = ChannelBuffers.wrappedBuffer(ref.bytes, ref.offset, fragmentSize);
            pos += fragmentSize;
        }

        // no need to create a composite buffer for a single page
        if (pos == length && currentBuffer != null) {
            return currentBuffer;
        }

        // a slice > pagesize will likely require extra buffers for initial/trailing fragments
        int numBuffers = countRequiredBuffers((currentBuffer != null ? 1 : 0), length - pos);

        buffers = new ChannelBuffer[numBuffers];
        int bufferSlot = 0;

        if (currentBuffer != null) {
            buffers[bufferSlot] = currentBuffer;
            bufferSlot++;
        }

        // handle remainder of pages + trailing fragment
        while (pos < length) {
            int remaining = length - pos;
            int bulkSize = (remaining > PAGE_SIZE) ? PAGE_SIZE : remaining;
            bytearray.get(offset + pos, bulkSize, ref);
            currentBuffer = ChannelBuffers.wrappedBuffer(ref.bytes, ref.offset, bulkSize);
            buffers[bufferSlot] = currentBuffer;
            bufferSlot++;
            pos += bulkSize;
        }

        // this would indicate that our numBuffer calculation is off by one.
        assert (numBuffers == bufferSlot);

        return ChannelBuffers.wrappedBuffer(NettyUtils.DEFAULT_GATHERING, buffers);
    }

    @Override
    public boolean hasArray() {
        return (offset + length <= PAGE_SIZE);
    }

    @Override
    public byte[] array() {
        if (hasArray()) {
            if (length == 0) {
                return BytesRef.EMPTY_BYTES;
            }

            BytesRef ref = new BytesRef();
            bytearray.get(offset, length, ref);
            return ref.bytes;
        }

        throw new IllegalStateException("array not available");
    }

    @Override
    public int arrayOffset() {
        if (hasArray()) {
            BytesRef ref = new BytesRef();
            bytearray.get(offset, length, ref);
            return ref.offset;
        }

        throw new IllegalStateException("array not available");
    }

    @Override
    public String toUtf8() {
        if (length() == 0) {
            return "";
        }

        byte[] bytes = toBytes();
        final CharsRefBuilder ref = new CharsRefBuilder();
        ref.copyUTF8Bytes(bytes, offset, length);
        return ref.toString();
    }

    @Override
    public BytesRef toBytesRef() {
        BytesRef bref = new BytesRef();
        // if length <= pagesize this will dereference the page, or materialize the byte[]
        bytearray.get(offset, length, bref);
        return bref;
    }

    @Override
    public BytesRef copyBytesRef() {
        byte[] bytes = toBytes();
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            // TODO: delegate to BigArrays via:
            // hash = bigarrays.hashCode(bytearray);
            // and for slices:
            // hash = bigarrays.hashCode(bytearray, offset, length);
            int tmphash = 1;
            for (int i = 0; i < length; i++) {
                tmphash = 31 * tmphash + bytearray.get(offset + i);
            }
            hash = tmphash;
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (!(obj instanceof PagedBytesReference)) {
            return BytesReference.Helper.bytesEqual(this, (BytesReference) obj);
        }

        PagedBytesReference other = (PagedBytesReference) obj;
        if (length != other.length) {
            return false;
        }

        // TODO: delegate to BigArrays via:
        // return bigarrays.equals(bytearray, other.bytearray);
        // and for slices:
        // return bigarrays.equals(bytearray, start, other.bytearray, otherstart, len);
        ByteArray otherArray = other.bytearray;
        int otherOffset = other.offset;
        for (int i = 0; i < length; i++) {
            if (bytearray.get(offset + i) != otherArray.get(otherOffset + i)) {
                return false;
            }
        }
        return true;
    }

    private int countRequiredBuffers(int initialCount, int numBytes) {
        int numBuffers = initialCount;
        // an "estimate" of how many pages remain - rounded down
        int pages = numBytes / PAGE_SIZE;
        // a remaining fragment < pagesize needs at least one buffer
        numBuffers += (pages == 0) ? 1 : pages;
        // a remainder that is not a multiple of pagesize also needs an extra buffer
        numBuffers += (pages > 0 && numBytes % PAGE_SIZE > 0) ? 1 : 0;
        return numBuffers;
    }

    private static class PagedBytesReferenceStreamInput extends StreamInput {

        private final ByteArray bytearray;
        private final BytesRef ref;
        private final int offset;
        private final int length;
        private int pos;
        private int mark;

        public PagedBytesReferenceStreamInput(ByteArray bytearray, int offset, int length) {
            this.bytearray = bytearray;
            this.ref = new BytesRef();
            this.offset = offset;
            this.length = length;
            this.pos = 0;

            if (offset + length > bytearray.size()) {
                throw new IndexOutOfBoundsException("offset+length >= bytearray.size()");
            }
        }

        @Override
        public byte readByte() throws IOException {
            if (pos >= length) {
                throw new EOFException();
            }

            return bytearray.get(offset + pos++);
        }

        @Override
        public void readBytes(byte[] b, int bOffset, int len) throws IOException {
            if (len > offset + length) {
                throw new IndexOutOfBoundsException("Cannot read " + len + " bytes from stream with length " + length + " at pos " + pos);
            }

            read(b, bOffset, len);
        }

        @Override
        public int read() throws IOException {
            return (pos < length) ? bytearray.get(offset + pos++) : -1;
        }

        @Override
        public int read(final byte[] b, final int bOffset, final int len) throws IOException {
            if (len == 0) {
                return 0;
            }

            if (pos >= offset + length) {
                return -1;
            }

            final int numBytesToCopy = Math.min(len, length - pos); // copy the full length or the remaining part

            // current offset into the underlying ByteArray
            long byteArrayOffset = offset + pos;

            // bytes already copied
            int copiedBytes = 0;

            while (copiedBytes < numBytesToCopy) {
                long pageFragment = PAGE_SIZE - (byteArrayOffset % PAGE_SIZE); // how much can we read until hitting N*PAGE_SIZE?
                int bulkSize = (int) Math.min(pageFragment, numBytesToCopy - copiedBytes); // we cannot copy more than a page fragment
                boolean copied = bytearray.get(byteArrayOffset, bulkSize, ref); // get the fragment
                assert (copied == false); // we should never ever get back a materialized byte[]
                System.arraycopy(ref.bytes, ref.offset, b, bOffset + copiedBytes, bulkSize); // copy fragment contents
                copiedBytes += bulkSize; // count how much we copied
                byteArrayOffset += bulkSize; // advance ByteArray index
            }

            pos += copiedBytes; // finally advance our stream position
            return copiedBytes;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public void mark(int readlimit) {
            this.mark = pos;
        }

        @Override
        public void reset() throws IOException {
            pos = mark;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public int available() throws IOException {
            return length - pos;
        }

    }
}
