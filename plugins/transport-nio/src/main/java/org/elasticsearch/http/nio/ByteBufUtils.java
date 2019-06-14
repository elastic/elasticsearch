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
package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class ByteBufUtils {

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        if (reference instanceof ByteBufBytesReference) {
            return ((ByteBufBytesReference) reference).toByteBuf();
        } else {
            final BytesRefIterator iterator = reference.iterator();
            // usually we have one, two, or three components from the header, the message, and a buffer
            final List<ByteBuf> buffers = new ArrayList<>(3);
            try {
                BytesRef slice;
                while ((slice = iterator.next()) != null) {
                    buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
                }

                if (buffers.size() == 1) {
                    return buffers.get(0);
                } else {
                    CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                    composite.addComponents(true, buffers);
                    return composite;
                }
            } catch (IOException ex) {
                throw new AssertionError("no IO happens here", ex);
            }
        }
    }

    static BytesReference toBytesReference(final ByteBuf buffer) {
        return new ByteBufBytesReference(buffer, buffer.readableBytes());
    }

    private static class ByteBufBytesReference extends BytesReference {

        private final ByteBuf buffer;
        private final int length;
        private final int offset;

        ByteBufBytesReference(ByteBuf buffer, int length) {
            this.buffer = buffer;
            this.length = length;
            this.offset = buffer.readerIndex();
            assert length <= buffer.readableBytes() : "length[" + length +"] > " + buffer.readableBytes();
        }

        @Override
        public byte get(int index) {
            return buffer.getByte(offset + index);
        }

        @Override
        public int getInt(int index) {
            return buffer.getInt(offset + index);
        }

        @Override
        public int indexOf(byte marker, int from) {
            final int start = offset + from;
            return buffer.forEachByte(start, length - start, value -> value != marker);
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public BytesReference slice(int from, int length) {
            return new ByteBufBytesReference(buffer.slice(offset + from, length), length);
        }

        @Override
        public StreamInput streamInput() {
            return new ByteBufStreamInput(buffer.duplicate(), length);
        }

        @Override
        public void writeTo(OutputStream os) throws IOException {
            buffer.getBytes(offset, os, length);
        }

        ByteBuf toByteBuf() {
            return buffer.duplicate();
        }

        @Override
        public String utf8ToString() {
            return buffer.toString(offset, length, StandardCharsets.UTF_8);
        }

        @Override
        public BytesRef toBytesRef() {
            if (buffer.hasArray()) {
                return new BytesRef(buffer.array(), buffer.arrayOffset() + offset, length);
            }
            final byte[] copy = new byte[length];
            buffer.getBytes(offset, copy);
            return new BytesRef(copy);
        }

        @Override
        public long ramBytesUsed() {
            return buffer.capacity();
        }

    }

    private static class ByteBufStreamInput extends StreamInput {

        private final ByteBuf buffer;
        private final int endIndex;

        ByteBufStreamInput(ByteBuf buffer, int length) {
            if (length > buffer.readableBytes()) {
                throw new IndexOutOfBoundsException();
            }
            this.buffer = buffer;
            int startIndex = buffer.readerIndex();
            endIndex = startIndex + length;
            buffer.markReaderIndex();
        }

        @Override
        public BytesReference readBytesReference(int length) throws IOException {
            // NOTE: It is unsafe to share a reference of the internal structure, so we
            // use the default implementation which will copy the bytes. It is unsafe because
            // a netty ByteBuf might be pooled which requires a manual release to prevent
            // memory leaks.
            return super.readBytesReference(length);
        }

        @Override
        public BytesRef readBytesRef(int length) throws IOException {
            // NOTE: It is unsafe to share a reference of the internal structure, so we
            // use the default implementation which will copy the bytes. It is unsafe because
            // a netty ByteBuf might be pooled which requires a manual release to prevent
            // memory leaks.
            return super.readBytesRef(length);
        }

        @Override
        public int available() throws IOException {
            return endIndex - buffer.readerIndex();
        }

        @Override
        protected void ensureCanReadBytes(int length) throws EOFException {
            int bytesAvailable = endIndex - buffer.readerIndex();
            if (bytesAvailable < length) {
                throw new EOFException("tried to read: " + length + " bytes but only " + bytesAvailable + " remaining");
            }
        }

        @Override
        public void mark(int readlimit) {
            buffer.markReaderIndex();
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public int read() throws IOException {
            if (available() == 0) {
                return -1;
            }
            return buffer.readByte() & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            int available = available();
            if (available == 0) {
                return -1;
            }

            len = Math.min(available, len);
            buffer.readBytes(b, off, len);
            return len;
        }

        @Override
        public short readShort() throws IOException {
            try {
                return buffer.readShort();
            } catch (IndexOutOfBoundsException ex) {
                EOFException eofException = new EOFException();
                eofException.initCause(ex);
                throw eofException;
            }
        }

        @Override
        public int readInt() throws IOException {
            try {
                return buffer.readInt();
            } catch (IndexOutOfBoundsException ex) {
                EOFException eofException = new EOFException();
                eofException.initCause(ex);
                throw eofException;
            }
        }

        @Override
        public long readLong() throws IOException {
            try {
                return buffer.readLong();
            } catch (IndexOutOfBoundsException ex) {
                EOFException eofException = new EOFException();
                eofException.initCause(ex);
                throw eofException;
            }
        }

        @Override
        public void reset() throws IOException {
            buffer.resetReaderIndex();
        }

        @Override
        public long skip(long n) throws IOException {
            if (n > Integer.MAX_VALUE) {
                return skipBytes(Integer.MAX_VALUE);
            } else {
                return skipBytes((int) n);
            }
        }

        public int skipBytes(int n) throws IOException {
            int nBytes = Math.min(available(), n);
            buffer.skipBytes(nBytes);
            return nBytes;
        }


        @Override
        public byte readByte() throws IOException {
            try {
                return buffer.readByte();
            } catch (IndexOutOfBoundsException ex) {
                EOFException eofException = new EOFException();
                eofException.initCause(ex);
                throw eofException;
            }
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            int read = read(b, offset, len);
            if (read < len) {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public void close() throws IOException {
            // nothing to do here
        }
    }
}
