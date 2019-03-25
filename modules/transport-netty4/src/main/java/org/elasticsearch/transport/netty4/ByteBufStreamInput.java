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

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * A Netty {@link ByteBuf} based {@link StreamInput}.
 */
class ByteBufStreamInput extends StreamInput {

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
