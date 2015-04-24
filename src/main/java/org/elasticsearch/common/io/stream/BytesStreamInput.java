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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.EOFException;
import java.io.IOException;

/**
 *
 */
public class BytesStreamInput extends StreamInput {

    protected byte buf[];

    protected int pos;

    protected int end;

    public BytesStreamInput(BytesReference bytes) {
        if (!bytes.hasArray()) {
            bytes = bytes.toBytesArray();
        }
        this.buf = bytes.array();
        this.pos = bytes.arrayOffset();
        this.end = pos + bytes.length();
    }

    public BytesStreamInput(byte buf[]) {
        this(buf, 0, buf.length);
    }

    public BytesStreamInput(byte buf[], int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.end = offset + length;
    }

    @Override
    public BytesReference readBytesReference(int length) throws IOException {
        BytesArray bytes = new BytesArray(buf, pos, length);
        pos += length;
        return bytes;
    }

    @Override
    public BytesRef readBytesRef(int length) throws IOException {
        BytesRef bytes = new BytesRef(buf, pos, length);
        pos += length;
        return bytes;
    }

    @Override
    public long skip(long n) throws IOException {
        if (pos + n > end) {
            n = end - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    public int position() {
        return this.pos;
    }

    @Override
    public int read() throws IOException {
        return (pos < end) ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= end) {
            return -1;
        }
        if (pos + len > end) {
            len = end - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    public byte[] underlyingBuffer() {
        return buf;
    }

    @Override
    public byte readByte() throws IOException {
        if (pos >= end) {
            throw new EOFException();
        }
        return buf[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (len == 0) {
            return;
        }
        if (pos >= end) {
            throw new EOFException();
        }
        if (pos + len > end) {
            len = end - pos;
        }
        if (len <= 0) {
            throw new EOFException();
        }
        System.arraycopy(buf, pos, b, offset, len);
        pos += len;
    }

    @Override
    public void reset() throws IOException {
        pos = 0;
    }

    @Override
    public void close() throws IOException {
        // nothing to do here...
    }
}
