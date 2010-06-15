/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.util.Unicode;

import java.io.EOFException;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BytesStreamInput extends StreamInput {

    protected byte buf[];

    protected int pos;

    protected int count;

    public BytesStreamInput(byte buf[]) {
        this(buf, 0, buf.length);
    }

    public BytesStreamInput(byte buf[], int position, int count) {
        this.buf = buf;
        this.pos = position;
        this.count = count;
    }

    @Override public int read() throws IOException {
        return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    @Override public byte readByte() throws IOException {
        if (pos >= count) {
            throw new EOFException();
        }
        return buf[pos++];
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (pos >= count) {
            throw new EOFException();
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            throw new EOFException();
        }
        System.arraycopy(buf, pos, b, offset, len);
        pos += len;
    }

    @Override public String readUTF() throws IOException {
        int length = readVInt();
        if (pos + length > count) {
            throw new EOFException();
        }
        String str = Unicode.fromBytes(buf, pos, length);
        pos += length;
        return str;
    }

    @Override public void reset() throws IOException {
        pos = 0;
    }

    @Override public void close() throws IOException {
        // nothing to do here...
    }
}
