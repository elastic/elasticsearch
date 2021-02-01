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
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

public final class BytesArray extends AbstractBytesReference {

    public static final BytesArray EMPTY = new BytesArray(BytesRef.EMPTY_BYTES, 0, 0);
    private final byte[] bytes;
    private final int offset;
    private final int length;

    public BytesArray(String bytes) {
        this(new BytesRef(bytes));
    }

    public BytesArray(BytesRef bytesRef) {
        this(bytesRef, false);
    }

    public BytesArray(BytesRef bytesRef, boolean deepCopy) {
        if (deepCopy) {
            bytesRef = BytesRef.deepCopyOf(bytesRef);
        }
        bytes = bytesRef.bytes;
        offset = bytesRef.offset;
        length = bytesRef.length;
    }

    public BytesArray(byte[] bytes) {
        this(bytes, 0, bytes.length);
    }

    public BytesArray(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public byte get(int index) {
        return bytes[offset + index];
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int hashCode() {
        // NOOP override to satisfy Checkstyle's EqualsHashCode
        return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof BytesArray) {
            final BytesArray that = (BytesArray) other;
            return Arrays.equals(bytes, offset, offset + length, that.bytes, that.offset, that.offset + that.length);
        }
        return super.equals(other);
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from == 0 && this.length == length) {
            return this;
        }
        Objects.checkFromIndexSize(from, length, this.length);
        return new BytesArray(bytes, offset + from, length);
    }

    public byte[] array() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    @Override
    public BytesRef toBytesRef() {
        return new BytesRef(bytes, offset, length);
    }

    @Override
    public long ramBytesUsed() {
        return bytes.length;
    }

    @Override
    public StreamInput streamInput() {
        return StreamInput.wrap(bytes, offset, length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes, offset, length);
    }
}
