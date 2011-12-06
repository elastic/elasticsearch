/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

public class BytesHolder implements Streamable {

    private byte[] bytes;
    private int offset;
    private int length;

    BytesHolder() {

    }

    public BytesHolder(byte[] bytes) {
        this.bytes = bytes;
        this.offset = 0;
        this.length = bytes.length;
    }

    public BytesHolder(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    public byte[] copyBytes() {
        return Arrays.copyOfRange(bytes, offset, offset + length);
    }

    public byte[] bytes() {
        return bytes;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public static BytesHolder readBytesHolder(StreamInput in) throws IOException {
        BytesHolder holder = new BytesHolder();
        holder.readFrom(in);
        return holder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        offset = 0;
        length = in.readVInt();
        bytes = new byte[length];
        in.readFully(bytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(length);
        out.writeBytes(bytes, offset, length);
    }

    @Override
    public boolean equals(Object obj) {
        return bytesEquals((BytesHolder) obj);
    }

    public boolean bytesEquals(BytesHolder other) {
        if (length == other.length) {
            int otherUpto = other.offset;
            final byte[] otherBytes = other.bytes;
            final int end = offset + length;
            for (int upto = offset; upto < end; upto++, otherUpto++) {
                if (bytes[upto] != otherBytes[otherUpto]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 0;
        final int end = offset + length;
        for (int i = offset; i < end; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }
}