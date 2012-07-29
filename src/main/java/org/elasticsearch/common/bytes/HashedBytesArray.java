/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bytes;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 *
 */
public class HashedBytesArray implements BytesReference {

    private final byte[] bytes;

    // we pre-compute the hashCode for better performance (especially in IdCache)
    private final int hashCode;

    public HashedBytesArray(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
    }

    public HashedBytesArray(String str) {
        this(Unicode.fromStringAsBytes(str));
    }

    @Override
    public byte get(int index) {
        return bytes[index];
    }

    @Override
    public int length() {
        return bytes.length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        if (from < 0 || (from + length) > bytes.length) {
            throw new ElasticSearchIllegalArgumentException("can't slice a buffer with length [" + bytes.length + "], with slice parameters from [" + from + "], length [" + length + "]");
        }
        return new BytesArray(bytes, from, length);
    }

    @Override
    public StreamInput streamInput() {
        return new BytesStreamInput(bytes, false);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(bytes);
    }

    @Override
    public byte[] toBytes() {
        return bytes;
    }

    @Override
    public BytesArray toBytesArray() {
        return new BytesArray(bytes);
    }

    @Override
    public BytesArray copyBytesArray() {
        byte[] copy = new byte[bytes.length];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return new BytesArray(copy);
    }

    @Override
    public ChannelBuffer toChannelBuffer() {
        return ChannelBuffers.wrappedBuffer(bytes, 0, bytes.length);
    }

    @Override
    public boolean hasArray() {
        return true;
    }

    @Override
    public byte[] array() {
        return bytes;
    }

    @Override
    public int arrayOffset() {
        return 0;
    }

    @Override
    public String toUtf8() {
        if (bytes.length == 0) {
            return "";
        }
        return new String(bytes, Charsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        HashedBytesArray bytesWrap = (HashedBytesArray) o;
        return Arrays.equals(bytes, bytesWrap.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
