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

package org.elasticsearch.common.compress;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 */
public final class CompressedString {

    private final byte[] bytes;
    private int hashCode;

    public CompressedString(BytesReference data) throws IOException {
        Compressor compressor = CompressorFactory.compressor(data);
        if (compressor != null) {
            // already compressed...
            this.bytes = data.toBytes();
        } else {
            BytesArray bytesArray = data.toBytesArray();
            this.bytes = CompressorFactory.defaultCompressor().compress(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length());
            assert CompressorFactory.compressor(bytes) != null;
        }

    }

    public CompressedString(byte[] data, int offset, int length) throws IOException {
        this(new BytesArray(data, offset, length));
    }

    public CompressedString(byte[] data) throws IOException {
        this(data, 0, data.length);
    }

    public CompressedString(String str) throws IOException {
        this(new BytesArray(new BytesRef(str)));
    }

    /** Return the compressed bytes. */
    public byte[] compressed() {
        return this.bytes;
    }

    /** Return the uncompressed bytes. */
    public byte[] uncompressed() {
        Compressor compressor = CompressorFactory.compressor(bytes);
        assert compressor != null;
        try {
            return compressor.uncompress(bytes, 0, bytes.length);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed string", e);
        }
    }

    public String string() throws IOException {
        return new BytesRef(uncompressed()).utf8ToString();
    }

    public static CompressedString readCompressedString(StreamInput in) throws IOException {
        byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return new CompressedString(bytes);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bytes.length);
        out.writeBytes(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedString that = (CompressedString) o;

        if (Arrays.equals(compressed(), that.compressed())) {
            return true;
        }

        return Arrays.equals(uncompressed(), that.uncompressed());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int h = Arrays.hashCode(uncompressed());
            if (h == 0) {
                h = 1;
            }
            hashCode = h;
        }
        return hashCode;
    }

    @Override
    public String toString() {
        try {
            return string();
        } catch (IOException e) {
            return "_na_";
        }
    }
}
