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
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 */
public class CompressedString implements Streamable {

    private byte[] bytes;

    CompressedString() {
    }

    /**
     * Constructor assuming the data provided is compressed (UTF8). It uses the provided
     * array without copying it.
     */
    public CompressedString(byte[] compressed) {
        this.bytes = compressed;
    }

    public CompressedString(BytesReference data) throws IOException {
        Compressor compressor = CompressorFactory.compressor(data);
        if (compressor != null) {
            // already compressed...
            this.bytes = data.toBytes();
        } else {
            BytesArray bytesArray = data.toBytesArray();
            this.bytes = CompressorFactory.defaultCompressor().compress(bytesArray.array(), bytesArray.arrayOffset(), bytesArray.length());
        }
    }

    /**
     * Constructs a new compressed string, assuming the bytes are UTF8, by copying it over.
     *
     * @param data   The byte array
     * @param offset Offset into the byte array
     * @param length The length of the data
     * @throws IOException
     */
    public CompressedString(byte[] data, int offset, int length) throws IOException {
        Compressor compressor = CompressorFactory.compressor(data, offset, length);
        if (compressor != null) {
            // already compressed...
            this.bytes = Arrays.copyOfRange(data, offset, offset + length);
        } else {
            // default to LZF
            this.bytes = CompressorFactory.defaultCompressor().compress(data, offset, length);
        }
    }

    public CompressedString(String str) throws IOException {
        BytesRef result = new BytesRef(str);
        this.bytes = CompressorFactory.defaultCompressor().compress(result.bytes, result.offset, result.length);
    }

    public byte[] compressed() {
        return this.bytes;
    }

    public byte[] uncompressed() throws IOException {
        Compressor compressor = CompressorFactory.compressor(bytes);
        return compressor.uncompress(bytes, 0, bytes.length);
    }

    public String string() throws IOException {
        return new BytesRef(uncompressed()).utf8ToString();
    }

    public static CompressedString readCompressedString(StreamInput in) throws IOException {
        CompressedString compressedString = new CompressedString();
        compressedString.readFrom(in);
        return compressedString;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bytes.length);
        out.writeBytes(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedString that = (CompressedString) o;

        if (!Arrays.equals(bytes, that.bytes)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return bytes != null ? Arrays.hashCode(bytes) : 0;
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
