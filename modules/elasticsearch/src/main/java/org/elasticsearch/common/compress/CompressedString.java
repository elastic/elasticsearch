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

package org.elasticsearch.common.compress;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.compress.lzf.LZFDecoder;
import org.elasticsearch.common.compress.lzf.LZFEncoder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author kimchy (shay.banon)
 */
public class CompressedString implements Streamable {

    private byte[] bytes;

    CompressedString() {
    }

    public CompressedString(String str) throws IOException {
        UnicodeUtil.UTF8Result result = Unicode.unsafeFromStringAsUtf8(str);
        this.bytes = LZFEncoder.encode(result.result, result.length);
    }

    public byte[] compressed() {
        return this.bytes;
    }

    public byte[] uncompressed() throws IOException {
        return LZFDecoder.decode(bytes);
    }

    public String string() throws IOException {
        return Unicode.fromBytes(LZFDecoder.decode(bytes));
    }

    public static CompressedString readCompressedString(StreamInput in) throws IOException {
        CompressedString compressedString = new CompressedString();
        compressedString.readFrom(in);
        return compressedString;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bytes.length);
        out.writeBytes(bytes);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedString that = (CompressedString) o;

        if (!Arrays.equals(bytes, that.bytes)) return false;

        return true;
    }

    @Override public int hashCode() {
        return bytes != null ? Arrays.hashCode(bytes) : 0;
    }

    @Override public String toString() {
        try {
            return string();
        } catch (IOException e) {
            return "_na_";
        }
    }
}
