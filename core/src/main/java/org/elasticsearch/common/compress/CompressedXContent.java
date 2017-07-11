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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 */
public final class CompressedXContent {

    private static int crc32(BytesReference data) {
        OutputStream dummy = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                // no-op
            }
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                // no-op
            }
        };
        CRC32 crc32 = new CRC32();
        try {
            data.writeTo(new CheckedOutputStream(dummy, crc32));
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return (int) crc32.getValue();
    }

    private final byte[] bytes;
    private final int crc32;

    // Used for serialization
    private CompressedXContent(byte[] compressed, int crc32) {
        this.bytes = compressed;
        this.crc32 = crc32;
        assertConsistent();
    }

    /**
     * Create a {@link CompressedXContent} out of a {@link ToXContent} instance.
     */
    public CompressedXContent(ToXContent xcontent, XContentType type, ToXContent.Params params) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        OutputStream compressedStream = CompressorFactory.COMPRESSOR.streamOutput(bStream);
        CRC32 crc32 = new CRC32();
        OutputStream checkedStream = new CheckedOutputStream(compressedStream, crc32);
        try (XContentBuilder builder = XContentFactory.contentBuilder(type, checkedStream)) {
            builder.startObject();
            xcontent.toXContent(builder, params);
            builder.endObject();
        }
        this.bytes = BytesReference.toBytes(bStream.bytes());
        this.crc32 = (int) crc32.getValue();
        assertConsistent();
    }

    /**
     * Create a {@link CompressedXContent} out of a serialized {@link ToXContent}
     * that may already be compressed.
     */
    public CompressedXContent(BytesReference data) throws IOException {
        Compressor compressor = CompressorFactory.compressor(data);
        if (compressor != null) {
            // already compressed...
            this.bytes = BytesReference.toBytes(data);
            this.crc32 = crc32(new BytesArray(uncompressed()));
        } else {
            BytesStreamOutput out = new BytesStreamOutput();
            try (OutputStream compressedOutput = CompressorFactory.COMPRESSOR.streamOutput(out)) {
                data.writeTo(compressedOutput);
            }
            this.bytes = BytesReference.toBytes(out.bytes());
            this.crc32 = crc32(data);
        }
        assertConsistent();
    }

    private void assertConsistent() {
        assert CompressorFactory.compressor(new BytesArray(bytes)) != null;
        assert this.crc32 == crc32(new BytesArray(uncompressed()));
    }

    public CompressedXContent(byte[] data) throws IOException {
        this(new BytesArray(data));
    }

    public CompressedXContent(String str) throws IOException {
        this(new BytesArray(new BytesRef(str)));
    }

    /** Return the compressed bytes. */
    public byte[] compressed() {
        return this.bytes;
    }

    /** Return the compressed bytes as a {@link BytesReference}. */
    public BytesReference compressedReference() {
        return new BytesArray(bytes);
    }

    /** Return the uncompressed bytes. */
    public byte[] uncompressed() {
        try {
            return BytesReference.toBytes(CompressorFactory.uncompress(new BytesArray(bytes)));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed string", e);
        }
    }

    public String string() throws IOException {
        return new BytesRef(uncompressed()).utf8ToString();
    }

    public static CompressedXContent readCompressedString(StreamInput in) throws IOException {
        int crc32 = in.readInt();
        byte[] compressed = new byte[in.readVInt()];
        in.readBytes(compressed, 0, compressed.length);
        return new CompressedXContent(compressed, crc32);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(crc32);
        out.writeVInt(bytes.length);
        out.writeBytes(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedXContent that = (CompressedXContent) o;

        if (Arrays.equals(compressed(), that.compressed())) {
            return true;
        }

        if (crc32 != that.crc32) {
            return false;
        }

        return Arrays.equals(uncompressed(), that.uncompressed());
    }

    @Override
    public int hashCode() {
        return crc32;
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
