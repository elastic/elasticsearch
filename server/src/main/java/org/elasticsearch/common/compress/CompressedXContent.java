/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 */
public final class CompressedXContent {

    private static final ThreadLocal<Inflater> inflater1 = ThreadLocal.withInitial(() -> new Inflater(true));
    private static final ThreadLocal<ByteBuffer> buffer1 =
        ThreadLocal.withInitial(() -> ByteBuffer.allocate(DeflateCompressor.BUFFER_SIZE));

    private static final ThreadLocal<Inflater> inflater2 = ThreadLocal.withInitial(() -> new Inflater(true));
    private static final ThreadLocal<ByteBuffer> buffer2 =
        ThreadLocal.withInitial(() -> ByteBuffer.allocate(DeflateCompressor.BUFFER_SIZE));

    private static int crc32(BytesReference data) {
        CRC32 crc32 = new CRC32();
        try {
            data.writeTo(new CheckedOutputStream(Streams.NULL_OUTPUT_STREAM, crc32));
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return (int) crc32.getValue();
    }

    private static int crc32FromCompressed(byte[] compressed) {
        final Inflater inflater = inflater1.get();
        final ByteBuffer buffer = buffer1.get();
        CRC32 crc32 = new CRC32();
        try {
            setInflaterInput(compressed, inflater);
            do {
                buffer.clear();
                if (inflater.inflate(buffer) > 0) {
                    crc32.update(buffer.flip());
                }
            } while (inflater.finished() == false);
            return (int) crc32.getValue();
        } catch (DataFormatException e) {
            throw new ElasticsearchException(e);
        } finally {
            inflater.reset();
        }
    }

    /**
     * Set the given bytes as inflater input, accounting for the fact that they start with our header of size
     * {@link DeflateCompressor#HEADER_SIZE}.
     */
    private static void setInflaterInput(byte[] compressed, Inflater inflater) {
        inflater.setInput(compressed, DeflateCompressor.HEADER_SIZE, compressed.length - DeflateCompressor.HEADER_SIZE);
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
        CRC32 crc32 = new CRC32();
        OutputStream checkedStream = new CheckedOutputStream(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream), crc32);
        try (XContentBuilder builder = XContentFactory.contentBuilder(type, checkedStream)) {
            if (xcontent.isFragment()) {
                builder.startObject();
            }
            xcontent.toXContent(builder, params);
            if (xcontent.isFragment()) {
                builder.endObject();
            }
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
            this.crc32 = crc32FromCompressed(this.bytes);
        } else {
            this.bytes = BytesReference.toBytes(CompressorFactory.COMPRESSOR.compress(data));
            this.crc32 = crc32(data);
        }
        assertConsistent();
    }

    private void assertConsistent() {
        assert CompressorFactory.compressor(new BytesArray(bytes)) != null;
        assert this.crc32 == crc32(uncompressed());
        assert this.crc32 == crc32FromCompressed(bytes);
    }

    public CompressedXContent(byte[] data) throws IOException {
        this(new BytesArray(data));
    }

    public CompressedXContent(String str) throws IOException {
        this(new BytesArray(str.getBytes(StandardCharsets.UTF_8)));
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
    public BytesReference uncompressed() {
        try {
            return CompressorFactory.uncompress(new BytesArray(bytes));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed string", e);
        }
    }

    public String string() {
        return uncompressed().utf8ToString();
    }

    public static CompressedXContent readCompressedString(StreamInput in) throws IOException {
        int crc32 = in.readInt();
        return new CompressedXContent(in.readByteArray(), crc32);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(crc32);
        out.writeByteArray(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedXContent that = (CompressedXContent) o;

        if (crc32 != that.crc32) {
            return false;
        }

        if (Arrays.equals(bytes, that.bytes)) {
            return true;
        }
        return equals(bytes, that.bytes);
    }

    private static boolean equals(byte[] bytes1, byte[] bytes2) {
        final Inflater inf1 = inflater1.get();
        final Inflater def2 = inflater2.get();
        final ByteBuffer buf1 = buffer1.get();
        final ByteBuffer buf2 = buffer2.get();
        try {
            setInflaterInput(bytes1, inf1);
            setInflaterInput(bytes2, def2);
            while (true) {
                buf1.clear();
                buf2.clear();
                while (inf1.inflate(buf1) > 0 && buf1.hasRemaining()) ;
                while (def2.inflate(buf2) > 0 && buf2.hasRemaining()) ;
                if (buf1.flip().equals(buf2.flip()) == false) {
                    return false;
                }
                if (inf1.finished()) {
                    return def2.finished();
                }
            }
        } catch (DataFormatException e) {
            throw new ElasticsearchException(e);
        } finally {
            inf1.reset();
            def2.reset();
        }
    }

    @Override
    public int hashCode() {
        return crc32;
    }

    @Override
    public String toString() {
        return string();
    }
}
