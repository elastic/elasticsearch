/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 */
public final class CompressedXContent {

    private static final ThreadLocal<InflaterAndBuffer> inflater1 = ThreadLocal.withInitial(InflaterAndBuffer::new);
    private static final ThreadLocal<InflaterAndBuffer> inflater2 = ThreadLocal.withInitial(InflaterAndBuffer::new);

    private static String sha256(BytesReference data) {
        MessageDigest messageDigest = MessageDigests.sha256();
        try {
            data.writeTo(new DigestOutputStream(Streams.NULL_OUTPUT_STREAM, messageDigest));
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return MessageDigests.toHexString(messageDigest.digest());
    }

    private static String sha256FromCompressed(byte[] compressed) {
        MessageDigest messageDigest = MessageDigests.sha256();
        try (InflaterAndBuffer inflaterAndBuffer = inflater1.get()) {
            final Inflater inflater = inflaterAndBuffer.inflater;
            final ByteBuffer buffer = inflaterAndBuffer.buffer;
            assert assertBufferIsCleared(buffer);
            setInflaterInput(compressed, inflater);
            do {
                if (inflater.inflate(buffer) > 0) {
                    messageDigest.update(buffer.flip());
                }
                buffer.clear();
            } while (inflater.finished() == false);
            return MessageDigests.toHexString(messageDigest.digest());
        } catch (DataFormatException e) {
            throw new ElasticsearchException(e);
        }
    }

    private final byte[] bytes;
    private final String sha256;

    // Used for serialization
    private CompressedXContent(byte[] compressed, String sha256) {
        this.bytes = compressed;
        this.sha256 = sha256;
        assertConsistent();
    }

    public CompressedXContent(ToXContent xcontent) throws IOException {
        this(xcontent, ToXContent.EMPTY_PARAMS);
    }

    /**
     * Create a {@link CompressedXContent} out of a {@link ToXContent} instance.
     */
    public CompressedXContent(ToXContent xcontent, ToXContent.Params params) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        MessageDigest messageDigest = MessageDigests.sha256();
        OutputStream checkedStream = new DigestOutputStream(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream), messageDigest);
        try (XContentBuilder builder = XContentFactory.jsonBuilder(checkedStream)) {
            if (xcontent.isFragment()) {
                builder.startObject();
            }
            xcontent.toXContent(builder, params);
            if (xcontent.isFragment()) {
                builder.endObject();
            }
        }
        this.bytes = BytesReference.toBytes(bStream.bytes());
        this.sha256 = MessageDigests.toHexString(messageDigest.digest());
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
            this.sha256 = sha256FromCompressed(this.bytes);
        } else {
            this.bytes = BytesReference.toBytes(CompressorFactory.COMPRESSOR.compress(data));
            this.sha256 = sha256(data);
        }
        assertConsistent();
    }

    private void assertConsistent() {
        assert CompressorFactory.compressor(new BytesArray(bytes)) != null;
        assert this.sha256.equals(sha256(uncompressed()));
        assert this.sha256.equals(sha256FromCompressed(bytes));
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

    public String getSha256() {
        return sha256;
    }

    public static CompressedXContent readCompressedString(StreamInput in) throws IOException {
        final String sha256;
        final byte[] compressedData;
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            sha256 = in.readString();
            compressedData = in.readByteArray();
        } else {
            int crc32 = in.readInt();
            compressedData = in.readByteArray();
            sha256 = sha256FromCompressed(compressedData);
        }
        return new CompressedXContent(compressedData, sha256);
    }

    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeString(sha256);
        } else {
            int crc32 = crc32FromCompressed(bytes);
            out.writeInt(crc32);
        }
        out.writeByteArray(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedXContent that = (CompressedXContent) o;
        return sha256.equals(that.sha256);
    }

    // package private for testing
    static boolean equalsWhenUncompressed(byte[] compressed1, byte[] compressed2) {
        try (InflaterAndBuffer inflaterAndBuffer1 = inflater1.get(); InflaterAndBuffer inflaterAndBuffer2 = inflater2.get()) {
            final Inflater inf1 = inflaterAndBuffer1.inflater;
            final Inflater inf2 = inflaterAndBuffer2.inflater;
            setInflaterInput(compressed1, inf1);
            setInflaterInput(compressed2, inf2);
            final ByteBuffer buf1 = inflaterAndBuffer1.buffer;
            assert assertBufferIsCleared(buf1);
            final ByteBuffer buf2 = inflaterAndBuffer2.buffer;
            assert assertBufferIsCleared(buf2);
            while (true) {
                while (inf1.inflate(buf1) > 0 && buf1.hasRemaining())
                    ;
                while (inf2.inflate(buf2) > 0 && buf2.hasRemaining())
                    ;
                if (buf1.flip().equals(buf2.flip()) == false) {
                    return false;
                }
                if (inf1.finished()) {
                    // if the first inflater is done but the second one still has data we fail here, if it's the other way around we fail
                    // on the next round because we will only read bytes into 2
                    return inf2.finished();
                }
                buf1.clear();
                buf2.clear();
            }
        } catch (DataFormatException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public int hashCode() {
        return sha256.hashCode();
    }

    @Override
    public String toString() {
        return string();
    }

    private static int crc32FromCompressed(byte[] compressed) {
        CRC32 crc32 = new CRC32();
        try (InflaterAndBuffer inflaterAndBuffer = inflater1.get()) {
            final Inflater inflater = inflaterAndBuffer.inflater;
            final ByteBuffer buffer = inflaterAndBuffer.buffer;
            assert assertBufferIsCleared(buffer);
            setInflaterInput(compressed, inflater);
            do {
                if (inflater.inflate(buffer) > 0) {
                    crc32.update(buffer.flip());
                }
                buffer.clear();
            } while (inflater.finished() == false);
            return (int) crc32.getValue();
        } catch (DataFormatException e) {
            throw new ElasticsearchException(e);
        }
    }

    /**
     * Set the given bytes as inflater input, accounting for the fact that they start with our header of size
     * {@link DeflateCompressor#HEADER_SIZE}.
     */
    private static void setInflaterInput(byte[] compressed, Inflater inflater) {
        inflater.setInput(compressed, DeflateCompressor.HEADER_SIZE, compressed.length - DeflateCompressor.HEADER_SIZE);
    }

    private static boolean assertBufferIsCleared(ByteBuffer buffer) {
        assert buffer.limit() == buffer.capacity()
            : "buffer limit != capacity, was [" + buffer.limit() + "] and [" + buffer.capacity() + "]";
        assert buffer.position() == 0 : "buffer position != 0, was [" + buffer.position() + "]";
        return true;
    }

    private static final class InflaterAndBuffer implements Releasable {

        final ByteBuffer buffer = ByteBuffer.allocate(DeflateCompressor.BUFFER_SIZE);

        final Inflater inflater = new Inflater(true);

        @Override
        public void close() {
            inflater.reset();
            buffer.clear();
        }
    }
}
