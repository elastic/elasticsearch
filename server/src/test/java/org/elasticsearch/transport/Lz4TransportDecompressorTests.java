/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class Lz4TransportDecompressorTests extends ESTestCase {

    private final Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));

    public void testSimpleCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            byte randomByte = randomByte();
            try (OutputStream lz4BlockStream = Compression.Scheme.lz4OutputStream(org.elasticsearch.core.Streams.noCloseStream(output))) {
                lz4BlockStream.write(randomByte);
            }

            BytesReference bytes = output.bytes();

            Lz4TransportDecompressor decompressor = new Lz4TransportDecompressor(recycler);
            int bytesConsumed = decompressor.decompress(bytes);
            assertEquals(bytes.length(), bytesConsumed);
            ReleasableBytesReference releasableBytesReference = decompressor.pollDecompressedPage(true);
            assertEquals(randomByte, releasableBytesReference.get(0));
            releasableBytesReference.close();
        }
    }

    public void testMultiPageCompression() throws IOException {
        int intsToWrite = 50000;
        int uncompressedLength = intsToWrite * 4;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                StreamOutput lz4BlockStream = new OutputStreamStreamOutput(
                    Compression.Scheme.lz4OutputStream(Streams.flushOnCloseStream(output))
                )
            ) {
                for (int i = 0; i < intsToWrite; ++i) {
                    int lowByte = (i & 0xFF);
                    if (lowByte < 128) {
                        lz4BlockStream.writeInt(0);
                    } else if (lowByte < 200) {
                        lz4BlockStream.writeInt(1);
                    } else {
                        lz4BlockStream.writeInt(i);
                    }
                }
            }

            BytesReference bytes = output.bytes();
            // Since 200 / 255 data is repeated, we should get a compression ratio of at least 50%
            assertThat(bytes.length(), lessThan(uncompressedLength / 2));

            Lz4TransportDecompressor decompressor = new Lz4TransportDecompressor(recycler);
            int bytesConsumed = decompressor.decompress(bytes);
            assertEquals(bytes.length(), bytesConsumed);

            int numOfUncompressedPages = uncompressedLength / PageCacheRecycler.BYTE_PAGE_SIZE;
            if (bytes.length() % PageCacheRecycler.BYTE_PAGE_SIZE > 0) {
                numOfUncompressedPages += 1;
            }

            ReleasableBytesReference[] polledReferences = new ReleasableBytesReference[numOfUncompressedPages];
            for (int i = 0; i < numOfUncompressedPages - 1; ++i) {
                polledReferences[i] = decompressor.pollDecompressedPage(false);
            }

            polledReferences[numOfUncompressedPages - 1] = decompressor.pollDecompressedPage(true);
            assertNull(decompressor.pollDecompressedPage(true));

            BytesReference composite = CompositeBytesReference.of(polledReferences);
            assertEquals(uncompressedLength, composite.length());
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < intsToWrite; ++i) {
                int lowByte = (i & 0xFF);
                if (lowByte < 128) {
                    assertEquals(0, streamInput.readInt());
                } else if (lowByte < 200) {
                    assertEquals(1, streamInput.readInt());
                } else {
                    assertEquals(i, streamInput.readInt());
                }
            }
            Releasables.close(polledReferences);
        }
    }

    public void testIncrementalMultiPageCompression() throws IOException {
        int intsToWrite = 50000;
        int uncompressedLength = intsToWrite * 4;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                StreamOutput lz4BlockStream = new OutputStreamStreamOutput(
                    Compression.Scheme.lz4OutputStream(Streams.flushOnCloseStream(output))
                )
            ) {
                for (int i = 0; i < intsToWrite; ++i) {
                    int lowByte = (i & 0xFF);
                    if (lowByte < 128) {
                        lz4BlockStream.writeInt(0);
                    } else if (lowByte < 200) {
                        lz4BlockStream.writeInt(1);
                    } else {
                        lz4BlockStream.writeInt(i);
                    }
                }
            }

            BytesReference bytes = output.bytes();
            // Since 200 / 255 data is repeated, we should get a compression ratio of at least 50%
            assertThat(bytes.length(), lessThan(uncompressedLength / 2));

            Lz4TransportDecompressor decompressor = new Lz4TransportDecompressor(recycler);

            int split1 = (int) (bytes.length() * 0.3);
            int split2 = (int) (bytes.length() * 0.65);
            BytesReference inbound1 = bytes.slice(0, split1);
            BytesReference inbound2 = bytes.slice(split1, split2 - split1);
            BytesReference inbound3 = bytes.slice(split2, bytes.length() - split2);

            int bytesConsumed1 = decompressor.decompress(inbound1);
            BytesReference next = CompositeBytesReference.of(inbound1.slice(bytesConsumed1, inbound1.length() - bytesConsumed1), inbound2);
            int bytesConsumed2 = decompressor.decompress(next);
            BytesReference next2 = CompositeBytesReference.of(next.slice(bytesConsumed2, next.length() - bytesConsumed2), inbound3);
            int bytesConsumed3 = decompressor.decompress(next2);
            assertEquals(bytes.length(), bytesConsumed1 + bytesConsumed2 + bytesConsumed3);

            int numOfUncompressedPages = uncompressedLength / PageCacheRecycler.BYTE_PAGE_SIZE;
            if (bytes.length() % PageCacheRecycler.BYTE_PAGE_SIZE > 0) {
                numOfUncompressedPages += 1;
            }

            ReleasableBytesReference[] polledReferences = new ReleasableBytesReference[numOfUncompressedPages];
            for (int i = 0; i < numOfUncompressedPages - 1; ++i) {
                polledReferences[i] = decompressor.pollDecompressedPage(false);
            }

            polledReferences[numOfUncompressedPages - 1] = decompressor.pollDecompressedPage(true);
            assertNull(decompressor.pollDecompressedPage(true));

            BytesReference composite = CompositeBytesReference.of(polledReferences);
            assertEquals(uncompressedLength, composite.length());
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < intsToWrite; ++i) {
                int lowByte = (i & 0xFF);
                if (lowByte < 128) {
                    assertEquals(0, streamInput.readInt());
                } else if (lowByte < 200) {
                    assertEquals(1, streamInput.readInt());
                } else {
                    assertEquals(i, streamInput.readInt());
                }
            }
            Releasables.close(polledReferences);

        }
    }

    /**
     * A block whose literal run declares more bytes than its {@code compressedLength} covers must be rejected by the
     * decoder rather than completed from whatever follows the block in the inbound buffer.
     */
    public void testCompressedBlockDecodingStopsAtDeclaredBoundary() throws IOException {
        final byte[] literals = "Lorem ipsum".getBytes(StandardCharsets.US_ASCII);
        final byte[] trailingBytes = "dolor sit amet consectetur adipiscing elit sed do eiusmod tempor".getBytes(StandardCharsets.US_ASCII);
        final int declaredLength = literals.length + randomIntBetween(1, trailingBytes.length);
        final byte[] block = literalRun(literals, declaredLength);
        final BytesReference bytes = compressedBlockMessage(block.length, declaredLength, block, trailingBytes);

        final Lz4TransportDecompressor decompressor = new Lz4TransportDecompressor(recycler);
        expectThrows(IllegalStateException.class, containsString("Malformed input at"), () -> decompressor.decompress(bytes));
        assertNull(decompressor.pollDecompressedPage(true));
    }

    /**
     * A block that decodes to its full {@code decompressedLength} before its {@code compressedLength} is used up must
     * be rejected rather than have the unconsumed remainder silently skipped.
     */
    public void testCompressedBlockRejectsUnconsumedTrailingBytes() throws IOException {
        final byte[] literals = "consectetur adipiscing".getBytes(StandardCharsets.US_ASCII);
        final byte[] block = literalRun(literals, literals.length);
        final byte[] unconsumedBytes = "do eiusmod tempor".getBytes(StandardCharsets.US_ASCII);
        final int compressedLength = block.length + unconsumedBytes.length;
        final BytesReference bytes = compressedBlockMessage(compressedLength, literals.length, block, unconsumedBytes);

        final Lz4TransportDecompressor decompressor = new Lz4TransportDecompressor(recycler);
        expectThrows(
            IllegalStateException.class,
            equalTo("stream corrupted: read " + block.length + " bytes, expected compressedLength(" + compressedLength + ")"),
            () -> decompressor.decompress(bytes)
        );
        assertNull(decompressor.pollDecompressedPage(true));
    }

    /**
     * A single LZ4 sequence declaring a literal run of {@code declaredLength} bytes, followed by {@code literals}.
     */
    private static byte[] literalRun(byte[] literals, int declaredLength) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (declaredLength < 15) {
            out.write(declaredLength << 4);
        } else {
            out.write(0xF0);
            int remaining = declaredLength - 15;
            while (remaining >= 255) {
                out.write(255);
                remaining -= 255;
            }
            out.write(remaining);
        }
        out.writeBytes(literals);
        return out.toByteArray();
    }

    /**
     * A transport message carrying one compressed LZ4 block with the given header lengths and payload chunks.
     */
    private static BytesReference compressedBlockMessage(int compressedLength, int decompressedLength, byte[]... payload) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(new byte[] { 'L', 'Z', '4', 0 }); // Compression.Scheme.LZ4_HEADER
        out.writeBytes("LZ4Block".getBytes(StandardCharsets.US_ASCII)); // Lz4TransportDecompressor.MAGIC_NUMBER
        out.write(Lz4TransportDecompressor.BLOCK_TYPE_COMPRESSED);
        out.writeBytes(le32(compressedLength));
        out.writeBytes(le32(decompressedLength));
        out.writeBytes(le32(0)); // checksum, not verified
        for (byte[] chunk : payload) {
            out.writeBytes(chunk);
        }
        return new BytesArray(out.toByteArray());
    }

    private static byte[] le32(int value) {
        return new byte[] { (byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24) };
    }
}
