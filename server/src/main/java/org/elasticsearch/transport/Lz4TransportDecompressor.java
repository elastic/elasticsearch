/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Locale;

/**
 * This file is forked from the https://netty.io project. In particular it forks the following file
 * io.netty.handler.codec.compression.Lz4FrameDecoder.
 *
 * It modifies the original netty code to operate on byte arrays opposed to ByteBufs.
 * Additionally, it integrates the decompression code to work in the Elasticsearch transport
 * pipeline, Finally, it replaces the custom Netty decoder exceptions.
 *
 * This class is necessary as Netty is not a dependency in Elasticsearch server module.
 */
public class Lz4TransportDecompressor implements TransportDecompressor {

    private static final ThreadLocal<byte[]> DECOMPRESSED = ThreadLocal.withInitial(() -> BytesRef.EMPTY_BYTES);
    private static final ThreadLocal<byte[]> COMPRESSED = ThreadLocal.withInitial(() -> BytesRef.EMPTY_BYTES);

    /**
     * Magic number of LZ4 block.
     */
    static final long MAGIC_NUMBER = (long) 'L' << 56 | (long) 'Z' << 48 | (long) '4' << 40 | (long) 'B' << 32 | 'l' << 24 | 'o' << 16 | 'c'
        << 8 | 'k';

    static final int HEADER_LENGTH = 8 +  // magic number
        1 +  // token
        4 +  // compressed length
        4 +  // decompressed length
        4;   // checksum

    /**
     * Base value for compression level.
     */
    static final int COMPRESSION_LEVEL_BASE = 10;

    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << COMPRESSION_LEVEL_BASE + 0x0F;   // 32 M
    static final int DEFAULT_BLOCK_SIZE = 1 << 16;  // 64 KB

    static final int BLOCK_TYPE_NON_COMPRESSED = 0x10;
    static final int BLOCK_TYPE_COMPRESSED = 0x20;

    private enum State {
        INIT_BLOCK,
        DECOMPRESS_DATA,
        FINISHED,
        CORRUPTED
    }

    private State currentState = State.INIT_BLOCK;

    /**
     * Underlying decompressor in use.
     */
    private LZ4FastDecompressor decompressor;

    /**
     * Type of current block.
     */
    private int blockType;

    /**
     * Compressed length of current incoming block.
     */
    private int compressedLength;

    /**
     * Decompressed length of current incoming block.
     */
    private int decompressedLength;

    private final Recycler<BytesRef> recycler;
    private final ArrayDeque<Recycler.V<BytesRef>> pages;
    private int pageOffset = 0;
    private int pageLength = 0;
    private boolean hasSkippedESHeader = false;

    public Lz4TransportDecompressor(Recycler<BytesRef> recycler) {
        this.decompressor = Compression.Scheme.lz4Decompressor();
        this.recycler = recycler;
        this.pages = new ArrayDeque<>(4);
    }

    @Override
    public ReleasableBytesReference pollDecompressedPage(boolean isEOS) {
        if (pages.isEmpty()) {
            return null;
        } else if (pages.size() == 1) {
            if (isEOS) {
                Recycler.V<BytesRef> page = pages.pollFirst();
                BytesArray delegate = new BytesArray(page.v().bytes, page.v().offset, pageOffset);
                ReleasableBytesReference reference = new ReleasableBytesReference(delegate, page);
                pageLength = 0;
                pageOffset = 0;
                return reference;
            } else {
                return null;
            }
        } else {
            Recycler.V<BytesRef> page = pages.pollFirst();
            return new ReleasableBytesReference(new BytesArray(page.v()), page);
        }
    }

    @Override
    public Compression.Scheme getScheme() {
        return Compression.Scheme.LZ4;
    }

    @Override
    public void close() {
        for (Recycler.V<BytesRef> page : pages) {
            page.close();
        }
    }

    @Override
    public int decompress(BytesReference bytesReference) throws IOException {
        int bytesConsumed = 0;
        if (hasSkippedESHeader == false) {
            hasSkippedESHeader = true;
            int esHeaderLength = Compression.Scheme.HEADER_LENGTH;
            bytesReference = bytesReference.slice(esHeaderLength, bytesReference.length() - esHeaderLength);
            bytesConsumed += esHeaderLength;
        }

        while (true) {
            int consumed = decodeBlock(bytesReference);
            bytesConsumed += consumed;
            int newLength = bytesReference.length() - consumed;
            if (consumed > 0 && newLength > 0) {
                bytesReference = bytesReference.slice(consumed, newLength);
            } else {
                break;
            }
        }

        return bytesConsumed;
    }

    private int decodeBlock(BytesReference reference) throws IOException {
        int bytesConsumed = 0;
        try {
            switch (currentState) {
                case INIT_BLOCK:
                    if (reference.length() < HEADER_LENGTH) {
                        return bytesConsumed;
                    }
                    try (StreamInput in = reference.streamInput()) {
                        final long magic = in.readLong();
                        if (magic != MAGIC_NUMBER) {
                            throw new IllegalStateException("unexpected block identifier");
                        }

                        final int token = in.readByte();
                        final int compressionLevel = (token & 0x0F) + COMPRESSION_LEVEL_BASE;
                        int blockType = token & 0xF0;

                        int compressedLength = Integer.reverseBytes(in.readInt());
                        if (compressedLength < 0 || compressedLength > MAX_BLOCK_SIZE) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "invalid compressedLength: %d (expected: 0-%d)",
                                    compressedLength,
                                    MAX_BLOCK_SIZE
                                )
                            );
                        }

                        int decompressedLength = Integer.reverseBytes(in.readInt());
                        final int maxDecompressedLength = 1 << compressionLevel;
                        if (decompressedLength < 0 || decompressedLength > maxDecompressedLength) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "invalid decompressedLength: %d (expected: 0-%d)",
                                    decompressedLength,
                                    maxDecompressedLength
                                )
                            );
                        }
                        if (decompressedLength == 0 && compressedLength != 0
                            || decompressedLength != 0 && compressedLength == 0
                            || blockType == BLOCK_TYPE_NON_COMPRESSED && decompressedLength != compressedLength) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "stream corrupted: compressedLength(%d) and decompressedLength(%d) mismatch",
                                    compressedLength,
                                    decompressedLength
                                )
                            );
                        }

                        // Read int where checksum would normally be written
                        in.readInt();
                        bytesConsumed += HEADER_LENGTH;

                        if (decompressedLength == 0) {
                            currentState = State.FINISHED;
                            decompressor = null;
                            break;
                        }

                        this.blockType = blockType;
                        this.compressedLength = compressedLength;
                        this.decompressedLength = decompressedLength;
                    }

                    currentState = State.DECOMPRESS_DATA;
                    break;
                case DECOMPRESS_DATA:
                    if (reference.length() < compressedLength) {
                        break;
                    }

                    byte[] decompressed = getThreadLocalBuffer(DECOMPRESSED, decompressedLength);

                    try {
                        switch (blockType) {
                            case BLOCK_TYPE_NON_COMPRESSED:
                                try (StreamInput streamInput = reference.streamInput()) {
                                    streamInput.readBytes(decompressed, 0, decompressedLength);
                                }
                                break;
                            case BLOCK_TYPE_COMPRESSED:
                                BytesRef ref = reference.iterator().next();
                                final byte[] compressed;
                                final int compressedOffset;
                                if (ref.length >= compressedLength) {
                                    compressed = ref.bytes;
                                    compressedOffset = ref.offset;
                                } else {
                                    compressed = getThreadLocalBuffer(COMPRESSED, compressedLength);
                                    compressedOffset = 0;
                                    try (StreamInput streamInput = reference.streamInput()) {
                                        streamInput.readBytes(compressed, 0, compressedLength);
                                    }
                                }
                                decompressor.decompress(compressed, compressedOffset, decompressed, 0, decompressedLength);
                                break;
                            default:
                                throw new IllegalStateException(
                                    String.format(
                                        Locale.ROOT,
                                        "unexpected blockType: %d (expected: %d or %d)",
                                        blockType,
                                        BLOCK_TYPE_NON_COMPRESSED,
                                        BLOCK_TYPE_COMPRESSED
                                    )
                                );
                        }
                        // Skip inbound bytes after we processed them.
                        bytesConsumed += compressedLength;

                        int bytesToCopy = decompressedLength;
                        int uncompressedOffset = 0;
                        while (bytesToCopy > 0) {
                            final boolean isNewPage = pageOffset == pageLength;
                            if (isNewPage) {
                                Recycler.V<BytesRef> newPage = recycler.obtain();
                                pageOffset = 0;
                                pageLength = newPage.v().length;
                                assert newPage.v().length > 0;
                                pages.add(newPage);
                            }
                            final Recycler.V<BytesRef> page = pages.getLast();

                            int toCopy = Math.min(bytesToCopy, pageLength - pageOffset);
                            System.arraycopy(decompressed, uncompressedOffset, page.v().bytes, page.v().offset + pageOffset, toCopy);
                            pageOffset += toCopy;
                            bytesToCopy -= toCopy;
                            uncompressedOffset += toCopy;
                        }
                        currentState = State.INIT_BLOCK;
                    } catch (LZ4Exception e) {
                        throw new IllegalStateException(e);
                    }
                    break;
                case FINISHED:
                    break;
                case CORRUPTED:
                    throw new IllegalStateException("LZ4 stream corrupted.");
                default:
                    throw new IllegalStateException();
            }
        } catch (IOException e) {
            currentState = State.CORRUPTED;
            throw e;
        }
        return bytesConsumed;
    }

    private static byte[] getThreadLocalBuffer(ThreadLocal<byte[]> threadLocal, int requiredSize) {
        byte[] buffer = threadLocal.get();
        if (requiredSize > buffer.length) {
            buffer = new byte[requiredSize];
            threadLocal.set(buffer);
        }
        return buffer;
    }

    /**
     * Returns {@code true} if and only if the end of the compressed stream
     * has been reached.
     */
    public boolean isClosed() {
        return currentState == State.FINISHED;
    }
}
