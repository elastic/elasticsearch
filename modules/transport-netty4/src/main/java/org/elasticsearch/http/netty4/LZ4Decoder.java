/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.transport.Compression;

import java.util.List;
import java.util.Locale;

public class LZ4Decoder extends ByteToMessageDecoder {
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

    static final int MAX_BLOCK_SIZE = 1 << COMPRESSION_LEVEL_BASE + 0x0F;   // 32 M

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

    public LZ4Decoder() {
        this.decompressor = Compression.Scheme.lz4Decompressor();
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            switch (currentState) {
                case INIT_BLOCK:
                    if (in.readableBytes() < HEADER_LENGTH) {
                        return;
                    }

                {
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
                            String.format(Locale.ROOT, "invalid compressedLength: %d (expected: 0-%d)", compressedLength, MAX_BLOCK_SIZE)
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
                    if (in.readableBytes() < this.compressedLength) {
                        break;
                    }
                    ByteBuf decompressed = ctx.alloc().heapBuffer(decompressedLength);
                    try {
                        switch (blockType) {
                            case BLOCK_TYPE_NON_COMPRESSED:
                                in.readBytes(decompressed, decompressedLength);
                                break;
                            case BLOCK_TYPE_COMPRESSED:
                                final byte[] compressed;
                                final int compressedOffset;
                                if (in.hasArray()) {
                                    compressed = in.array();
                                    compressedOffset = in.arrayOffset() + in.readerIndex();
                                    in.skipBytes(this.compressedLength);
                                } else {
                                    compressed = getThreadLocalBuffer(COMPRESSED, this.compressedLength);
                                    compressedOffset = 0;
                                    in.readBytes(compressed, 0, this.compressedLength);
                                }
                                byte[] decompressedArray = decompressed.array();
                                int writerIndex = decompressed.writerIndex();
                                int outIndex = decompressed.arrayOffset() + writerIndex;
                                decompressor.decompress(compressed, compressedOffset, decompressedArray, outIndex, decompressedLength);
                                decompressed.writerIndex(writerIndex + decompressedLength);
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
                        currentState = State.INIT_BLOCK;
                    } catch (LZ4Exception e) {
                        throw new IllegalStateException(e);
                    } finally {
                        if (decompressed.isReadable()) {
                            out.add(decompressed);
                        } else {
                            decompressed.release();
                        }
                    }
                    break;
                case FINISHED:
                    break;
                case CORRUPTED:
                    throw new IllegalStateException("LZ4 stream corrupted.");
                default:
                    throw new IllegalStateException();
            }
        } catch (Exception e) {
            currentState = State.CORRUPTED;
            throw e;
        }
    }

    private byte[] getThreadLocalBuffer(ThreadLocal<byte[]> threadLocal, int requiredSize) {
        byte[] buffer = threadLocal.get();
        if (requiredSize > buffer.length) {
            buffer = new byte[requiredSize];
            threadLocal.set(buffer);
        }
        return buffer;
    }

}
