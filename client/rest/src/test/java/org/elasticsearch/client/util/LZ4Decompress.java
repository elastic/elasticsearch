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

package org.elasticsearch.client.util;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.OutputStream;
import java.util.Locale;

public class LZ4Decompress {

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

    public LZ4Decompress() {
        this.decompressor = LZ4Factory.safeInstance().fastDecompressor();
    }

    public static long readLong(byte[] bytes, int index) {
        return (((long) readInt(bytes, index)) << 32) | (readInt(bytes, index + Integer.BYTES) & 0xFFFFFFFFL);
    }

    public static short readShort(byte[] bytes, int index) {
        return (short) (((bytes[index] & 0xFF) << 8) | (bytes[index + 1] & 0xFF));
    }

    public static int readInt(byte[] bytes, int index) {
        return ((bytes[index] & 0xFF) << 24) | ((bytes[index + 1] & 0xFF) << 16) | ((bytes[index + 2] & 0xFF) << 8) | (bytes[index + 3]
            & 0xFF);
    }

    public void decode(byte[] in, OutputStream outputStream) throws Exception {
        int currentIndex = 0;
        while (true) {
            try {
                switch (currentState) {
                    case INIT_BLOCK:
                        if (in.length - currentIndex < HEADER_LENGTH) {
                            return;
                        }

                    {
                        final long magic = readLong(in, currentIndex);
                        currentIndex += Long.BYTES;
                        if (magic != MAGIC_NUMBER) {
                            throw new IllegalStateException("unexpected block identifier");
                        }

                        final int token = in[currentIndex];
                        currentIndex++;

                        final int compressionLevel = (token & 0x0F) + COMPRESSION_LEVEL_BASE;
                        int blockType = token & 0xF0;

                        int compressedLength = Integer.reverseBytes(readInt(in, currentIndex));
                        currentIndex += Integer.BYTES;
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

                        int decompressedLength = Integer.reverseBytes(readInt(in, currentIndex));
                        currentIndex += Integer.BYTES;
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
                        readInt(in, currentIndex);
                        currentIndex += Integer.BYTES;

                        if (decompressedLength == 0) {
                            currentState = State.FINISHED;
                            decompressor = null;
                            return;
                        }

                        this.blockType = blockType;
                        this.compressedLength = compressedLength;
                        this.decompressedLength = decompressedLength;
                    }

                        currentState = State.DECOMPRESS_DATA;
                        break;
                    case DECOMPRESS_DATA:
                        if (in.length - currentIndex < HEADER_LENGTH) {
                            return;
                        }
                        byte[] decompressed = new byte[decompressedLength];
                        try {
                            switch (blockType) {
                                case BLOCK_TYPE_NON_COMPRESSED:
                                    System.arraycopy(in, currentIndex, decompressed, 0, decompressedLength);
                                    currentIndex += decompressedLength;
                                    break;
                                case BLOCK_TYPE_COMPRESSED:
                                    decompressor.decompress(in, currentIndex, decompressed, 0, decompressedLength);
                                    currentIndex += compressedLength;
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
                            outputStream.write(decompressed);
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
            } catch (Exception e) {
                currentState = State.CORRUPTED;
                throw e;
            }
        }
    }

}
