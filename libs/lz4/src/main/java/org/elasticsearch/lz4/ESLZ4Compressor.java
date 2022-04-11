/*
 * @notice
 *
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.elasticsearch.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This file is forked from https://github.com/lz4/lz4-java. In particular, it forks the following file
 * net.jpountz.lz4.LZ4JavaSafeCompressor.
 *
 * It modifies the original implementation to use custom LZ4SafeUtils and SafeUtils implementations which
 * include performance improvements. Additionally, instead of allocating a new hashtable for each compress
 * call, it reuses thread-local hashtables. Comments are included to mark the changes.
 */
public class ESLZ4Compressor extends LZ4Compressor {

    // Modified to add thread-local hash tables
    private static final ThreadLocal<short[]> sixtyFourKBHashTable = ThreadLocal.withInitial(() -> new short[8192]);
    private static final ThreadLocal<int[]> biggerHashTable = ThreadLocal.withInitial(() -> new int[4096]);

    public static final LZ4Compressor INSTANCE = new ESLZ4Compressor();

    ESLZ4Compressor() {}

    static int compress64k(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int destEnd) {
        int srcEnd = srcOff + srcLen;
        int srcLimit = srcEnd - 5;
        int mflimit = srcEnd - 12;
        int dOff = destOff;
        int anchor = srcOff;
        if (srcLen >= 13) {
            // Modified to use thread-local hash table
            short[] hashTable = sixtyFourKBHashTable.get();
            Arrays.fill(hashTable, (short) 0);
            int sOff = srcOff + 1;

            label53: while (true) {
                int forwardOff = sOff;
                int step = 1;
                int var16 = 1 << LZ4Constants.SKIP_STRENGTH;

                int ref;
                int excess;
                do {
                    sOff = forwardOff;
                    forwardOff += step;
                    step = var16++ >>> LZ4Constants.SKIP_STRENGTH;
                    if (forwardOff > mflimit) {
                        break label53;
                    }

                    excess = LZ4Utils.hash64k(SafeUtils.readInt(src, sOff));
                    ref = srcOff + SafeUtils.readShort(hashTable, excess);
                    SafeUtils.writeShort(hashTable, excess, sOff - srcOff);
                    // Modified to use explicit == false
                } while (LZ4SafeUtils.readIntEquals(src, ref, sOff) == false);

                excess = LZ4SafeUtils.commonBytesBackward(src, ref, sOff, srcOff, anchor);
                sOff -= excess;
                ref -= excess;
                int runLen = sOff - anchor;
                int tokenOff = dOff++;
                if (dOff + runLen + 8 + (runLen >>> 8) > destEnd) {
                    throw new LZ4Exception("maxDestLen is too small");
                }

                if (runLen >= 15) {
                    SafeUtils.writeByte(dest, tokenOff, 240);
                    dOff = LZ4SafeUtils.writeLen(runLen - 15, dest, dOff);
                } else {
                    SafeUtils.writeByte(dest, tokenOff, runLen << 4);
                }

                LZ4SafeUtils.wildArraycopy(src, anchor, dest, dOff, runLen);
                dOff += runLen;

                while (true) {
                    SafeUtils.writeShortLE(dest, dOff, (short) (sOff - ref));
                    dOff += 2;
                    sOff += 4;
                    ref += 4;
                    int matchLen = LZ4SafeUtils.commonBytes(src, ref, sOff, srcLimit);
                    if (dOff + 6 + (matchLen >>> 8) > destEnd) {
                        throw new LZ4Exception("maxDestLen is too small");
                    }

                    sOff += matchLen;
                    if (matchLen >= 15) {
                        SafeUtils.writeByte(dest, tokenOff, SafeUtils.readByte(dest, tokenOff) | 15);
                        dOff = LZ4SafeUtils.writeLen(matchLen - 15, dest, dOff);
                    } else {
                        SafeUtils.writeByte(dest, tokenOff, SafeUtils.readByte(dest, tokenOff) | matchLen);
                    }

                    if (sOff > mflimit) {
                        anchor = sOff;
                        break label53;
                    }

                    SafeUtils.writeShort(hashTable, LZ4Utils.hash64k(SafeUtils.readInt(src, sOff - 2)), sOff - 2 - srcOff);
                    int h = LZ4Utils.hash64k(SafeUtils.readInt(src, sOff));
                    ref = srcOff + SafeUtils.readShort(hashTable, h);
                    SafeUtils.writeShort(hashTable, h, sOff - srcOff);
                    // Modified to use explicit == false
                    if (LZ4SafeUtils.readIntEquals(src, sOff, ref) == false) {
                        anchor = sOff++;
                        break;
                    }

                    tokenOff = dOff++;
                    SafeUtils.writeByte(dest, tokenOff, 0);
                }
            }
        }

        dOff = LZ4SafeUtils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
        return dOff - destOff;
    }

    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
        SafeUtils.checkRange(src, srcOff, srcLen);
        SafeUtils.checkRange(dest, destOff, maxDestLen);
        int destEnd = destOff + maxDestLen;
        if (srcLen < 65547) {
            return compress64k(src, srcOff, srcLen, dest, destOff, destEnd);
        } else {
            int srcEnd = srcOff + srcLen;
            int srcLimit = srcEnd - 5;
            int mflimit = srcEnd - 12;
            int dOff = destOff;
            int sOff = srcOff + 1;
            int anchor = srcOff;
            // Modified to use thread-local hash table
            int[] hashTable = biggerHashTable.get();
            Arrays.fill(hashTable, srcOff);

            label63: while (true) {
                int forwardOff = sOff;
                int step = 1;
                int var18 = 1 << LZ4Constants.SKIP_STRENGTH;

                while (true) {
                    sOff = forwardOff;
                    forwardOff += step;
                    step = var18++ >>> LZ4Constants.SKIP_STRENGTH;
                    if (forwardOff <= mflimit) {
                        int excess = LZ4Utils.hash(SafeUtils.readInt(src, sOff));
                        int ref = SafeUtils.readInt(hashTable, excess);
                        int back = sOff - ref;
                        SafeUtils.writeInt(hashTable, excess, sOff);
                        // Modified to use explicit == false
                        if (back >= 65536 || LZ4SafeUtils.readIntEquals(src, ref, sOff) == false) {
                            continue;
                        }

                        excess = LZ4SafeUtils.commonBytesBackward(src, ref, sOff, srcOff, anchor);
                        sOff -= excess;
                        ref -= excess;
                        int runLen = sOff - anchor;
                        int tokenOff = dOff++;
                        if (dOff + runLen + 8 + (runLen >>> 8) > destEnd) {
                            throw new LZ4Exception("maxDestLen is too small");
                        }

                        if (runLen >= 15) {
                            SafeUtils.writeByte(dest, tokenOff, 240);
                            dOff = LZ4SafeUtils.writeLen(runLen - 15, dest, dOff);
                        } else {
                            SafeUtils.writeByte(dest, tokenOff, runLen << 4);
                        }

                        LZ4SafeUtils.wildArraycopy(src, anchor, dest, dOff, runLen);
                        dOff += runLen;

                        while (true) {
                            SafeUtils.writeShortLE(dest, dOff, back);
                            dOff += 2;
                            sOff += 4;
                            int matchLen = LZ4SafeUtils.commonBytes(src, ref + 4, sOff, srcLimit);
                            if (dOff + 6 + (matchLen >>> 8) > destEnd) {
                                throw new LZ4Exception("maxDestLen is too small");
                            }

                            sOff += matchLen;
                            if (matchLen >= 15) {
                                SafeUtils.writeByte(dest, tokenOff, SafeUtils.readByte(dest, tokenOff) | 15);
                                dOff = LZ4SafeUtils.writeLen(matchLen - 15, dest, dOff);
                            } else {
                                SafeUtils.writeByte(dest, tokenOff, SafeUtils.readByte(dest, tokenOff) | matchLen);
                            }

                            if (sOff > mflimit) {
                                anchor = sOff;
                                break;
                            }

                            SafeUtils.writeInt(hashTable, LZ4Utils.hash(SafeUtils.readInt(src, sOff - 2)), sOff - 2);
                            int h = LZ4Utils.hash(SafeUtils.readInt(src, sOff));
                            ref = SafeUtils.readInt(hashTable, h);
                            SafeUtils.writeInt(hashTable, h, sOff);
                            back = sOff - ref;
                            // Modified to use explicit == false
                            if (back >= 65536 || LZ4SafeUtils.readIntEquals(src, ref, sOff) == false) {
                                anchor = sOff++;
                                continue label63;
                            }

                            tokenOff = dOff++;
                            SafeUtils.writeByte(dest, tokenOff, 0);
                        }
                    }

                    dOff = LZ4SafeUtils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
                    return dOff - destOff;
                }
            }
        }
    }

    @Override
    public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
        if (src.hasArray() && dest.hasArray()) {
            return this.compress(src.array(), srcOff + src.arrayOffset(), srcLen, dest.array(), destOff + dest.arrayOffset(), maxDestLen);
        } else {
            throw new AssertionError("Do not support compression on direct buffers");
        }
    }
}
