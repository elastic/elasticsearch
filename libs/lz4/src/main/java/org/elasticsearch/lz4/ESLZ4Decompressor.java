/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lz4;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

/*
 * This file is forked from https://github.com/lz4/lz4-java, which is licensed under Apache-2 and Copyright
 * 2020 Adrien Grand and the lz4-java contributors. In particular, it forks the following file
 * net.jpountz.lz4.LZ4JavaSafeFastDecompressor.
 *
 * It modifies the original implementation to use custom LZ4SafeUtils and SafeUtils implementations which
 * include performance improvements.
 */
public class ESLZ4Decompressor extends LZ4FastDecompressor {
    public static final LZ4FastDecompressor INSTANCE = new ESLZ4Decompressor();

    ESLZ4Decompressor() {}

    public int decompress(byte[] src, int srcOff, byte[] dest, int destOff, int destLen) {
        SafeUtils.checkRange(src, srcOff);
        SafeUtils.checkRange(dest, destOff, destLen);
        if (destLen == 0) {
            if (SafeUtils.readByte(src, srcOff) != 0) {
                throw new LZ4Exception("Malformed input at " + srcOff);
            } else {
                return 1;
            }
        } else {
            int destEnd = destOff + destLen;
            int sOff = srcOff;
            int dOff = destOff;

            while (true) {
                int token = SafeUtils.readByte(src, sOff) & 255;
                ++sOff;
                int literalLen = token >>> 4;
                if (literalLen == 15) {
                    byte len;
                    for (boolean var11 = true; (len = SafeUtils.readByte(src, sOff++)) == -1; literalLen += 255) {
                    }

                    literalLen += len & 255;
                }

                int literalCopyEnd = dOff + literalLen;
                if (literalCopyEnd > destEnd - 8) {
                    if (literalCopyEnd != destEnd) {
                        throw new LZ4Exception("Malformed input at " + sOff);
                    } else {
                        LZ4SafeUtils.safeArraycopy(src, sOff, dest, dOff, literalLen);
                        sOff += literalLen;
                        return sOff - srcOff;
                    }
                }

                LZ4SafeUtils.wildArraycopy(src, sOff, dest, dOff, literalLen);
                sOff += literalLen;
                int matchDec = SafeUtils.readShortLE(src, sOff);
                sOff += 2;
                int matchOff = literalCopyEnd - matchDec;
                if (matchOff < destOff) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                }

                int matchLen = token & 15;
                if (matchLen == 15) {
                    byte len;
                    for (boolean var15 = true; (len = SafeUtils.readByte(src, sOff++)) == -1; matchLen += 255) {
                    }

                    matchLen += len & 255;
                }

                matchLen += 4;
                int matchCopyEnd = literalCopyEnd + matchLen;
                if (matchCopyEnd > destEnd - 8) {
                    if (matchCopyEnd > destEnd) {
                        throw new LZ4Exception("Malformed input at " + sOff);
                    }

                    LZ4SafeUtils.safeIncrementalCopy(dest, matchOff, literalCopyEnd, matchLen);
                } else {
                    LZ4SafeUtils.wildIncrementalCopy(dest, matchOff, literalCopyEnd, matchCopyEnd);
                }

                dOff = matchCopyEnd;
            }
        }
    }

    public int decompress(ByteBuffer src, int srcOff, ByteBuffer dest, int destOff, int destLen) {
        if (src.hasArray() && dest.hasArray()) {
            return this.decompress(src.array(), srcOff + src.arrayOffset(), dest.array(), destOff + dest.arrayOffset(), destLen);
        } else {
            throw new AssertionError("Do not support decompression on direct buffers");
        }
    }
}
