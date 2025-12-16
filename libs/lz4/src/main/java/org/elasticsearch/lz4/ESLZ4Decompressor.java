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

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.ByteBufferUtils;
import net.jpountz.util.SafeUtils;

import java.nio.ByteBuffer;

import static org.elasticsearch.lz4.LZ4Constants.COPY_LENGTH;
import static org.elasticsearch.lz4.LZ4Constants.ML_BITS;
import static org.elasticsearch.lz4.LZ4Constants.RUN_MASK;
import static org.elasticsearch.lz4.LZ4Utils.notEnoughSpace;


/**
 * This file is forked from https://github.com/yawkat/lz4-java. In particular, it forks the following file
 * net.jpountz.lz4.LZ4JavaSafeFastDecompressor.
 *
 * It modifies the original implementation to use custom LZ4SafeUtils and SafeUtils implementations which
 * include performance improvements.
 */
public class ESLZ4Decompressor extends LZ4FastDecompressor {
    public static final LZ4FastDecompressor INSTANCE = new ESLZ4Decompressor();

    ESLZ4Decompressor() {}

    @Override
    public int decompress(byte[] src, final int srcOff, byte[] dest, final int destOff, int destLen) {

        final int srcEnd = src.length;

        return decompress(src, srcOff, srcEnd - srcOff, dest, destOff, destLen);
    }

    private int decompress(byte[] src, final int srcOff, final int srcLen, byte[] dest, final int destOff, int destLen) {
        SafeUtils.checkRange(src, srcOff, srcLen);
        SafeUtils.checkRange(dest, destOff, destLen);

        if (destLen == 0) {
            // Allow `srcLen > 1` despite just one byte being consumed since this 'fast' decompressor does not have to fully consume the src
            if (srcLen < 1 || SafeUtils.readByte(src, srcOff) != 0) {
                throw new LZ4Exception("Malformed input at " + srcOff);
            }
            return 1;
        }

        final int srcEnd = srcOff + srcLen;
        final int destEnd = destOff + destLen;

        int sOff = srcOff;
        int dOff = destOff;

        while (true) {
            if (sOff >= srcEnd) {
                throw new LZ4Exception("Malformed input at " + sOff);
            }
            final int token = SafeUtils.readByte(src, sOff) & 0xFF;
            ++sOff;

            // literals
            int literalLen = token >>> ML_BITS;
            if (literalLen == RUN_MASK) {
                byte len = (byte) 0xFF;
                while (sOff < srcEnd && (len = SafeUtils.readByte(src, sOff++)) == (byte) 0xFF) {
                    literalLen += 0xFF;
                    if (literalLen < 0) {
                        throw new LZ4Exception("Too large literalLen");
                    }
                }
                literalLen += len & 0xFF;
            }

            final int literalCopyEnd = dOff + literalLen;
            // Check for overflow
            if (literalCopyEnd < dOff) {
                throw new LZ4Exception("Too large literalLen");
            }

            if (notEnoughSpace(destEnd - literalCopyEnd, COPY_LENGTH)
                || notEnoughSpace(srcEnd - sOff, COPY_LENGTH + literalLen)) {

                if (literalCopyEnd != destEnd) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                } else if (notEnoughSpace(srcEnd - sOff, literalLen)) {
                    throw new LZ4Exception("Malformed input at " + sOff);

                } else {
                    LZ4SafeUtils.safeArraycopy(src, sOff, dest, dOff, literalLen);
                    sOff += literalLen;
                    dOff = literalCopyEnd;
                    break; // EOF
                }
            }

            LZ4SafeUtils.wildArraycopy(src, sOff, dest, dOff, literalLen);
            sOff += literalLen;
            dOff = literalCopyEnd;

            // matchs
            final int matchDec = SafeUtils.readShortLE(src, sOff);
            sOff += 2;
            int matchOff = dOff - matchDec;

            if (matchOff < destOff) {
                throw new LZ4Exception("Malformed input at " + sOff);
            }

            int matchLen = token & LZ4Constants.ML_MASK;
            if (matchLen == LZ4Constants.ML_MASK) {
                byte len = (byte) 0xFF;
                while (sOff < srcEnd && (len = SafeUtils.readByte(src, sOff++)) == (byte) 0xFF) {
                    matchLen += 0xFF;
                    if (matchLen < 0) {
                        throw new LZ4Exception("Too large matchLen");
                    }
                }
                matchLen += len & 0xFF;
            }
            matchLen += LZ4Constants.MIN_MATCH;

            final int matchCopyEnd = dOff + matchLen;
            // Check for overflow
            if (matchCopyEnd < dOff) {
                throw new LZ4Exception("Too large matchLen");
            }

            if (matchDec == 0) {
                if (matchCopyEnd > destEnd) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                }
                // With matchDec == 0, matchOff == dOff, so we'd copy in place. Zero the data instead. (CVE-2025-66566)
                assert matchOff == dOff; // should always hold, but this extra check will trigger during fuzzing if my logic is wrong
                LZ4Utils.zero(dest, dOff, matchCopyEnd);
            } else if (notEnoughSpace(destEnd - matchCopyEnd, COPY_LENGTH)) {
                if (matchCopyEnd > destEnd) {
                    throw new LZ4Exception("Malformed input at " + sOff);
                }
                LZ4SafeUtils.safeIncrementalCopy(dest, matchOff, dOff, matchLen);
            } else {
                LZ4SafeUtils.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
            }
            dOff = matchCopyEnd;
        }

        return sOff - srcOff;

    }

    @Override
    public int decompress(ByteBuffer src, final int srcOff, ByteBuffer dest, final int destOff, int destLen) {

        final int srcEnd = src.capacity();

        return decompress(src, srcOff, srcEnd - srcOff, dest, destOff, destLen);
    }

    private int decompress(ByteBuffer src, final int srcOff, final int srcLen, ByteBuffer dest, final int destOff, int destLen) {
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        ByteBufferUtils.checkRange(dest, destOff, destLen);

        if (src.hasArray() && dest.hasArray()) {
            return decompress(src.array(), srcOff + src.arrayOffset(), srcLen, dest.array(), destOff + dest.arrayOffset(), destLen);
        } else {
            throw new AssertionError("Do not support decompression on direct buffers");
        }
    }
}
