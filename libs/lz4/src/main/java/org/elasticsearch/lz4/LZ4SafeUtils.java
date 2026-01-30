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
import net.jpountz.util.Utils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static org.elasticsearch.lz4.LZ4Constants.LAST_LITERALS;
import static org.elasticsearch.lz4.LZ4Constants.ML_BITS;
import static org.elasticsearch.lz4.LZ4Constants.ML_MASK;
import static org.elasticsearch.lz4.LZ4Constants.RUN_MASK;
import static org.elasticsearch.lz4.LZ4Utils.lengthOfEncodedInteger;
import static org.elasticsearch.lz4.LZ4Utils.notEnoughSpace;

/**
 * This file is a vendored version of {@code net.jpountz.lz4.LZ4SafeUtils} from
 * <a href="https://github.com/yawkat/lz4-java">yawkat/lz4-java</a>.
 * <p>
 * Differences from the original are annotated with [ES change from upstream]
 *
 * @see <a href="https://github.com/yawkat/lz4-java/blob/v1.10.1/src/java/net/jpountz/lz4/LZ4SafeUtils.java">LZ4SafeUtils.java</a>
 */
enum LZ4SafeUtils {
    ;

    // [ES change from upstream]: define VarHandles allowing us to do operations without direct byte[] operations
    private static final VarHandle intPlatformNative = MethodHandles.byteArrayViewVarHandle(int[].class, Utils.NATIVE_BYTE_ORDER);
    private static final VarHandle longPlatformNative = MethodHandles.byteArrayViewVarHandle(long[].class, Utils.NATIVE_BYTE_ORDER);

    static int hash(byte[] buf, int i) {
        return LZ4Utils.hash(SafeUtils.readInt(buf, i));
    }

    static int hash64k(byte[] buf, int i) {
        return LZ4Utils.hash64k(SafeUtils.readInt(buf, i));
    }

    static boolean readIntEquals(byte[] buf, int i, int j) {
        // [ES change from upstream]: use SafeUtils.readInt instead of comparing byte-by-byte
        return SafeUtils.readInt(buf, i) == SafeUtils.readInt(buf, j);
    }

    static void safeIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchLen) {
        for (int i = 0; i < matchLen; ++i) {
            dest[dOff + i] = dest[matchOff + i];
        }
    }

    static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
        // [ES change from upstream]: use the implementation from LZ4UnsafeUtils#wildIncrementalCopy
        // see https://github.com/yawkat/lz4-java/blob/v1.10.1/src/java-unsafe/net/jpountz/lz4/LZ4UnsafeUtils.java#L55-L99
        // with further modifications as noted with [ES change from upstream] comments
        if (dOff - matchOff < 4) {
            for (int i = 0; i < 4; ++i) {
                // [ES change from upstream]: just copy the byte directly, don't use readByte and writeByte
                dest[dOff + i] = dest[matchOff + i];
            }
            dOff += 4;
            matchOff += 4;
            int dec = 0;
            assert dOff >= matchOff && dOff - matchOff < 8;
            // [ES change from upstream]: use enhanced switch
            switch (dOff - matchOff) {
                case 1 -> matchOff -= 3;
                case 2 -> matchOff -= 2;
                case 3 -> {
                    matchOff -= 3;
                    dec = -1;
                }
                case 5 -> dec = 1;
                case 6 -> dec = 2;
                case 7 -> dec = 3;
            }

            // [ES change from upstream]: use copy4Bytes instead of readInt and writeInt
            copy4Bytes(dest, matchOff, dest, dOff);
            dOff += 4;
            matchOff -= dec;
        } else if (dOff - matchOff < LZ4Constants.COPY_LENGTH) {
            // [ES change from upstream]: use copy8Bytes instead of readLong and writeLong
            copy8Bytes(dest, matchOff, dest, dOff);
            dOff += dOff - matchOff;
        }
        while (dOff < matchCopyEnd) {
            // [ES change from upstream]: use copy8Bytes instead of readLong and writeLong
            copy8Bytes(dest, matchOff, dest, dOff);
            dOff += 8;
            matchOff += 8;
        }
    }

    static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        // [ES change from upstream]: use VarHandles instead of a byte-by-byte loop
        longPlatformNative.set(dest, dOff, (long) longPlatformNative.get(src, sOff));
    }

    // [ES change from upstream]: add this method to copy a single int (using VarHandles similarly to copy8Bytes)
    static void copy4Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        intPlatformNative.set(dest, dOff, (int) intPlatformNative.get(src, sOff));
    }

    static int commonBytes(byte[] b, int o1, int o2, int limit) {
        // [ES change from upstream]: use Arrays#mismatch instead of a byte-by-byte loop
        int mismatch = Arrays.mismatch(b, o1, limit, b, o2, limit);
        return mismatch == -1 ? limit : mismatch;
    }

    static int commonBytesBackward(byte[] b, int o1, int o2, int l1, int l2) {
        int count = 0;
        while (o1 > l1 && o2 > l2 && b[--o1] == b[--o2]) {
            ++count;
        }
        return count;
    }

    static void safeArraycopy(byte[] src, int sOff, byte[] dest, int dOff, int len) {
        System.arraycopy(src, sOff, dest, dOff, len);
    }

    static void wildArraycopy(byte[] src, int sOff, byte[] dest, int dOff, int len) {
        try {
            for (int i = 0; i < len; i += 8) {
                copy8Bytes(src, sOff + i, dest, dOff + i);
            }
            // [ES change from upstream]: Varhandles throw an IndexOutOfBoundsException instead of ArrayIndexOutOfBoundsException so we
            // change the type of exception caught here.
        } catch (IndexOutOfBoundsException e) {
            throw new LZ4Exception("Malformed input at offset " + sOff);
        }
    }

    static int encodeSequence(byte[] src, int anchor, int matchOff, int matchRef, int matchLen, byte[] dest, int dOff, int destEnd) {
        final int runLen = matchOff - anchor;
        final int tokenOff = dOff++;

        if (dOff + runLen + (2 + 1 + LAST_LITERALS) + (runLen >>> 8) > destEnd) {
            throw new LZ4Exception("maxDestLen is too small");
        }

        int token;
        if (runLen >= RUN_MASK) {
            token = (byte) (RUN_MASK << ML_BITS);
            dOff = writeLen(runLen - RUN_MASK, dest, dOff);
        } else {
            token = runLen << ML_BITS;
        }

        // copy literals
        wildArraycopy(src, anchor, dest, dOff, runLen);
        dOff += runLen;

        // encode offset
        final int matchDec = matchOff - matchRef;
        dest[dOff++] = (byte) matchDec;
        dest[dOff++] = (byte) (matchDec >>> 8);

        // encode match len
        matchLen -= 4;
        if (dOff + (1 + LAST_LITERALS) + (matchLen >>> 8) > destEnd) {
            throw new LZ4Exception("maxDestLen is too small");
        }
        if (matchLen >= ML_MASK) {
            token |= ML_MASK;
            dOff = writeLen(matchLen - RUN_MASK, dest, dOff);
        } else {
            token |= matchLen;
        }

        dest[tokenOff] = (byte) token;

        return dOff;
    }

    static int lastLiterals(byte[] src, int sOff, int srcLen, byte[] dest, int dOff, int destEnd) {
        final int runLen = srcLen;

        if (notEnoughSpace(destEnd - dOff, 1 + lengthOfEncodedInteger(runLen) + runLen)) {
            throw new LZ4Exception();
        }

        if (runLen >= RUN_MASK) {
            dest[dOff++] = (byte) (RUN_MASK << ML_BITS);
            dOff = writeLen(runLen - RUN_MASK, dest, dOff);
        } else {
            dest[dOff++] = (byte) (runLen << ML_BITS);
        }
        // copy literals
        System.arraycopy(src, sOff, dest, dOff, runLen);
        dOff += runLen;

        return dOff;
    }

    static int writeLen(int len, byte[] dest, int dOff) {
        while (len >= 0xFF) {
            dest[dOff++] = (byte) 0xFF;
            len -= 0xFF;
        }
        dest[dOff++] = (byte) len;
        return dOff;
    }

    static class Match {
        int start, ref, len;

        void fix(int correction) {
            start += correction;
            ref += correction;
            len -= correction;
        }

        int end() {
            return start + len;
        }
    }

    static void copyTo(LZ4SafeUtils.Match m1, LZ4SafeUtils.Match m2) {
        m2.len = m1.len;
        m2.start = m1.start;
        m2.ref = m1.ref;
    }
}
