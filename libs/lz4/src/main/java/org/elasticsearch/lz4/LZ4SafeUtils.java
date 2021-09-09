/*
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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

import static org.elasticsearch.lz4.LZ4Constants.LAST_LITERALS;
import static org.elasticsearch.lz4.LZ4Constants.ML_BITS;
import static org.elasticsearch.lz4.LZ4Constants.ML_MASK;
import static org.elasticsearch.lz4.LZ4Constants.RUN_MASK;

/**
 * This file is forked from https://github.com/lz4/lz4-java. In particular, it forks the following file
 * net.jpountz.lz4.LZ4SafeUtils.
 *
 * It modifies the original implementation to use Java9 array mismatch method and varhandle performance
 * improvements. Comments are included to mark the changes.
 */
enum LZ4SafeUtils {
    ;

    // Added MethodHandles and static initialization
    private static final MethodHandle readIntPlatformNative;
    private static final MethodHandle writeIntPlatformNative;
    private static final MethodHandle readLongPlatformNative;
    private static final MethodHandle writeLongPlatformNative;
    private static final MethodHandle copy4Bytes;
    private static final MethodHandle copy8Bytes;
    private static final MethodHandle commonBytes;

    static {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        boolean exceptionCaught = false;
        MethodHandle tempByteArrayViewVarHandle = null;
        MethodHandle tempToMethodHandle = null;
        Class<?> accessModeClass = null;
        try {
            ClassLoader classLoader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>)
                () -> lookup.lookupClass().getClassLoader());
            Class<?> varHandleClass = Class.forName("java.lang.invoke.VarHandle", true, classLoader);
            accessModeClass = Class.forName("java.lang.invoke.VarHandle$AccessMode", true, classLoader);
            MethodType t = MethodType.methodType(varHandleClass, Class.class, ByteOrder.class);
            tempByteArrayViewVarHandle = AccessController.doPrivileged((PrivilegedExceptionAction<MethodHandle>)
                () -> lookup.findStatic(MethodHandles.class, "byteArrayViewVarHandle", t));
            MethodType toMethodHandleType = MethodType.methodType(MethodHandle.class, accessModeClass);
            tempToMethodHandle = lookup.findVirtual(varHandleClass, "toMethodHandle", toMethodHandleType);
        } catch (Exception ignored) {
            exceptionCaught = true;
        }
        final MethodHandle byteArrayViewVarHandle = tempByteArrayViewVarHandle;
        final MethodHandle toMethodHandle = tempToMethodHandle;
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Object getAccessModeEnum = accessModeClass != null ? Enum.valueOf((Class) accessModeClass, "GET") : null;
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Object setAccessModeEnum = accessModeClass != null ? Enum.valueOf((Class) accessModeClass, "SET") : null;

        boolean initialized = exceptionCaught == false && byteArrayViewVarHandle != null && toMethodHandle != null
            && getAccessModeEnum != null && setAccessModeEnum != null;

        final Object intVarHandle;
        final Object longVarHandle;
        if (initialized) {
            intVarHandle = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    return byteArrayViewVarHandle.invoke(int[].class, Utils.NATIVE_BYTE_ORDER);
                } catch (Throwable ignored) {}
                return null;
            });
            longVarHandle = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    return byteArrayViewVarHandle.invoke(long[].class, Utils.NATIVE_BYTE_ORDER);
                } catch (Throwable ignored) {}
                return null;
            });
        } else {
            intVarHandle = null;
            longVarHandle = null;
        }

        readIntPlatformNative = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (intVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(intVarHandle, getAccessModeEnum);
                } catch (Throwable ignored) {}
            }
            return null;
        });

        writeIntPlatformNative = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (intVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(intVarHandle, setAccessModeEnum);
                } catch (Throwable ignored) {}
            }
            return null;
        });

        readLongPlatformNative = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (longVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(longVarHandle, getAccessModeEnum);
                } catch (Throwable ignored) {}

            }
            return null;
        });

        writeLongPlatformNative = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (longVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(longVarHandle, setAccessModeEnum);
                } catch (Throwable ignored) {}

            }
            return null;
        });

        copy4Bytes = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (readIntPlatformNative != null && writeIntPlatformNative != null) {
                try {
                    final MethodType type = MethodType.methodType(void.class, byte[].class, int.class, byte[].class, int.class);
                    return lookup.findStatic(LZ4SafeUtils.class, "mhCopy4Bytes", type);
                } catch (Throwable ignored) {}
            }
            try {
                final MethodType type = MethodType.methodType(void.class, byte[].class, int.class, byte[].class, int.class);
                return lookup.findStatic(LZ4SafeUtils.class, "legacyCopy4Bytes", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        copy8Bytes = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (readLongPlatformNative != null && writeLongPlatformNative != null) {
                try {
                    final MethodType type = MethodType.methodType(void.class, byte[].class, int.class, byte[].class, int.class);
                    return lookup.findStatic(LZ4SafeUtils.class, "mhCopy8Bytes", type);
                } catch (Throwable ignored) {}
            }
            try {
                final MethodType type = MethodType.methodType(void.class, byte[].class, int.class, byte[].class, int.class);
                return lookup.findStatic(LZ4SafeUtils.class, "legacyCopy8Bytes", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        commonBytes = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            MethodType type = MethodType.methodType(int.class, byte[].class, int.class, int.class, byte[].class, int.class, int.class);
            if (initialized) {
                try {
                    return lookup.findStatic(Arrays.class, "mismatch", type);
                } catch (Throwable ignored) {}
            }
            try {
                return lookup.findStatic(LZ4SafeUtils.class, "legacyCommonBytes", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    static int hash(byte[] buf, int i) {
        return LZ4Utils.hash(SafeUtils.readInt(buf, i));
    }

    static int hash64k(byte[] buf, int i) {
        return LZ4Utils.hash64k(SafeUtils.readInt(buf, i));
    }

    static boolean readIntEquals(byte[] buf, int i, int j) {
        return SafeUtils.readInt(buf, i) == SafeUtils.readInt(buf, j);
    }

    static void safeIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchLen) {
        for (int i = 0; i < matchLen; ++i) {
            dest[dOff + i] = dest[matchOff + i];
        }
    }

    // Modified wildIncrementalCopy to mirror version in LZ4UnsafeUtils
    static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
        if (dOff - matchOff < 4) {
            for (int i = 0; i < 4; ++i) {
                dest[dOff + i] = dest[matchOff + i];
            }
            dOff += 4;
            matchOff += 4;
            int dec = 0;
            assert dOff >= matchOff && dOff - matchOff < 8;
            switch (dOff - matchOff) {
                case 1:
                    matchOff -= 3;
                    break;
                case 2:
                    matchOff -= 2;
                    break;
                case 3:
                    matchOff -= 3;
                    dec = -1;
                    break;
                case 5:
                    dec = 1;
                    break;
                case 6:
                    dec = 2;
                    break;
                case 7:
                    dec = 3;
                    break;
                default:
                    break;
            }

            copy4Bytes(dest, matchOff, dest, dOff);
            dOff += 4;
            matchOff -= dec;
        } else if (dOff - matchOff < LZ4Constants.COPY_LENGTH) {
            copy8Bytes(dest, matchOff, dest, dOff);
            dOff += dOff - matchOff;
        }
        while (dOff < matchCopyEnd) {
            copy8Bytes(dest, matchOff, dest, dOff);
            dOff += 8;
            matchOff += 8;
        }
    }

    // Added legacy method to copy 8 bytes incrementally
    private static void legacyCopy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        for (int i = 0; i < 8; ++i) {
            dest[dOff + i] = src[sOff + i];
        }
    }

    // Added to read long. Having a dedicated private static method, makes the JDK optimization consistent.
    private static long readLong(byte[] bytes, int off) {
        try {
            return (long) readLongPlatformNative.invokeExact(bytes, off);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Added to write long. Having a dedicated private static method, makes the JDK optimization consistent.
    private static void writeLong(byte[] bytes, int off, long value) {
        try {
            writeLongPlatformNative.invokeExact(bytes, off, value);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Added to call MethodHandles
    private static void mhCopy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        writeLong(dest, dOff, readLong(src, sOff));
    }

    // Modified to use MethodHandle
    static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        try {
            copy8Bytes.invokeExact(src, sOff, dest, dOff);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Added legacy method to copy 4 bytes incrementally
    private static void legacyCopy4Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        for (int i = 0; i < 4; ++i) {
            dest[dOff + i] = src[sOff + i];
        }
    }

    // Added to read int. Having a dedicated private static method, makes the JDK optimization consistent.
    private static int readInt(byte[] bytes, int off) {
        try {
            return (int) readIntPlatformNative.invokeExact(bytes, off);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Added to write int. Having a dedicated private static method, makes the JDK optimization consistent.
    private static void writeInt(byte[] bytes, int off, int value) {
        try {
            writeIntPlatformNative.invokeExact(bytes, off, value);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Added to call MethodHandles
    private static void mhCopy4Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        writeInt(dest, dOff, readInt(src, sOff));
    }

    // Added to call method handle
    static void copy4Bytes(byte[] src, int sOff, byte[] dest, int dOff) {
        try {
            copy4Bytes.invokeExact(src, sOff, dest, dOff);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static int legacyCommonBytes(byte[] b, int o1, int ignored1, byte[] ignored2, int o2, int limit) {
        int count = 0;
        while (o2 < limit && b[o1++] == b[o2++]) {
            ++count;
        }
        return count;
    }

    // Modified to use Arrays.mismatch
    static int commonBytes(byte[] b, int o1, int o2, int limit) {
        try {
            int mismatch = (int) commonBytes.invokeExact(b, o1, limit, b, o2, limit);
            return mismatch == -1 ? limit : mismatch;
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
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
            // Modified to catch IndexOutOfBoundsException instead of ArrayIndexOutOfBoundsException.
            // VarHandles throw IndexOutOfBoundsException
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

        if (dOff + runLen + 1 + (runLen + 255 - RUN_MASK) / 255 > destEnd) {
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
