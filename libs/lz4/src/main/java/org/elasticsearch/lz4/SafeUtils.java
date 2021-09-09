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

import net.jpountz.util.Utils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * This file is forked from https://github.com/lz4/lz4-java. In particular, it forks the following file
 * net.jpountz.lz4.SafeUtils.
 *
 * It modifies the original implementation to use Java9 varhandle performance improvements. Comments
 * are included to mark the changes.
 */
public enum SafeUtils {
    ;

    // Added MethodHandles and static initialization
    private static final MethodHandle readShortLittleEndian;
    private static final MethodHandle writeShortLittleEndian;
    private static final MethodHandle readIntPlatformNative;

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

        Object shortLEVarHandle;
        Object intVarHandle;
        if (initialized) {
            shortLEVarHandle = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    return byteArrayViewVarHandle.invoke(short[].class, ByteOrder.LITTLE_ENDIAN);
                } catch (Throwable ignored) {}
                return null;
            });
            intVarHandle = AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                try {
                    return byteArrayViewVarHandle.invoke(int[].class, Utils.NATIVE_BYTE_ORDER);
                } catch (Throwable ignored) {}
                return null;
            });
        } else {
            shortLEVarHandle = null;
            intVarHandle = null;
        }

        readShortLittleEndian = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (shortLEVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(shortLEVarHandle, getAccessModeEnum);
                } catch (Throwable ignored) {}
            }
            try {
                final MethodType type = MethodType.methodType(short.class, byte[].class, int.class);
                return lookup.findStatic(SafeUtils.class, "legacyReadShortLE", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        writeShortLittleEndian = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (shortLEVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(shortLEVarHandle, setAccessModeEnum);
                } catch (Throwable ignored) {}
            }
            try {
                final MethodType type = MethodType.methodType(void.class, byte[].class, int.class, short.class);
                return lookup.findStatic(SafeUtils.class, "legacyWriteShortLE", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        readIntPlatformNative = AccessController.doPrivileged((PrivilegedAction<MethodHandle>) () -> {
            if (intVarHandle != null) {
                try {
                    return (MethodHandle) toMethodHandle.invoke(intVarHandle, getAccessModeEnum);
                } catch (Throwable ignored) {}

            }
            try {
                final MethodType type = MethodType.methodType(int.class, byte[].class, int.class);
                return lookup.findStatic(SafeUtils.class, "legacyReadInt", type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }


    public static void checkRange(byte[] buf, int off) {
        if (off < 0 || off >= buf.length) {
            throw new ArrayIndexOutOfBoundsException(off);
        }
    }

    public static void checkRange(byte[] buf, int off, int len) {
        checkLength(len);
        if (len > 0) {
            checkRange(buf, off);
            checkRange(buf, off + len - 1);
        }
    }

    public static void checkLength(int len) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
    }

    public static byte readByte(byte[] buf, int i) {
        return buf[i];
    }

    private static int readIntBE(byte[] buf, int i) {
        return (buf[i] & 255) << 24 | (buf[i + 1] & 255) << 16 | (buf[i + 2] & 255) << 8 | buf[i + 3] & 255;
    }

    private static int readIntLE(byte[] buf, int i) {
        return buf[i] & 255 | (buf[i + 1] & 255) << 8 | (buf[i + 2] & 255) << 16 | (buf[i + 3] & 255) << 24;
    }

    // Modified to rename to legacy read int method
    private static int legacyReadInt(byte[] buf, int i) {
        return Utils.NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN ? readIntBE(buf, i) : readIntLE(buf, i);
    }

    // Modified to use writeShortLE
    public static int readInt(byte[] buf, int i) {
        try {
            return (int) readIntPlatformNative.invokeExact(buf, i);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // Unused in forked instance, no need to optimize
    public static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
            | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
    }

    // Modified to rename to legacy write short method
    private static void legacyWriteShortLE(byte[] buf, int off, short v) {
        buf[off++] = (byte) v;
        buf[off++] = (byte) (v >>> 8);
    }

    // Modified to use MethodHandle
    public static void writeShortLE(byte[] buf, int off, int v) {
        try {
            writeShortLittleEndian.invokeExact(buf, off, (short) v);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeInt(int[] buf, int off, int v) {
        buf[off] = v;
    }

    public static int readInt(int[] buf, int off) {
        return buf[off];
    }

    public static void writeByte(byte[] dest, int off, int i) {
        dest[off] = (byte) i;
    }

    public static void writeShort(short[] buf, int off, int v) {
        buf[off] = (short) v;
    }

    // Modified to rename to legacy read short method
    private static short legacyReadShortLE(byte[] buf, int i) {
        return (short) (buf[i] & 255 | (buf[i + 1] & 255) << 8);
    }

    // Modified to use MethodHandle
    public static int readShortLE(byte[] buf, int i) {
        try {
            return Short.toUnsignedInt((short) readShortLittleEndian.invokeExact(buf, i));
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static int readShort(short[] buf, int off) {
        return buf[off] & 0xFFFF;
    }
}
