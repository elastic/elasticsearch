/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** Little-endian {@code long} access over byte arrays, used by {@link DocValuesForUtil}. */
final class ByteUtils {

    private static final VarHandle LITTLE_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private ByteUtils() {}

    static void writeLongLE(long value, byte[] array, int offset) {
        LITTLE_ENDIAN_LONG.set(array, offset, value);
    }

    static long readLongLE(byte[] array, int offset) {
        return (long) LITTLE_ENDIAN_LONG.get(array, offset);
    }
}
