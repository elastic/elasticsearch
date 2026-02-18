/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.service.windows;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Compatibility utilities for Panama FFI across JDK versions.
 *
 * <p>The base implementation targets JDK 21 preview APIs. The {@code main22} source set
 * overrides methods where the API changed between JDK 21 (preview) and JDK 22 (final).
 */
class PanamaUtil {

    private static final Linker LINKER = Linker.nativeLinker();

    static MethodHandle downcallHandle(MemorySegment address, FunctionDescriptor descriptor, Linker.Option... options) {
        return LINKER.downcallHandle(address, descriptor, options);
    }

    static MemorySegment findFunction(SymbolLookup lookup, String name) {
        return lookup.find(name).orElseThrow(() -> new LinkageError("Native function " + name + " could not be found"));
    }

    /**
     * Return a {@link VarHandle} to access an element within the given layout.
     * In JDK 21, {@code MemoryLayout.varHandle} returns a VarHandle directly.
     * In JDK 22+, it returns one with an extra offset coordinate that must be removed.
     */
    static VarHandle varHandleWithoutOffset(MemoryLayout layout, MemoryLayout.PathElement element) {
        return layout.varHandle(element);
    }

    /**
     * Allocate a wide (UTF-16LE) string in the given arena, null-terminated.
     */
    static MemorySegment allocateWideString(java.lang.foreign.Arena arena, String s) {
        byte[] bytes = (s + "\0").getBytes(StandardCharsets.UTF_16LE);
        MemorySegment segment = arena.allocateArray(JAVA_BYTE, bytes.length);
        segment.copyFrom(MemorySegment.ofArray(bytes));
        return segment;
    }

    private PanamaUtil() {}
}
