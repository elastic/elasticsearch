/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import java.lang.foreign.Linker;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;

public final class LinkerAdapter {

    static final Linker.Option[] ALLOW_HEAP_ACCESS = new Linker.Option[] { Linker.Option.critical(true) };

    /** Returns a linker option used to mark a foreign function as critical. */
    public static Linker.Option[] critical() {
        return ALLOW_HEAP_ACCESS;
    }

    /**
     * JDK 22+ identity adapter: {@code Linker.Option.critical(true)} already lets the raw downcall accept heap
     * segments, so the {@code @Critical} fallback adapter is never resolved. Mirrors the JDK 21 signature so
     * the generated {@code $Impl} bytecode resolves on both releases.
     */
    public static MethodHandle adaptCritical(Lookup lookup, MethodHandle rawHandle, Class<?> adapterClass, String methodName) {
        return rawHandle;
    }

    private LinkerAdapter() {}
}
