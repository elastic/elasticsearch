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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;

/**
 * Adapts Linker APIs that changed between JDK 21 and 22+.
 */
public final class LinkerAdapter {

    static final Linker.Option[] NONE = new Linker.Option[0];

    /** Returns an empty linker option array, since critical is only available since Java 22. */
    public static Linker.Option[] critical() {
        return NONE;
    }

    /**
     * JDK 21 wraps the raw downcall handle of a {@code @Critical} binding through the user-supplied adapter:
     * {@code Linker.Option.critical(true)} is unavailable on this release, so the raw handle would reject any
     * heap {@link java.lang.foreign.MemorySegment} argument. The adapter must declare a {@code public static}
     * method with the given name whose parameter list is {@code (MethodHandle, …origParams)} and whose return
     * type matches {@code rawHandle}'s return type; the processor enforces this at compile time. We resolve it
     * here with the supplied {@link Lookup} (the generated {@code $Impl}'s own lookup, so the adapter package
     * does not need to be exported beyond the binding's module) and bind {@code rawHandle} as the leading
     * argument so the returned handle has the same {@link MethodType} as {@code rawHandle}.
     */
    public static MethodHandle adaptCritical(Lookup lookup, MethodHandle rawHandle, Class<?> adapterClass, String methodName) {
        MethodType adapterType = rawHandle.type().insertParameterTypes(0, MethodHandle.class);
        try {
            MethodHandle wrapper = lookup.findStatic(adapterClass, methodName, adapterType);
            return MethodHandles.insertArguments(wrapper, 0, rawHandle);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("@Critical fallback adapter not resolvable: " + adapterClass.getName() + "." + methodName, e);
        }
    }

    private LinkerAdapter() {}
}
