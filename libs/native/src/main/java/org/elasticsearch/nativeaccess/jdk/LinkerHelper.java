/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * Utility methods for calling into the native linker.
 */
class LinkerHelper {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup SYMBOL_LOOKUP;
    private static final MethodHandles.Lookup MH_LOOKUP = MethodHandles.lookup();

    static {
        // We first check the loader lookup, which contains libs loaded by System.load and System.loadLibrary.
        // If the symbol isn't found there, we fall back to the default lookup, which is "common libraries" for
        // the platform, typically eg libc
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        SYMBOL_LOOKUP = (name) -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
    }

    static MemorySegment functionAddress(String function) {
        return SYMBOL_LOOKUP.find(function).orElseThrow(() -> new LinkageError("Native function " + function + " could not be found"));
    }

    static MethodHandle downcallHandle(String function, FunctionDescriptor functionDescriptor, Linker.Option... options) {
        return LINKER.downcallHandle(functionAddress(function), functionDescriptor, options);
    }

    static MethodHandle upcallHandle(Class<?> clazz, String methodName, FunctionDescriptor functionDescriptor) {
        try {
            return MH_LOOKUP.findVirtual(clazz, methodName, functionDescriptor.toMethodType());
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    static <T> MemorySegment upcallStub(MethodHandle mh, T instance, FunctionDescriptor functionDescriptor, Arena arena) {
        try {
            mh = mh.bindTo(instance);
            return LINKER.upcallStub(mh, functionDescriptor, arena);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
