/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;

class RuntimeHelper {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup SYMBOL_LOOKUP = LINKER.defaultLookup();

    static MemorySegment functionAddress(String function) {
        return SYMBOL_LOOKUP.find(function).orElseThrow(
            () -> new LinkageError("Native function " + function + " could not be found"));
    }

    static MethodHandle downcallHandle(String function, FunctionDescriptor functionDescriptor, Linker.Option... options) {
        return LINKER.downcallHandle(functionAddress(function), functionDescriptor);
    }
}
