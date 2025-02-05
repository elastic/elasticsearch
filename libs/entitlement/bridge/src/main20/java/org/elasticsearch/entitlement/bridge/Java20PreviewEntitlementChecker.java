/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Interface with Java20 Preview specific functions and types.
 * This interface must be kept isolated, as we cannot inherit from it in subsequent Java-specific versions as it contains types that
 * were removed in the following previews/in final code (like MemorySession or MemoryAddress)
 */
public interface Java20PreviewEntitlementChecker {

    // Docs with function signature:
    // https://github.com/openjdk/jdk20u/blob/9ced461a4d8cb2ecfe2d6a74ec218ec589dcd617/src/java.base/share/classes/java/lang/foreign/Linker.java#L211
    // Implementation:
    // https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/lang/foreign/Linker.html#downcallHandle(java.lang.foreign.MemorySegment,java.lang.foreign.FunctionDescriptor,java.lang.foreign.Linker.Option...)
    void check$java_lang_foreign_Linker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    );

    // Implementation:
    // https://github.com/openjdk/jdk20u/blob/9ced461a4d8cb2ecfe2d6a74ec218ec589dcd617/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L69
    void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        SegmentScope scope
    );

    // Docs with function signature:
    // https://docs.oracle.com/en/java/javase/20/docs/api/java.base/java/lang/foreign/MemorySegment.html#ofAddress(long,long,java.lang.foreign.SegmentScope)
    // Implementation:
    // https://github.com/openjdk/jdk20u/blob/9ced461a4d8cb2ecfe2d6a74ec218ec589dcd617/src/java.base/share/classes/java/lang/foreign/MemorySegment.java#L1071C5-L1071C64
    void check$java_lang_foreign_MemorySegment$$ofAddress(Class<?> callerClass, long address, long byteSize);

    // Java20 only -- superseded by MemorySegment.reinterpret
    void check$java_lang_foreign_MemorySegment$$ofAddress(Class<?> callerClass, long address, long byteSize, SegmentScope scope);

    // Java20 only -- superseded by MemorySegment.reinterpret
    void check$java_lang_foreign_MemorySegment$$ofAddress(
        Class<?> callerClass,
        long address,
        long byteSize,
        SegmentScope scope,
        Runnable cleanupAction
    );

    // Java20 only -- superseded by MemorySegment.reinterpret
    // Implementation:
    // https://github.com/openjdk/jdk20u/blob/9ced461a4d8cb2ecfe2d6a74ec218ec589dcd617/src/java.base/share/classes/jdk/internal/foreign/layout/ValueLayouts.java#L442
    void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$asUnbounded(Class<?> callerClass, ValueLayout.OfAddress that);

    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, SegmentScope scope);

    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, SegmentScope scope);
}
