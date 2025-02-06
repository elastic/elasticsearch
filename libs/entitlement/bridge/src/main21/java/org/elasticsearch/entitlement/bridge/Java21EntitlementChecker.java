/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.function.Consumer;

public interface Java21EntitlementChecker extends Java20StableEntitlementChecker {

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     * See docs: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/foreign/AddressLayout.html#withTargetLayout(java.lang.foreign.MemoryLayout)
     *
     * It has only one allowed implementation (interface is sealed).
     * See https://github.com/openjdk/jdk21u/blob/7069f193f1f8c61869fc68a36c17f3a9a7b7b2a0/src/java.base/share/classes/jdk/internal/foreign/layout/ValueLayouts.java#L350
     */
    void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$withTargetLayout(
        Class<?> callerClass,
        AddressLayout that,
        MemoryLayout memoryLayout
    );

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     * See docs: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/foreign/Linker.html#downcallHandle(java.lang.foreign.FunctionDescriptor,java.lang.foreign.Linker.Option...)
     *
     * It has only one allowed implementation (interface is sealed).
     * See https://github.com/openjdk/jdk21u/blob/d2cbada0b7c88521dfb4d3696205c9beb77018af/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L77
     */
    void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    );

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     *
     * It has only one allowed implementation (interface is sealed).
     * See https://github.com/openjdk/jdk21u/blob/d2cbada0b7c88521dfb4d3696205c9beb77018af/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L112
     */
    void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        Arena arena,
        Linker.Option... options
    );

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     *
     * It has only one allowed implementation (interface is sealed).
     * See https://github.com/openjdk/jdk21u/blob/d2cbada0b7c88521dfb4d3696205c9beb77018af/src/java.base/share/classes/jdk/internal/foreign/AbstractMemorySegmentImpl.java#L135
     */
    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(Class<?> callerClass, MemorySegment that, long newSize);

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     * It has only one allowed implementation in AbstractMemorySegmentImpl (interface is sealed).
     */
    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        long newSize,
        Arena arena,
        Consumer<MemorySegment> cleanup
    );

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     * It has only one allowed implementation in AbstractMemorySegmentImpl (interface is sealed).
     */
    void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        Arena arena,
        Consumer<MemorySegment> cleanup
    );

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     */
    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, Arena arena);

    /**
     * This function is in preview in Java 21, but it already has its final signature.
     */
    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, Arena arena);
}
