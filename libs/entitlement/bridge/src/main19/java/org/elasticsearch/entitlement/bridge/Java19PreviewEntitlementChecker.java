/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import java.lang.foreign.Addressable;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySession;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * Interface with Java19 Preview specific functions and types.
 * This interface must be kept isolated, as we cannot inherit from it in subsequent Java-specific versions as it contains types that
 * were removed in the following previews/in final code (like MemorySession or MemoryAddress)
 */
public interface Java19PreviewEntitlementChecker {

    /**
     * downcallHandle has a different signature in Java 19.
     * See docs: https://docs.oracle.com/en/java/javase/19/docs/api/java.base/java/lang/foreign/Linker.html#downcallHandle(java.lang.foreign.FunctionDescriptor)
     *
     * Its only allowed implementation is in AbstractLinker:
     * https://github.com/openjdk/jdk19u/blob/677bec11078ff41c21821fec46590752e0fc5128/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L47
     */
    void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(Class<?> callerClass, Linker that, FunctionDescriptor function);

    /**
     * downcallHandle has a different signature in Java 19, and it is a default interface method. Later implementations (Java 21+)
     * use an implementation class for this overload too.
     * See docs: https://docs.oracle.com/en/java/javase/19/docs/api/java.base/java/lang/foreign/Linker.html#downcallHandle(java.lang.foreign.Addressable,java.lang.foreign.FunctionDescriptor)
     */
    void check$java_lang_foreign_Linker$downcallHandle(Class<?> callerClass, Linker that, Addressable address, FunctionDescriptor function);

    /**
     * upcallStub has a different signature in Java 19,
     * Its only allowed implementation is in AbstractLinker:
     * https://github.com/openjdk/jdk19u/blob/677bec11078ff41c21821fec46590752e0fc5128/src/java.base/share/classes/jdk/internal/foreign/abi/AbstractLinker.java#L60
     */
    void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        MemorySession scope
    );

    /**
     * This function has a different signature in Java 20.
     * See docs: https://docs.oracle.com/en/java/javase/19/docs/api/java.base/java/lang/foreign/MemorySegment.html#ofAddress(java.lang.foreign.MemoryAddress,long,java.lang.foreign.MemorySession)
     *
     * It is superseded by {@code MemorySegment.reinterpret} in the final
     * implementation (Java 21+)
     */
    void check$java_lang_foreign_MemorySegment$$ofAddress(
        Class<?> callerClass,
        MemoryAddress address,
        long byteSize,
        MemorySession session
    );

    /**
     * This function signature changes from Java 19 to Java 20 (MemorySession parameter).
     */
    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, MemorySession session);

    /**
     * This function signature changes from Java 19 to Java 20 (MemorySession parameter).
     */
    void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, MemorySession session);
}
