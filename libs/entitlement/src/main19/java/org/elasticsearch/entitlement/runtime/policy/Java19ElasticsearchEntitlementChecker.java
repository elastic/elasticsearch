/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.bridge.Java19EntitlementChecker;

import java.lang.foreign.Addressable;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySession;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

public class Java19ElasticsearchEntitlementChecker extends ElasticsearchEntitlementChecker implements Java19EntitlementChecker {

    public Java19ElasticsearchEntitlementChecker(PolicyChecker policyChecker) {
        super(policyChecker);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_Linker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        Addressable address,
        FunctionDescriptor function
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        MemorySession scope
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_MemorySegment$$ofAddress(
        Class<?> callerClass,
        MemoryAddress address,
        long byteSize,
        MemorySession session
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, MemorySession session) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, MemorySession session) {
        policyChecker.checkFileRead(callerClass, path);
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }
}
