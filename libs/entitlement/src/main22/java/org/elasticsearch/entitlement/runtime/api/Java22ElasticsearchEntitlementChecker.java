/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.Java22EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.function.Consumer;

public class Java22ElasticsearchEntitlementChecker extends Java21ElasticsearchEntitlementChecker implements Java22EntitlementChecker {

    public Java22ElasticsearchEntitlementChecker(PolicyManager policyManager) {
        super(policyManager);
    }

    @Override
    public void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$withTargetLayout(
        Class<?> callerClass,
        AddressLayout that,
        MemoryLayout memoryLayout
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$upcallStub(
        Class<?> callerClass,
        Linker that,
        MethodHandle target,
        FunctionDescriptor function,
        Arena arena,
        Linker.Option... options
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(Class<?> callerClass, MemorySegment that, long newSize) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        long newSize,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, Arena arena) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, Arena arena) {
        // TODO: check filesystem entitlement READ
        policyManager.checkLoadingNativeLibraries(callerClass);
    }
}
