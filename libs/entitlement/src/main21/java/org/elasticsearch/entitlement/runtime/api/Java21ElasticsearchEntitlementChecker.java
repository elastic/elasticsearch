/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.Java21EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.function.Consumer;

public class Java21ElasticsearchEntitlementChecker extends ElasticsearchEntitlementChecker implements Java21EntitlementChecker {

    public Java21ElasticsearchEntitlementChecker(PolicyChecker policyChecker) {
        super(policyChecker);
    }

    @Override
    public void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$withTargetLayout(
        Class<?> callerClass,
        AddressLayout that,
        MemoryLayout memoryLayout
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        MemorySegment address,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_abi_AbstractLinker$downcallHandle(
        Class<?> callerClass,
        Linker that,
        FunctionDescriptor function,
        Linker.Option... options
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
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
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(Class<?> callerClass, MemorySegment that, long newSize) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        long newSize,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_AbstractMemorySegmentImpl$reinterpret(
        Class<?> callerClass,
        MemorySegment that,
        Arena arena,
        Consumer<MemorySegment> cleanup
    ) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, Arena arena) {
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, Arena arena) {
        policyChecker.checkFileRead(callerClass, path);
        policyChecker.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void checkReadAttributesIfExists(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Class<?> type,
        LinkOption... options
    ) {
        policyChecker.checkFileRead(callerClass, path);
    }

    @Override
    public void checkExists(Class<?> callerClass, FileSystemProvider that, Path path, LinkOption... options) {
        policyChecker.checkFileRead(callerClass, path);
    }
}
