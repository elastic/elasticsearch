/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.api;

import org.elasticsearch.entitlement.bridge.Java20EntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;

public class Java20ElasticsearchEntitlementChecker extends ElasticsearchEntitlementChecker implements Java20EntitlementChecker {

    public Java20ElasticsearchEntitlementChecker(PolicyManager policyManager) {
        super(policyManager);
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
    public void check$java_lang_foreign_Linker$downcallHandle(
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
        SegmentScope scope
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_MemorySegment$$ofAddress(Class<?> callerClass, long address) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_MemorySegment$$ofAddress(Class<?> callerClass, long address, long byteSize) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_MemorySegment$$ofAddress(Class<?> callerClass, long address, long byteSize, SegmentScope scope) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_MemorySegment$$ofAddress(
        Class<?> callerClass,
        long address,
        long byteSize,
        SegmentScope scope,
        Runnable cleanupAction
    ) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$jdk_internal_foreign_layout_ValueLayouts$OfAddressImpl$asUnbounded(Class<?> callerClass, ValueLayout.OfAddress that) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, String name, SegmentScope scope) {
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void check$java_lang_foreign_SymbolLookup$$libraryLookup(Class<?> callerClass, Path path, SegmentScope scope) {
        policyManager.checkFileRead(callerClass, path);
        policyManager.checkLoadingNativeLibraries(callerClass);
    }

    @Override
    public void checkReadAttributesIfExists(
        Class<?> callerClass,
        FileSystemProvider that,
        Path path,
        Class<?> type,
        LinkOption... options
    ) {
        policyManager.checkFileRead(callerClass, path);
    }

    @Override
    public void checkExists(Class<?> callerClass, FileSystemProvider that, Path path, LinkOption... options) {
        policyManager.checkFileRead(callerClass, path);
    }
}
