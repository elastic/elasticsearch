/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.lang.foreign.Addressable;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryAddress;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.net.spi.InetAddressResolverProvider;
import java.nio.file.Path;
import java.util.concurrent.ForkJoinPool;

public class Java19Instrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        var builder = new EntitlementRulesBuilder(registry);

        builder.on(ForkJoinPool.class)
            .calling(ForkJoinPool::setParallelism, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        builder.on(Linker.class, rule -> {
            rule.calling(Linker::downcallHandle, FunctionDescriptor.class).enforce(Policies::loadingNativeLibraries).elseThrowNotEntitled();
            rule.calling(Linker::downcallHandle, Addressable.class, FunctionDescriptor.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(Linker::upcallStub, MethodHandle.class, FunctionDescriptor.class, MemorySession.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
        });

        builder.on(MemorySegment.class)
            .callingStatic(MemorySegment::ofAddress, MemoryAddress.class, Long.class, MemorySession.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        builder.on(SymbolLookup.class, rule -> {
            rule.callingStatic(SymbolLookup::libraryLookup, String.class, MemorySession.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.callingStatic(SymbolLookup::libraryLookup, Path.class, MemorySession.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
        });

        builder.on(InetAddressResolverProvider.class, rule -> {
            rule.protectedCtor().enforce(Policies::changeNetworkHandling).elseThrowNotEntitled();
        });
    }
}
