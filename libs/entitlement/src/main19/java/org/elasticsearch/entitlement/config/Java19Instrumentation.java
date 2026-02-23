/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import jdk.internal.foreign.abi.AbstractLinker;

import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy;
import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySession;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class Java19Instrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        var builder = new EntitlementRulesBuilder(registry);

        builder.on(ForkJoinPool.class)
            .calling(ForkJoinPool::setParallelism, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        builder.on(AbstractLinker.class, rule -> {
            rule.calling(AbstractLinker::downcallHandle, FunctionDescriptor.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(AbstractLinker::downcallHandle, FunctionDescriptor.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(AbstractLinker::upcallStub, MethodHandle.class, FunctionDescriptor.class, MemorySession.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
        });

        registry.registerRule(
            new EntitlementRule(
                new MethodKey(
                    "java/lang/foreign/Linker",
                    "downcallHandle",
                    List.of("java.lang.foreign.Addressable", "java.lang.foreign.FunctionDescriptor")
                ),
                args -> Policies.loadingNativeLibraries(),
                new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
            )
        );

        registry.registerRule(
            new EntitlementRule(
                new MethodKey(
                    "java/lang/foreign/MemorySegment",
                    "ofAddress",
                    List.of("java.lang.foreign.MemoryAddress", "java.lang.Long", "java.lang.foreign.MemorySession")
                ),
                args -> Policies.loadingNativeLibraries(),
                new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
            )
        );

        registry.registerRule(
            new EntitlementRule(
                new MethodKey(
                    "java/lang/foreign/SymbolLookup",
                    "libraryLookup",
                    List.of("java.lang.String", "java.lang.foreign.MemorySession")
                ),
                args -> Policies.loadingNativeLibraries(),
                new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
            )
        );

        registry.registerRule(
            new EntitlementRule(
                new MethodKey(
                    "java/lang/foreign/SymbolLookup",
                    "libraryLookup",
                    List.of("java.nio.file.Path", "java.lang.foreign.MemorySession")
                ),
                args -> Policies.loadingNativeLibraries(),
                new DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy()
            )
        );
    }
}
