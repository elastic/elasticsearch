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
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.IOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

public class Java21Instrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        var builder = new EntitlementRulesBuilder(registry);

        builder.on(ForkJoinPool.class)
            .calling(ForkJoinPool::setParallelism, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        builder.on(Linker.class, rule -> {
            rule.calling(Linker::downcallHandle, MemorySegment.class, FunctionDescriptor.class, Linker.Option[].class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(Linker::downcallHandle, FunctionDescriptor.class, Linker.Option[].class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(Linker::upcallStub, MethodHandle.class, FunctionDescriptor.class, Arena.class, Linker.Option[].class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
        });

        builder.on(MemorySegment.class, rule -> {
            rule.calling(MemorySegment::reinterpret, Long.class).enforce(Policies::loadingNativeLibraries).elseThrowNotEntitled();
            rule.calling(MemorySegment::reinterpret, TypeToken.of(Arena.class), new TypeToken<Consumer<MemorySegment>>() {})
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.calling(
                MemorySegment::reinterpret,
                TypeToken.of(Long.class),
                TypeToken.of(Arena.class),
                new TypeToken<Consumer<MemorySegment>>() {}
            ).enforce(Policies::loadingNativeLibraries).elseThrowNotEntitled();
        });

        builder.on(SymbolLookup.class, rule -> {
            rule.callingStatic(SymbolLookup::libraryLookup, String.class, Arena.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
            rule.callingStatic(SymbolLookup::libraryLookup, Path.class, Arena.class)
                .enforce((path) -> Policies.fileRead(path).and(Policies.loadingNativeLibraries()))
                .elseThrowNotEntitled();
        });

        builder.on(AddressLayout.class, rule -> {
            rule.calling(AddressLayout::withTargetLayout, MemoryLayout.class)
                .enforce(Policies::loadingNativeLibraries)
                .elseThrowNotEntitled();
        });

        builder.on(ModuleLayer.Controller.class, rule -> {
            rule.callingVoid(ModuleLayer.Controller::enableNativeAccess, Module.class)
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
        });

        builder.on(FileSystems.getDefault().provider().getClass(), rule -> {
            rule.calling(
                FileSystemProvider::readAttributesIfExists,
                TypeToken.of(Path.class),
                new TypeToken<Class<? extends BasicFileAttributes>>() {},
                TypeToken.of(LinkOption[].class)
            ).enforce((provider, path) -> Policies.fileRead(path)).elseThrow(IOException::new);
        });

        builder.on(FileSystems.getDefault().provider().getClass(), rule -> {
            rule.calling(FileSystemProvider::exists, Path.class, LinkOption[].class)
                .enforce((provider, path) -> Policies.fileRead(path))
                .elseReturn(false);
        });
    }
}
