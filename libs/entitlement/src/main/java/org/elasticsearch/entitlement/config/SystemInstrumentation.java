/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import jdk.internal.foreign.AbstractMemorySegmentImpl;
import jdk.internal.foreign.abi.AbstractLinker;
import jdk.internal.foreign.layout.ValueLayouts;
import jdk.tools.jlink.internal.Jlink;
import jdk.tools.jlink.internal.Main;
import jdk.vm.ci.services.JVMCIServiceLocator;
import jdk.vm.ci.services.Services;

import com.sun.tools.jdi.VirtualMachineManagerImpl;

import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class SystemInstrumentation implements InstrumentationConfig {
    @Override
    public void init(Consumer<EntitlementRule> addRule) {
        EntitlementRules.on(addRule, Runtime.class)
            .callingVoid(Runtime::exit, Integer.class)
            .enforce(Policies::exitVM)
            .elseThrowNotEntitled()
            .callingVoid(Runtime::halt, Integer.class)
            .enforce(Policies::exitVM)
            .elseThrowNotEntitled()
            .callingVoid(Runtime::addShutdownHook, Thread.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoid(Runtime::removeShutdownHook, Thread.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoid(Runtime::load, String.class)
            .enforce((_, path) -> Policies.fileRead(Path.of(path)).and(Policies.loadingNativeLibraries()))
            .elseThrowNotEntitled()
            .callingVoid(Runtime::loadLibrary, String.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, System.class)
            .callingVoidStatic(System::exit, Integer.class)
            .enforce(Policies::exitVM)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::setProperty, String.class, String.class)
            .enforce(Policies::writeProperty)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::setProperties, Properties.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::clearProperty, String.class)
            .enforce(Policies::writeProperty)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::setIn, InputStream.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::setOut, PrintStream.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::setErr, PrintStream.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoidStatic(System::load, String.class)
            .enforce(path -> Policies.fileRead(Path.of(path)).and(Policies.loadingNativeLibraries()))
            .elseThrowNotEntitled()
            .callingVoidStatic(System::loadLibrary, String.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, ProcessBuilder.class)
            .calling(ProcessBuilder::start)
            .enforce(Policies::startProcess)
            .elseThrowNotEntitled()
            .callingStatic(ProcessBuilder::startPipeline, new TypeToken<List<ProcessBuilder>>() {})
            .enforce(Policies::startProcess)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, Jlink.class).protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();

        EntitlementRules.on(addRule, Main.class)
            .callingStatic(Main::run, PrintWriter.class, PrintWriter.class, String[].class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, JVMCIServiceLocator.class)
            .callingStatic(JVMCIServiceLocator::getProviders, new TypeToken<Class<?>>() {})
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, Services.class);
        // .callingStatic(Services::load, new TypeToken<Class<?>>() {})
        // .enforce(Policies::changeJvmGlobalState)
        // .elseThrowNotEntitled()
        // .callingStatic(Services::loadSingle, new TypeToken<Class<?>>() {}, TypeToken.of(Boolean.class))
        // .enforce(Policies::changeJvmGlobalState)
        // .elseThrowNotEntitled();

        EntitlementRules.on(addRule, VirtualMachineManagerImpl.class)
            .callingStatic(VirtualMachineManagerImpl::virtualMachineManager)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, ValueLayouts.OfAddressImpl.class)
            .calling(ValueLayouts.OfAddressImpl::withTargetLayout, MemoryLayout.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, AbstractLinker.class)
            .calling(AbstractLinker::downcallHandle, MemorySegment.class, FunctionDescriptor.class, Linker.Option[].class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(AbstractLinker::downcallHandle, FunctionDescriptor.class, Linker.Option[].class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(AbstractLinker::upcallStub, MethodHandle.class, FunctionDescriptor.class, Arena.class, Linker.Option[].class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, AbstractMemorySegmentImpl.class)
            .calling(AbstractMemorySegmentImpl::reinterpret, Long.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(
                AbstractMemorySegmentImpl::reinterpret,
                TypeToken.of(Long.class),
                TypeToken.of(Arena.class),
                new TypeToken<Consumer<MemorySegment>>() {}
            )
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(AbstractMemorySegmentImpl::reinterpret, TypeToken.of(Arena.class), new TypeToken<Consumer<MemorySegment>>() {})
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();
        ;

        EntitlementRules.on(addRule, MemorySegment.class)
            .calling(MemorySegment::reinterpret, Long.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(MemorySegment::reinterpret, TypeToken.of(Arena.class), new TypeToken<Consumer<MemorySegment>>() {})
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .calling(
                MemorySegment::reinterpret,
                TypeToken.of(Long.class),
                TypeToken.of(Arena.class),
                new TypeToken<Consumer<MemorySegment>>() {}
            )
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, SymbolLookup.class)
            .callingStatic(SymbolLookup::libraryLookup, String.class, Arena.class)
            .enforce(Policies::loadingNativeLibraries)
            .elseThrowNotEntitled()
            .callingStatic(SymbolLookup::libraryLookup, Path.class, Arena.class)
            .enforce((path) -> Policies.fileRead(path).and(Policies.loadingNativeLibraries()))
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, ModuleLayer.Controller.class)
            .callingVoid(ModuleLayer.Controller::enableNativeAccess, Module.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled();
    }
}
