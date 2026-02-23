/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import jdk.tools.jlink.internal.Jlink;
import jdk.tools.jlink.internal.Main;

import com.sun.tools.jdi.VirtualMachineManagerImpl;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.rules.TypeToken;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

public class SystemInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(Runtime.class, rule -> {
            rule.callingVoid(Runtime::exit, Integer.class).enforce(Policies::exitVM).elseThrowNotEntitled();
            rule.callingVoid(Runtime::halt, Integer.class).enforce(Policies::exitVM).elseThrowNotEntitled();
            rule.callingVoid(Runtime::addShutdownHook, Thread.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoid(Runtime::removeShutdownHook, Thread.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoid(Runtime::load, String.class)
                .enforce((runtime, path) -> Policies.fileRead(Path.of(path)).and(Policies.loadingNativeLibraries()))
                .elseThrowNotEntitled();
            rule.callingVoid(Runtime::loadLibrary, String.class).enforce(Policies::loadingNativeLibraries).elseThrowNotEntitled();
        });

        builder.on(System.class, rule -> {
            rule.callingVoidStatic(System::exit, Integer.class).enforce(Policies::exitVM).elseThrowNotEntitled();
            rule.callingVoidStatic(System::setProperty, String.class, String.class).enforce(Policies::writeProperty).elseThrowNotEntitled();
            rule.callingVoidStatic(System::setProperties, Properties.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoidStatic(System::clearProperty, String.class).enforce(Policies::writeProperty).elseThrowNotEntitled();
            rule.callingVoidStatic(System::setIn, InputStream.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoidStatic(System::setOut, PrintStream.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoidStatic(System::setErr, PrintStream.class).enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled();
            rule.callingVoidStatic(System::load, String.class)
                .enforce(path -> Policies.fileRead(Path.of(path)).and(Policies.loadingNativeLibraries()))
                .elseThrowNotEntitled();
            rule.callingVoidStatic(System::loadLibrary, String.class).enforce(Policies::loadingNativeLibraries).elseThrowNotEntitled();
        });

        builder.on(ProcessBuilder.class, rule -> {
            rule.calling(ProcessBuilder::start).enforce(Policies::startProcess).elseThrowNotEntitled();
            rule.callingStatic(ProcessBuilder::startPipeline, new TypeToken<List<ProcessBuilder>>() {})
                .enforce(Policies::startProcess)
                .elseThrowNotEntitled();
        });

        builder.on(Jlink.class, rule -> { rule.protectedCtor().enforce(Policies::changeJvmGlobalState).elseThrowNotEntitled(); });

        builder.on(Main.class, rule -> {
            rule.callingStatic(Main::run, PrintWriter.class, PrintWriter.class, String[].class)
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
        });

        builder.on(VirtualMachineManagerImpl.class, rule -> {
            rule.callingStatic(VirtualMachineManagerImpl::virtualMachineManager)
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
        });
    }
}
