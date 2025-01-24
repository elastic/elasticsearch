/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

class VersionSpecificNativeChecks {

    static void enableNativeAccess() throws Exception {
        ModuleLayer parent = ModuleLayer.boot();

        var location = EntitlementTestPlugin.class.getProtectionDomain().getCodeSource().getLocation();

        // We create a layer for our own module, so we have a controller to try and call enableNativeAccess on it.
        // This works in both the modular and non-modular case: the target module has to be present in the new layer, but its entitlements
        // and policies do not matter to us: we are checking that the caller is (or isn't) entitled to use enableNativeAccess
        Configuration cf = parent.configuration()
            .resolve(ModuleFinder.of(Path.of(location.toURI())), ModuleFinder.of(), Set.of("org.elasticsearch.entitlement.qa.test"));
        var controller = ModuleLayer.defineModulesWithOneLoader(cf, List.of(parent), ClassLoader.getSystemClassLoader());
        var targetModule = controller.layer().findModule("org.elasticsearch.entitlement.qa.test");

        controller.enableNativeAccess(targetModule.get());
    }

    static void addressLayoutWithTargetLayout() {
        AddressLayout addressLayout = ADDRESS.withoutTargetLayout();
        addressLayout.withTargetLayout(MemoryLayout.sequenceLayout(Long.MAX_VALUE, ValueLayout.JAVA_BYTE));
    }

    static void linkerDowncallHandle() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    }

    static void linkerDowncallHandleWithAddress() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(linker.defaultLookup().find("strlen").get(), FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    }

    static int callback() {
        return 0;
    }

    static void linkerUpcallStub() throws NoSuchMethodException {
        Linker linker = Linker.nativeLinker();

        MethodHandle mh = null;
        try {
            mh = MethodHandles.lookup().findStatic(VersionSpecificNativeChecks.class, "callback", MethodType.methodType(int.class));
        } catch (IllegalAccessException e) {
            assert false;
        }

        FunctionDescriptor callbackDescriptor = FunctionDescriptor.of(ValueLayout.JAVA_INT);
        linker.upcallStub(mh, callbackDescriptor, Arena.ofAuto());
    }

    static void memorySegmentReinterpret() {
        Arena arena = Arena.ofAuto();
        MemorySegment segment = arena.allocate(100);
        segment.reinterpret(50);
    }

    static void memorySegmentReinterpretWithCleanup() {
        Arena arena = Arena.ofAuto();
        MemorySegment segment = arena.allocate(100);
        segment.reinterpret(Arena.ofAuto(), s -> {});
    }

    static void memorySegmentReinterpretWithSizeAndCleanup() {
        Arena arena = Arena.ofAuto();
        MemorySegment segment = arena.allocate(100);
        segment.reinterpret(50, Arena.ofAuto(), s -> {});
    }

    static void symbolLookupWithPath() {
        try {
            SymbolLookup.libraryLookup(Path.of("/foo/bar/libFoo.so"), Arena.ofAuto());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }

    static void symbolLookupWithName() {
        try {
            SymbolLookup.libraryLookup("foo", Arena.ofAuto());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }
}
