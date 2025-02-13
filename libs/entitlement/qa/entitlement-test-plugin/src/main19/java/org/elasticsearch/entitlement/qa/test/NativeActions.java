/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemorySession;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

class NativeActions {

    @EntitlementTest(expectedAccess = PLUGINS)
    static void linkerDowncallHandle() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void linkerDowncallHandleWithAddress() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(linker.defaultLookup().lookup("strlen").get(), FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    }

    static int callback() {
        return 0;
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void linkerUpcallStub() throws NoSuchMethodException {
        Linker linker = Linker.nativeLinker();

        MethodHandle mh = null;
        try {
            mh = MethodHandles.lookup().findStatic(NativeActions.class, "callback", MethodType.methodType(int.class));
        } catch (IllegalAccessException e) {
            assert false;
        }

        FunctionDescriptor callbackDescriptor = FunctionDescriptor.of(ValueLayout.JAVA_INT);
        linker.upcallStub(mh, callbackDescriptor, MemorySession.openImplicit());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void memorySegmentReinterpret() {
        MemorySession scope = MemorySession.openImplicit();
        MemorySegment someSegment;
        try {
            someSegment = MemorySegment.allocateNative(100, scope);
            var foreign = someSegment.get(ValueLayout.ADDRESS, 0);
            var segment = MemorySegment.ofAddress(foreign, 4, scope);
        } finally {
            someSegment = null;
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void symbolLookupWithPath() {
        try {
            SymbolLookup.libraryLookup(FileCheckActions.readDir().resolve("libFoo.so"), MemorySession.openImplicit());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void symbolLookupWithName() {
        try {
            SymbolLookup.libraryLookup("foo", MemorySession.openImplicit());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }
}
