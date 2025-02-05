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
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

class VersionSpecificNativeChecks {

    static void enableNativeAccess() throws Exception {
        // Available only from Java20
    }

    static void addressLayoutWithTargetLayout() {
        // Available only from Java20
    }

    static void linkerDowncallHandle() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    }

    static void linkerDowncallHandleWithAddress() {
        Linker linker = Linker.nativeLinker();
        linker.downcallHandle(linker.defaultLookup().lookup("strlen").get(), FunctionDescriptor.of(JAVA_LONG, ADDRESS));
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
        linker.upcallStub(mh, callbackDescriptor, MemorySession.openImplicit());
    }

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

    static void memorySegmentReinterpretWithCleanup() {
        // No equivalent to this function before Java20
    }

    static void memorySegmentReinterpretWithSize() {
        // No equivalent to this function before Java20
    }

    static void symbolLookupWithPath() {
        try {
            SymbolLookup.libraryLookup(Path.of("/foo/bar/libFoo.so"), MemorySession.openImplicit());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }

    static void symbolLookupWithName() {
        try {
            SymbolLookup.libraryLookup("foo", MemorySession.openImplicit());
        } catch (IllegalArgumentException e) {
            // IllegalArgumentException is thrown if path does not point to a valid library (and it does not)
        }
    }
}
