/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.ffi;

import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.ffi.RuntimeHelper.downcallHandle;

class JdkSystemdLibrary implements SystemdLibrary {

    static {
        System.load(findLibSystemd());
    }

    // On some systems libsystemd does not have a non-versioned symlink. System.loadLibrary only knows how to find
    // non-versioned library files. So we must manually check the library path to find what we need.
    static String findLibSystemd() {
        final String libsystemd = "libsystemd.so.0";
        String libpath = System.getProperty("java.library.path");
        for (String basepath : libpath.split(":")) {
            var fullpath = Paths.get(basepath, libsystemd);
            if (Files.exists(fullpath)) {
                return fullpath.toAbsolutePath().toString();
            }
        }
        throw new UnsatisfiedLinkError("Could not find " + libsystemd + " in java.library.path: " + libpath);
    }

    private static final MethodHandle sd_notify$mh = downcallHandle("sd_notify", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));

    @Override
    public int sd_notify(int unset_environment, String state) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeState = arena.allocateUtf8String(state);
            return (int) sd_notify$mh.invokeExact(unset_environment, nativeState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
