/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk;

import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkSystemdLibrary implements SystemdLibrary {

    static {
        loadLibSystemd();
    }

    // loadLibSystemd looks through java.library.path for one or more instances
    // of libsystemd, returning the first one that can be loaded. If none
    // can be found, or loaded, UnsatisfiedLinkError is thrown.
    static String loadLibSystemd() {
        // Note: on some systems libsystemd does not have a non-versioned symlink.
        // System.loadLibrary only knows how to find non-versioned library files,
        // so we must manually check the library path to find what we need.
        final String libsystemd = "libsystemd.so.0";
        String libpath = System.getProperty("java.library.path");
        for (String basepathStr : libpath.split(":")) {
            var basepath = Paths.get(basepathStr);
            if (Files.exists(basepath) == false) {
                continue;
            }
            try (var stream = Files.walk(basepath)) {
                var foundpaths = stream.filter(Files::isDirectory)
                        .map(p -> p.resolve(libsystemd))
                        .filter(Files::exists)
                        .map(p -> p.toAbsolutePath().toString())
                        .toList();

                UnsatisfiedLinkError last = null;
                for (String path : foundpaths) {
                    try {
                        System.load(path);
                        return path;
                    } catch (UnsatisfiedLinkError e) {
                        last = e;
                    }
                }
                if (last != null) {
                    throw last;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        }
        throw new UnsatisfiedLinkError("Could not find " + libsystemd + " in java.library.path: " + libpath);
    }

    private static final MethodHandle sd_notify$mh = downcallHandle("sd_notify", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));

    @Override
    public int sd_notify(int unset_environment, String state) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeState = MemorySegmentUtil.allocateString(arena, state);
            return (int) sd_notify$mh.invokeExact(unset_environment, nativeState);
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
    }
}
