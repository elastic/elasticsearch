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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static org.elasticsearch.nativeaccess.jdk.LinkerHelper.downcallHandle;

class JdkSystemdLibrary implements SystemdLibrary {

    static {
        // Find and load libsystemd. We attempt all instances of
        // libsystemd in case of multiarch systems, and stop when
        // one is successfully loaded. If none can be loaded,
        // UnsatisfiedLinkError will be thrown.
        List<String> paths = findLibSystemd();
        if (paths.isEmpty()) {
            String libpath = System.getProperty("java.library.path");
            throw new UnsatisfiedLinkError("Could not find libsystemd in java.library.path: " + libpath);
        }
        UnsatisfiedLinkError last = null;
        for (String path : paths) {
            try {
                System.load(path);
                last = null;
                break;
            } catch (UnsatisfiedLinkError e) {
                last = e;
            }
        }
        if (last != null) {
            throw last;
        }
    }

    // findLibSystemd returns a list of paths to instances of libsystemd
    // found within java.library.path.
    static List<String> findLibSystemd() {
        // Note: on some systems libsystemd does not have a non-versioned symlink.
        // System.loadLibrary only knows how to find non-versioned library files,
        // so we must manually check the library path to find what we need.
        final Path libsystemd = Paths.get("libsystemd.so.0");
        final String libpath = System.getProperty("java.library.path");
        return Arrays.stream(libpath.split(":")).map(Paths::get).filter(Files::exists).flatMap(p -> {
            try {
                return Files.find(
                    p,
                    Integer.MAX_VALUE,
                    (fp, attrs) -> (attrs.isDirectory() == false && fp.getFileName().equals(libsystemd))
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).map(p -> p.toAbsolutePath().toString()).toList();
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
