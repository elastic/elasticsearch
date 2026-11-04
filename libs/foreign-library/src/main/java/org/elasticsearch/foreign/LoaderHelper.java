/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * A utility for loading libraries from Elasticsearch's platform specific lib dirs.
 */
public class LoaderHelper {
    public static final List<Path> platformLibDirs = findPlatformLibDirs();

    private static List<Path> findPlatformLibDirs() {
        // tests don't have an ES install, so the platform dirs must be passed in explicitly
        String paths = System.getProperty("es.nativelibs.path");
        if (paths != null) {
            return getPathList(paths);
        }

        Path homeDir = Paths.get(System.getProperty("es.path.home"));
        Path platformDir = homeDir.resolve("lib/platform");

        String osname = System.getProperty("os.name");
        String os;
        if (osname.startsWith("Windows")) {
            os = "windows";
        } else if (osname.startsWith("Linux")) {
            os = "linux";
        } else if (osname.startsWith("Mac OS")) {
            os = "darwin";
        } else {
            os = "unsupported_os[" + osname + "]";
        }
        String archname = System.getProperty("os.arch");
        String arch;
        if (archname.equals("amd64") || archname.equals("x86_64")) {
            arch = "x64";
        } else if (archname.equals("aarch64")) {
            arch = archname;
        } else {
            arch = "unsupported_arch[" + archname + "]";
        }
        return List.of(platformDir.resolve(os + "-" + arch));
    }

    @SuppressForbidden(reason = "pathSeparator")
    private static List<Path> getPathList(String paths) {
        return Arrays.stream(paths.split(File.pathSeparator)).filter(path -> path.isEmpty() == false).map(Paths::get).toList();
    }

    public static void loadLibrary(String libname) {
        String filename = System.mapLibraryName(libname);
        for (Path platformLibDir : platformLibDirs) {
            Path libpath = platformLibDir.resolve(filename);
            if (Files.exists(libpath)) {
                System.load(libpath.toAbsolutePath().toString());
                return;
            }
        }
        throw new UnsatisfiedLinkError("Native library [" + filename + "] does not exist in " + platformLibDirs);
    }

    private LoaderHelper() {} // no construction
}
