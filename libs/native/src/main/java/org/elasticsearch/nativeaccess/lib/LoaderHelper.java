/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A utility for loading libraries from Elasticsearch's platform specific lib dir.
 */
public class LoaderHelper {
    public static final Path platformLibDir = findPlatformLibDir();

    private static Path findPlatformLibDir() {
        // tests don't have an ES install, so the platform dir must be passed in explicitly
        String path = System.getProperty("es.nativelibs.path");
        if (path != null) {
            return Paths.get(path);
        }

        Path platformDir = Paths.get("lib", "platform");

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
        return platformDir.resolve(os + "-" + arch);
    }

    public static void loadLibrary(String libname) {
        Path libpath = platformLibDir.resolve(System.mapLibraryName(libname));
        if (Files.exists(libpath) == false) {
            throw new UnsatisfiedLinkError("Native library [" + libpath + "] does not exist");
        }
        System.load(libpath.toAbsolutePath().toString());
    }

    private LoaderHelper() {} // no construction
}
