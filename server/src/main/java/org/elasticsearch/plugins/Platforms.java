/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.util.Constants;

import java.nio.file.Path;
import java.util.Locale;

/**
 * Encapsulates platform-dependent methods for handling native components of plugins.
 */
public class Platforms {

    private static final String PROGRAM_NAME = Constants.WINDOWS ? "controller.exe" : "controller";
    public static final String PLATFORM_NAME = Platforms.platformName(Constants.OS_NAME, Constants.OS_ARCH);

    private Platforms() {}

    /**
     * The path to the native controller for a plugin with native components.
     */
    public static Path nativeControllerPath(Path plugin) {
        if (Constants.MAC_OS_X) {
            return plugin
                .resolve("platform")
                .resolve(PLATFORM_NAME)
                .resolve(PROGRAM_NAME + ".app")
                .resolve("Contents")
                .resolve("MacOS")
                .resolve(PROGRAM_NAME);
        }
        return plugin
                .resolve("platform")
                .resolve(PLATFORM_NAME)
                .resolve("bin")
                .resolve(PROGRAM_NAME);
    }

    /**
     * Return the platform name based on the OS name and architecture, for example:
     * - darwin-x86_64
     * - linux-x86-64
     * - windows-x86_64
     * For *nix platforms this is more-or-less `uname -s`-`uname -m` converted to lower case.
     * However, for consistency between different operating systems on the same architecture
     * "amd64" is replaced with "x86_64" and "i386" with "x86".
     * For Windows it's "windows-" followed by either "x86" or "x86_64".
     */
    public static String platformName(final String osName, final String osArch) {
        final String lowerCaseOs = osName.toLowerCase(Locale.ROOT);
        final String normalizedOs;
        if (lowerCaseOs.startsWith("windows")) {
            normalizedOs = "windows";
        } else if (lowerCaseOs.equals("mac os x")) {
            normalizedOs = "darwin";
        } else {
            normalizedOs = lowerCaseOs;
        }

        final String lowerCaseArch = osArch.toLowerCase(Locale.ROOT);
        final String normalizedArch;
        if (lowerCaseArch.equals("amd64")) {
            normalizedArch = "x86_64";
        } else if (lowerCaseArch.equals("i386")) {
            normalizedArch = "x86";
        } else {
            normalizedArch = lowerCaseArch;
        }

        return normalizedOs + "-" + normalizedArch;
    }

}
