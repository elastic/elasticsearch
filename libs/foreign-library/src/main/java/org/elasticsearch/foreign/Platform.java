/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

/**
 * Represents an OS and architecture combination that Elasticsearch ships on.
 *
 * <p>Use {@link #current()} to detect the running platform at load time.
 */
public enum Platform {
    LINUX_X64,
    LINUX_AARCH64,
    DARWIN_X64,
    DARWIN_AARCH64,
    WINDOWS_X64;

    /**
     * Returns the {@code Platform} for the current JVM, derived from
     * {@code os.name} and {@code os.arch} system properties.
     *
     * @throws IllegalStateException if the OS or architecture is not recognized
     */
    public static Platform current() {
        return resolve(System.getProperty("os.name"), System.getProperty("os.arch"));
    }

    /**
     * Resolves a {@code Platform} from the given OS name and architecture strings,
     * using the same normalization logic as {@link LoaderHelper}.
     *
     * @throws IllegalStateException if either argument is null, or if the OS or architecture is not recognized
     */
    static Platform resolve(String osName, String archName) {
        if (osName == null) {
            throw new IllegalStateException("os.name system property is not set");
        }
        if (archName == null) {
            throw new IllegalStateException("os.arch system property is not set");
        }

        boolean isWindows = osName.startsWith("Windows");
        boolean isLinux = osName.startsWith("Linux");
        boolean isDarwin = osName.startsWith("Mac OS");

        if (isLinux == false && isDarwin == false && isWindows == false) {
            throw new IllegalStateException("Unrecognized OS: " + osName);
        }

        boolean isX64 = archName.equals("amd64") || archName.equals("x86_64");
        boolean isAarch64 = archName.equals("aarch64");

        if (isX64 == false && isAarch64 == false) {
            throw new IllegalStateException("Unrecognized architecture: " + archName);
        }

        if (isLinux && isX64) return LINUX_X64;
        if (isLinux && isAarch64) return LINUX_AARCH64;
        if (isDarwin && isX64) return DARWIN_X64;
        if (isDarwin && isAarch64) return DARWIN_AARCH64;
        if (isWindows && isX64) return WINDOWS_X64;

        // e.g. Windows aarch64 is not a supported Elasticsearch platform
        throw new IllegalStateException("Unsupported platform: " + osName + "/" + archName);
    }
}
