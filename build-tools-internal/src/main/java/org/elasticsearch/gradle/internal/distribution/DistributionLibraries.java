/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

/**
 * Utility types and helpers for script-registered distribution libraries.
 */
public final class DistributionLibraries {

    /**
     * Supported copy layouts for registered distribution libraries.
     */
    public enum Layout {
        ROOT,
        NESTED,
        SERVER_LAUNCHER,
        PLATFORM_NATIVE
    }

    private DistributionLibraries() {}

    public static String linuxServerLauncherNativeConfiguration(
        String os,
        String architecture,
        String x64Configuration,
        String aarch64Configuration
    ) {
        if ("linux".equals(os) == false) {
            return null;
        }
        if (architecture == null) {
            throw new IllegalArgumentException("Architecture must not be null for linux server-launcher native libraries");
        }
        return switch (architecture) {
            case "x64" -> x64Configuration;
            case "aarch64" -> aarch64Configuration;
            default -> throw new IllegalArgumentException("Unsupported linux server-launcher architecture [" + architecture + "]");
        };
    }

    public static String platformInclude(String os, String architecture) {
        if (os == null) {
            return null;
        }
        return os + "-" + architecture + "/*";
    }
}
