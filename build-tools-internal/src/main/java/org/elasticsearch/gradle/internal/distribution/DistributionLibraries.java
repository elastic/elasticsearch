/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import java.util.List;

/**
 * Canonical mapping of distribution library configurations to their target directories in the assembled distribution.
 */
public final class DistributionLibraries {

    public static final String LIBS = "libs";
    public static final String LIBS_VERSION_CHECKER = "libsVersionChecker";
    public static final String LIBS_CLI_LAUNCHER = "libsCliLauncher";
    public static final String LIBS_SERVER_CLI = "libsServerCli";
    public static final String LIBS_SERVER_LAUNCHER = "libsServerLauncher";
    public static final String LIBS_SERVER_LAUNCHER_NATIVE_X64 = "libsServerLauncherNativeX64";
    public static final String LIBS_SERVER_LAUNCHER_NATIVE_AARCH64 = "libsServerLauncherNativeAarch64";
    public static final String LIBS_WINDOWS_SERVICE_CLI = "libsWindowsServiceCli";
    public static final String LIBS_PLUGIN_CLI = "libsPluginCli";
    public static final String LIBS_KEYSTORE_CLI = "libsKeystoreCli";
    public static final String LIBS_SECURITY_CLI = "libsSecurityCli";
    public static final String LIBS_GEOIP_CLI = "libsGeoIpCli";
    public static final String LIBS_ANSI_CONSOLE = "libsAnsiConsole";
    public static final String LIBS_NATIVE = "libsNative";
    public static final String LIBS_ENTITLEMENT_AGENT = "libsEntitlementAgent";
    public static final String LIBS_ENTITLEMENT_BRIDGE = "libsEntitlementBridge";

    private static final List<LibraryEntry> ENTRIES = List.of(
        new LibraryEntry(LIBS, ""),
        new LibraryEntry(LIBS_VERSION_CHECKER, "java-version-checker"),
        new LibraryEntry(LIBS_CLI_LAUNCHER, "cli-launcher"),
        new LibraryEntry(LIBS_SERVER_CLI, "tools/server-cli"),
        new LibraryEntry(LIBS_SERVER_LAUNCHER, "tools/server-launcher"),
        new LibraryEntry(LIBS_WINDOWS_SERVICE_CLI, "tools/windows-service-cli"),
        new LibraryEntry(LIBS_GEOIP_CLI, "tools/geoip-cli"),
        new LibraryEntry(LIBS_PLUGIN_CLI, "tools/plugin-cli"),
        new LibraryEntry(LIBS_KEYSTORE_CLI, "tools/keystore-cli"),
        new LibraryEntry(LIBS_SECURITY_CLI, "tools/security-cli"),
        new LibraryEntry(LIBS_ANSI_CONSOLE, "tools/ansi-console"),
        new LibraryEntry(LIBS_NATIVE, "platform"),
        new LibraryEntry(LIBS_ENTITLEMENT_AGENT, "entitlement-agent"),
        new LibraryEntry(LIBS_ENTITLEMENT_BRIDGE, "entitlement-bridge")
    );

    private DistributionLibraries() {}

    public static List<LibraryEntry> entries() {
        return ENTRIES;
    }

    public static List<String> linuxServerLauncherNativeConfigurations(String os, String architecture) {
        if ("linux".equals(os) == false) {
            return List.of();
        }
        if (architecture == null) {
            throw new IllegalArgumentException("Architecture must not be null for linux server-launcher native libraries");
        }
        return switch (architecture) {
            case "x64" -> List.of(LIBS_SERVER_LAUNCHER_NATIVE_X64);
            case "aarch64" -> List.of(LIBS_SERVER_LAUNCHER_NATIVE_AARCH64);
            default -> throw new IllegalArgumentException("Unsupported linux server-launcher architecture [" + architecture + "]");
        };
    }

    public static String platformInclude(String os, String architecture) {
        if (os == null) {
            return null;
        }
        return os + "-" + architecture + "/*";
    }

    public record LibraryEntry(String configuration, String targetDirectory) {}
}
