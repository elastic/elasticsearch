/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class DistributionLibrariesTests {

    @Test
    public void entriesAreStable() {
        assertEquals(
            List.of(
                "libs->",
                "libsVersionChecker->java-version-checker",
                "libsCliLauncher->cli-launcher",
                "libsServerCli->tools/server-cli",
                "libsServerLauncher->tools/server-launcher",
                "libsWindowsServiceCli->tools/windows-service-cli",
                "libsGeoIpCli->tools/geoip-cli",
                "libsPluginCli->tools/plugin-cli",
                "libsKeystoreCli->tools/keystore-cli",
                "libsSecurityCli->tools/security-cli",
                "libsAnsiConsole->tools/ansi-console",
                "libsNative->platform",
                "libsEntitlementAgent->entitlement-agent",
                "libsEntitlementBridge->entitlement-bridge"
            ),
            DistributionLibraries.entries().stream().map(entry -> entry.configuration() + "->" + entry.targetDirectory()).toList()
        );
    }

    @Test
    public void linuxServerLauncherNativeConfigurations() {
        assertEquals(
            List.of(DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_X64),
            DistributionLibraries.linuxServerLauncherNativeConfigurations("linux", "x64")
        );
        assertEquals(
            List.of(DistributionLibraries.LIBS_SERVER_LAUNCHER_NATIVE_AARCH64),
            DistributionLibraries.linuxServerLauncherNativeConfigurations("linux", "aarch64")
        );
        IllegalArgumentException nullArchitecture = assertThrows(
            IllegalArgumentException.class,
            () -> DistributionLibraries.linuxServerLauncherNativeConfigurations("linux", null)
        );
        assertEquals(
            "Architecture must not be null for linux server-launcher native libraries",
            nullArchitecture.getMessage()
        );

        IllegalArgumentException unsupportedArchitecture = assertThrows(
            IllegalArgumentException.class,
            () -> DistributionLibraries.linuxServerLauncherNativeConfigurations("linux", "ppc64le")
        );
        assertEquals("Unsupported linux server-launcher architecture [ppc64le]", unsupportedArchitecture.getMessage());
        assertEquals(List.of(), DistributionLibraries.linuxServerLauncherNativeConfigurations("darwin", "x64"));
    }

    @Test
    public void platformIncludePattern() {
        assertEquals("linux-x64/*", DistributionLibraries.platformInclude("linux", "x64"));
        assertEquals("darwin-aarch64/*", DistributionLibraries.platformInclude("darwin", "aarch64"));
        assertNull(DistributionLibraries.platformInclude(null, "x64"));
    }
}
