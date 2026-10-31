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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class DistributionLibrariesTests {

    @Test
    public void linuxServerLauncherNativeConfiguration() {
        assertEquals(
            "libsServerLauncherNativeX64",
            DistributionLibraries.linuxServerLauncherNativeConfiguration(
                "linux",
                "x64",
                "libsServerLauncherNativeX64",
                "libsServerLauncherNativeAarch64"
            )
        );
        assertEquals(
            "libsServerLauncherNativeAarch64",
            DistributionLibraries.linuxServerLauncherNativeConfiguration(
                "linux",
                "aarch64",
                "libsServerLauncherNativeX64",
                "libsServerLauncherNativeAarch64"
            )
        );
        IllegalArgumentException nullArchitecture = assertThrows(
            IllegalArgumentException.class,
            () -> DistributionLibraries.linuxServerLauncherNativeConfiguration(
                "linux",
                null,
                "libsServerLauncherNativeX64",
                "libsServerLauncherNativeAarch64"
            )
        );
        assertEquals(
            "Architecture must not be null for linux server-launcher native libraries",
            nullArchitecture.getMessage()
        );

        IllegalArgumentException unsupportedArchitecture = assertThrows(
            IllegalArgumentException.class,
            () -> DistributionLibraries.linuxServerLauncherNativeConfiguration(
                "linux",
                "ppc64le",
                "libsServerLauncherNativeX64",
                "libsServerLauncherNativeAarch64"
            )
        );
        assertEquals("Unsupported linux server-launcher architecture [ppc64le]", unsupportedArchitecture.getMessage());
        assertNull(
            DistributionLibraries.linuxServerLauncherNativeConfiguration(
                "darwin",
                "x64",
                "libsServerLauncherNativeX64",
                "libsServerLauncherNativeAarch64"
            )
        );
    }

    @Test
    public void platformIncludePattern() {
        assertEquals("linux-x64/*", DistributionLibraries.platformInclude("linux", "x64"));
        assertEquals("darwin-aarch64/*", DistributionLibraries.platformInclude("darwin", "aarch64"));
        assertNull(DistributionLibraries.platformInclude(null, "x64"));
    }
}
