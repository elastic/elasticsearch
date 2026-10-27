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
import static org.junit.Assert.assertThrows;

public class ModulePlatformsTests {

    @Test
    public void directoryUsesX86_64Nomenclature() {
        assertEquals("linux-x86_64", ModulePlatforms.directory("linux", "x64"));
        assertEquals("linux-aarch64", ModulePlatforms.directory("linux", "aarch64"));
        assertEquals("windows-x86_64", ModulePlatforms.directory("windows", "x64"));
        assertEquals("darwin-x86_64", ModulePlatforms.directory("darwin", "x64"));
        assertEquals("darwin-aarch64", ModulePlatforms.directory("darwin", "aarch64"));
        assertThrows(IllegalArgumentException.class, () -> ModulePlatforms.directory("windows", "aarch64"));
    }

    @Test
    public void excludedDirectoriesKeepOnlyTargetPlatform() {
        assertEquals(
            List.of("linux-aarch64", "windows-x86_64", "darwin-x86_64", "darwin-aarch64"),
            ModulePlatforms.excludedDirectories("linux", "x64")
        );
        assertEquals(
            List.of("linux-x86_64", "linux-aarch64", "windows-x86_64", "darwin-x86_64"),
            ModulePlatforms.excludedDirectories("darwin", "aarch64")
        );
    }

    @Test
    public void platformIndependentDistributionKeepsAllDirectories() {
        assertEquals(List.of(), ModulePlatforms.excludedDirectories(null, null));
    }
}
