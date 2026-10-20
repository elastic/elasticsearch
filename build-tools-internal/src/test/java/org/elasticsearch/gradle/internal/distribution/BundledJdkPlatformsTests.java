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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BundledJdkPlatformsTests {

    @Test
    public void linuxSupportsX64AndAarch64() {
        assertEquals(List.of("x64", "aarch64"), BundledJdkPlatforms.architecturesFor("linux", 25));
        assertEquals(List.of("x64", "aarch64"), BundledJdkPlatforms.architecturesFor("linux", 27));
    }

    @Test
    public void windowsSupportsX64Only() {
        assertEquals(List.of("x64"), BundledJdkPlatforms.architecturesFor("windows", 25));
        assertEquals(List.of("x64"), BundledJdkPlatforms.architecturesFor("windows", 27));
    }

    @Test
    public void darwinX64DroppedWithJdk27() {
        assertEquals(List.of("x64", "aarch64"), BundledJdkPlatforms.architecturesFor("darwin", 26));
        assertEquals(List.of("aarch64"), BundledJdkPlatforms.architecturesFor("darwin", 27));
        assertEquals(List.of("aarch64"), BundledJdkPlatforms.architecturesFor("darwin", 28));

        assertTrue(BundledJdkPlatforms.isSupported("darwin", "x64", 26));
        assertFalse(BundledJdkPlatforms.isSupported("darwin", "x64", 27));
        assertTrue(BundledJdkPlatforms.isSupported("darwin", "aarch64", 27));
    }

    @Test
    public void unknownPlatformIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> BundledJdkPlatforms.architecturesFor("solaris", 27));
    }

    @Test
    public void jdkName() {
        assertEquals("bundled_linux_x64", BundledJdkPlatforms.jdkName("linux", "x64"));
        assertEquals("bundled_darwin_aarch64", BundledJdkPlatforms.jdkName("darwin", "aarch64"));
    }

    @Test
    public void modulePlatformDirectoryUsesX86_64Nomenclature() {
        assertEquals("linux-x86_64", BundledJdkPlatforms.modulePlatformDirectory("linux", "x64"));
        assertEquals("linux-aarch64", BundledJdkPlatforms.modulePlatformDirectory("linux", "aarch64"));
        assertEquals("windows-x86_64", BundledJdkPlatforms.modulePlatformDirectory("windows", "x64"));
        assertEquals("darwin-x86_64", BundledJdkPlatforms.modulePlatformDirectory("darwin", "x64"));
        assertEquals("darwin-aarch64", BundledJdkPlatforms.modulePlatformDirectory("darwin", "aarch64"));
        assertThrows(IllegalArgumentException.class, () -> BundledJdkPlatforms.modulePlatformDirectory("windows", "aarch64"));
    }

    @Test
    public void excludedModulePlatformDirectoriesKeepOnlyTargetPlatform() {
        assertEquals(
            List.of("linux-aarch64", "windows-x86_64", "darwin-x86_64", "darwin-aarch64"),
            BundledJdkPlatforms.excludedModulePlatformDirectories("linux", "x64")
        );
        assertEquals(
            List.of("linux-x86_64", "linux-aarch64", "windows-x86_64", "darwin-x86_64"),
            BundledJdkPlatforms.excludedModulePlatformDirectories("darwin", "aarch64")
        );
    }

    @Test
    public void platformIndependentDistributionKeepsAllModulePlatformDirectories() {
        assertEquals(List.of(), BundledJdkPlatforms.excludedModulePlatformDirectories(null, null));
    }
}
