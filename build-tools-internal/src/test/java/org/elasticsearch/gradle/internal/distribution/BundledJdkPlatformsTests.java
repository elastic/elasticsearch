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
}
