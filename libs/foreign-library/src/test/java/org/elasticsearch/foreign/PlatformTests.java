/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import junit.framework.TestCase;

/**
 * Tests for {@link Platform#resolve(String, String)}.
 *
 * <p>These tests use JUnit 3 ({@code junit.framework.TestCase}) directly rather than
 * {@code ESTestCase} to keep the foreign-library module's test dependencies minimal — the
 * test framework pulls in large transitive dependencies that cannot be resolved in
 * restricted build environments.
 */
public class PlatformTests extends TestCase {

    public void testLinuxX64AmdVariant() {
        assertEquals(Platform.LINUX_X64, Platform.resolve("Linux", "amd64"));
    }

    public void testLinuxX64X86Variant() {
        assertEquals(Platform.LINUX_X64, Platform.resolve("Linux", "x86_64"));
    }

    public void testLinuxAarch64() {
        assertEquals(Platform.LINUX_AARCH64, Platform.resolve("Linux", "aarch64"));
    }

    public void testDarwinX64AmdVariant() {
        assertEquals(Platform.DARWIN_X64, Platform.resolve("Mac OS X", "amd64"));
    }

    public void testDarwinX64X86Variant() {
        assertEquals(Platform.DARWIN_X64, Platform.resolve("Mac OS X", "x86_64"));
    }

    public void testDarwinAarch64() {
        assertEquals(Platform.DARWIN_AARCH64, Platform.resolve("Mac OS X", "aarch64"));
    }

    public void testWindowsX64() {
        assertEquals(Platform.WINDOWS_X64, Platform.resolve("Windows 10", "amd64"));
    }

    public void testWindowsX64X86Variant() {
        assertEquals(Platform.WINDOWS_X64, Platform.resolve("Windows Server 2019", "x86_64"));
    }

    public void testUnknownOsThrows() {
        try {
            Platform.resolve("SunOS", "amd64");
            fail("Expected IllegalStateException for unknown OS");
        } catch (IllegalStateException e) {
            assertTrue("Error message should mention the OS name", e.getMessage().contains("SunOS"));
        }
    }

    public void testUnknownArchThrows() {
        try {
            Platform.resolve("Linux", "sparc");
            fail("Expected IllegalStateException for unknown arch");
        } catch (IllegalStateException e) {
            assertTrue("Error message should mention the arch name", e.getMessage().contains("sparc"));
        }
    }

    public void testUnsupportedCombinationThrows() {
        // Windows aarch64 is not a supported Elasticsearch platform
        try {
            Platform.resolve("Windows 10", "aarch64");
            fail("Expected IllegalStateException for unsupported Windows/aarch64 combination");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testNullOsNameThrows() {
        try {
            Platform.resolve(null, "amd64");
            fail("Expected IllegalStateException for null os.name");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
        }
    }

    public void testNullArchNameThrows() {
        try {
            Platform.resolve("Linux", null);
            fail("Expected IllegalStateException for null os.arch");
        } catch (IllegalStateException e) {
            assertNotNull(e.getMessage());
        }
    }
}
