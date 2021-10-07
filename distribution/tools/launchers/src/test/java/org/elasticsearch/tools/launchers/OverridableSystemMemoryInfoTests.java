/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.elasticsearch.tools.launchers.SystemMemoryInfo.SystemMemoryInfoException;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class OverridableSystemMemoryInfoTests extends LaunchersTestCase {

    private static final long FALLBACK = -1L;

    public void testNoOptions() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(List.of(), fallbackSystemMemoryInfo());
        assertThat(memoryInfo.availableSystemMemory(), is(FALLBACK));
    }

    public void testNoOverrides() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(List.of("-Da=b", "-Dx=y"), fallbackSystemMemoryInfo());
        assertThat(memoryInfo.availableSystemMemory(), is(FALLBACK));
    }

    public void testValidSingleOverride() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(
            List.of("-Des.total_memory_bytes=123456789"),
            fallbackSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(123456789L));
    }

    public void testValidOverrideInList() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_bytes=987654321", "-Dx=y"),
            fallbackSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(987654321L));
    }

    public void testMultipleValidOverridesInList() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(
            List.of("-Des.total_memory_bytes=123456789", "-Da=b", "-Des.total_memory_bytes=987654321", "-Dx=y"),
            fallbackSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(987654321L));
    }

    public void testNegativeOverride() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_bytes=-123", "-Dx=y"),
            fallbackSystemMemoryInfo()
        );
        try {
            memoryInfo.availableSystemMemory();
            fail("expected to fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Negative memory size specified in [-Des.total_memory_bytes=-123]"));
        }
    }

    public void testUnparsableOverride() throws SystemMemoryInfoException {
        final SystemMemoryInfo memoryInfo = new OverridableSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_bytes=invalid", "-Dx=y"),
            fallbackSystemMemoryInfo()
        );
        try {
            memoryInfo.availableSystemMemory();
            fail("expected to fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Unable to parse number of bytes from [-Des.total_memory_bytes=invalid]"));
        }
    }

    private static SystemMemoryInfo fallbackSystemMemoryInfo() {
        return () -> FALLBACK;
    }
}
