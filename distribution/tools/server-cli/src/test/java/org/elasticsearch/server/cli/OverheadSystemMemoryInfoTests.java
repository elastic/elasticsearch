/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.server.cli.OverheadSystemMemoryInfo.SERVER_CLI_OVERHEAD;
import static org.hamcrest.Matchers.is;

public class OverheadSystemMemoryInfoTests extends ESTestCase {

    private static final long TRUE_SYSTEM_MEMORY = 1024 * 1024 * 1024L;

    public void testNoOptions() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(List.of(), delegateSystemMemoryInfo());
        assertThat(memoryInfo.availableSystemMemory(), is(TRUE_SYSTEM_MEMORY - SERVER_CLI_OVERHEAD));
    }

    public void testNoOverrides() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(List.of("-Da=b", "-Dx=y"), delegateSystemMemoryInfo());
        assertThat(memoryInfo.availableSystemMemory(), is(TRUE_SYSTEM_MEMORY - SERVER_CLI_OVERHEAD));
    }

    public void testValidSingleOverride() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(
            List.of("-Des.total_memory_overhead_bytes=50000"),
            delegateSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(TRUE_SYSTEM_MEMORY - 50000));
    }

    public void testValidOverrideInList() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_overhead_bytes=50000", "-Dx=y"),
            delegateSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(TRUE_SYSTEM_MEMORY - 50000));
    }

    public void testMultipleValidOverridesInList() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(
            List.of("-Des.total_memory_overhead_bytes=50000", "-Da=b", "-Des.total_memory_overhead_bytes=100000", "-Dx=y"),
            delegateSystemMemoryInfo()
        );
        assertThat(memoryInfo.availableSystemMemory(), is(TRUE_SYSTEM_MEMORY - 100000));
    }

    public void testNegativeOverride() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_overhead_bytes=-123", "-Dx=y"),
            delegateSystemMemoryInfo()
        );
        try {
            memoryInfo.availableSystemMemory();
            fail("expected to fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Negative bytes size specified in [-Des.total_memory_overhead_bytes=-123]"));
        }
    }

    public void testUnparsableOverride() {
        final SystemMemoryInfo memoryInfo = new OverheadSystemMemoryInfo(
            List.of("-Da=b", "-Des.total_memory_overhead_bytes=invalid", "-Dx=y"),
            delegateSystemMemoryInfo()
        );
        try {
            memoryInfo.availableSystemMemory();
            fail("expected to fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Unable to parse number of bytes from [-Des.total_memory_overhead_bytes=invalid]"));
        }
    }

    private static SystemMemoryInfo delegateSystemMemoryInfo() {
        return () -> TRUE_SYSTEM_MEMORY;
    }
}
