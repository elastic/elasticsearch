/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Zero-overhead guard for allocation tracking: a script compiled with tracking off (the default {@code -1b}) must produce
 * bytecode with no trace of the counter, and turning tracking on must add the {@code $allocBytes} field and its overrides.
 * Grows with each allocation-tracking PR to cover the new emission sites.
 */
public class AllocationDisabledBytecodeTests extends ScriptTestCase {

    private static String bytecode(String source, long maxAllocationBytes) {
        CompilerSettings settings = new CompilerSettings();
        settings.setMaxAllocationBytes(maxAllocationBytes);
        return Debugger.toString(PainlessTestScript.class, source, settings, PAINLESS_BASE_WHITELIST);
    }

    public void testNoCounterBytecodeWhenDisabled() {
        // A script with an allocation site must still be bit-clean of tracking bytecode when the limit is off.
        String asm = bytecode("int[] a = new int[] {1, 2, 3}; return 1;", -1L);
        assertThat(asm, not(containsString("$allocBytes")));
        assertThat(asm, not(containsString("$incAllocBytes")));
        assertThat(asm, not(containsString("getAllocBytes")));
        assertThat(asm, not(containsString("$checkAllocBytes")));
        assertThat(asm, not(containsString("AllocationGuard")));
    }

    public void testCounterBytecodePresentWhenEnabled() {
        String asm = bytecode("return 1;", 1024 * 1024L);
        assertThat(asm, containsString("$allocBytes"));
        assertThat(asm, containsString("$incAllocBytes"));
        assertThat(asm, containsString("getAllocBytes"));
        assertThat(asm, containsString("$checkAllocBytes"));
    }

    public void testPreCheckEmittedAtAllocationSiteWhenEnabled() {
        // The allocation site invokes the script's $checkAllocBytes before allocating.
        String asm = bytecode("int[] a = new int[] {1, 2, 3}; return 1;", 1024 * 1024L);
        assertThat(asm, containsString("$checkAllocBytes"));
    }

    public void testPreCheckCallsAllocationGuardOnBreachPath() {
        // The generated $checkAllocBytes delegates to AllocationGuard on the breach path.
        String asm = bytecode("int[] a = new int[] {1, 2, 3}; return 1;", 1024 * 1024L);
        assertThat(asm, containsString("AllocationGuard"));
    }

    public void testNoCounterBytecodeForRuntimeArraysWhenDisabled() {
        // Runtime-sized arrays (1-D and multi-dim) take a separate emission path; it too must be clean when off.
        String asm = bytecode("int n = 3; int[][] a = new int[n][n]; return 1;", -1L);
        assertThat(asm, not(containsString("$checkAllocBytes")));
        assertThat(asm, not(containsString("AllocationGuard")));
    }

    public void testPreCheckEmittedForRuntimeArrayWhenEnabled() {
        String asm = bytecode("int n = 3; int[] a = new int[n]; return 1;", 1024 * 1024L);
        assertThat(asm, containsString("$checkAllocBytes"));
    }

    public void testNoCounterBytecodeForStringConcatWhenDisabled() {
        // String concat takes its own emission path; it too must be clean when tracking is off.
        String asm = bytecode("String a = 'ab'; String b = 'cd'; return a + b;", -1L);
        assertThat(asm, not(containsString("$checkAllocBytes")));
        assertThat(asm, not(containsString("AllocationGuard")));
    }

    public void testPreCheckEmittedForStringConcatWhenEnabled() {
        String asm = bytecode("String a = 'ab'; String b = 'cd'; return a + b;", 1024 * 1024L);
        assertThat(asm, containsString("$checkAllocBytes"));
    }
}
