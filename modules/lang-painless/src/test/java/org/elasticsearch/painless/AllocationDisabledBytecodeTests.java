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
        String asm = bytecode("return 1;", -1L);
        assertThat(asm, not(containsString("$allocBytes")));
        assertThat(asm, not(containsString("$incAllocBytes")));
        assertThat(asm, not(containsString("getAllocBytes")));
    }

    public void testCounterBytecodePresentWhenEnabled() {
        String asm = bytecode("return 1;", 1024 * 1024L);
        assertThat(asm, containsString("$allocBytes"));
        assertThat(asm, containsString("$incAllocBytes"));
        assertThat(asm, containsString("getAllocBytes"));
    }
}
