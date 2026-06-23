/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for runtime-sized array allocation pre-checks: {@code new T[n]} and multi-dimensional
 * {@code new T[d0][d1]...}. The size is computed as {@code pad8(ARRAY_HEADER + fieldSize(innermostType) * totalElements)}
 * before the allocating instruction, so an oversized array trips the limit (an uncatchable {@link PainlessError},
 * surfaced as {@link ScriptException}) before the JVM attempts the allocation.
 */
public class AllocationArrayPreCheckTests extends ESTestCase {

    private static final String LIMIT_KEY = "script.painless.max_allocation_bytes.context." + PainlessTestScript.CONTEXT.name + ".limit";

    private static PainlessTestScript compile(String source, String limit) {
        Settings settings = Settings.builder().put(LIMIT_KEY, limit).build();
        PainlessScriptEngine engine = new PainlessScriptEngine(settings, scriptContexts());
        PainlessTestScript.Factory factory = engine.compile("test", source, PainlessTestScript.CONTEXT, Map.of());
        return factory.newInstance(Map.of());
    }

    /** Runs {@code source} under a 1mb limit and returns the running allocation total afterwards. */
    private static long allocatedBytes(String source) {
        PainlessTestScript script = compile(source, "1mb");
        script.execute();
        return ((PainlessScript) script).getAllocBytes();
    }

    /** Asserts that running {@code source} under a 1b limit trips the allocation limit. */
    private static void assertTripsLimit(String source) {
        PainlessTestScript script = compile(source, "1b");
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error for [" + source + "], but got: " + e, e);
    }

    public void testOneDimConstantLengthCharged() {
        // new int[10] => pad8(16 + 4*10) = 56 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 10), allocatedBytes("int[] a = new int[10]; return \"x\";"));
    }

    public void testOneDimVariableLengthCharged() {
        // new long[n] with n=4 => pad8(16 + 8*4) = 48 bytes; the length is loaded from a local at runtime.
        assertEquals(AllocSizes.arraySize(long.class, 4), allocatedBytes("int n = 4; long[] a = new long[n]; return \"x\";"));
    }

    public void testOneDimByteArray() {
        // new byte[10] => pad8(16 + 1*10) = 32 bytes; exercises fieldSize returning 1 for byte.
        assertEquals(AllocSizes.arraySize(byte.class, 10), allocatedBytes("byte[] b = new byte[10]; return \"x\";"));
    }

    public void testOneDimZeroLength() {
        // new int[0] => pad8(16 + 4*0) = 16 bytes (just the array header).
        assertEquals(AllocSizes.arraySize(int.class, 0), allocatedBytes("int[] a = new int[0]; return \"x\";"));
    }

    public void testOneDimPreemptsHugeAllocation() {
        // The 2GB array would OOM if actually allocated; the pre-check trips first.
        assertTripsLimit("byte[] b = new byte[Integer.MAX_VALUE]; return \"x\";");
    }

    public void testTwoDimChargedExactly() {
        // new int[2][3] => pad8(16 + 4 * (2*3)) = pad8(40) = 40 bytes.
        assertEquals(AllocSizes.arraySize(int.class, 2 * 3), allocatedBytes("int[][] a = new int[2][3]; return \"x\";"));
    }

    public void testThreeDimChargedExactly() {
        assertEquals(AllocSizes.arraySize(int.class, 2 * 2 * 2), allocatedBytes("int[][][] a = new int[2][2][2]; return \"x\";"));
    }

    public void testMultiDimPreemptsHugeAllocation() {
        // new int[10][1_000_000_000]: product overflows int but not long; pre-check trips before MULTIANEWARRAY.
        assertTripsLimit("int[][] a = new int[10][1000000000]; return \"x\";");
    }

    public void testHighDimensionalArrayCharged() {
        // 100-dim array: new int[2][1][1]...[1] — product = 2, verifies the dim-product loop handles many dims.
        int dims = 100;
        StringBuilder type = new StringBuilder("int");
        for (int i = 0; i < dims; i++)
            type.append("[]");
        StringBuilder src = new StringBuilder(type).append(" a = new int[2]");
        for (int i = 1; i < dims; i++)
            src.append("[1]");
        src.append("; return \"x\";");
        assertEquals(AllocSizes.arraySize(int.class, 2), allocatedBytes(src.toString()));
    }

    public void testHighDimensionalArrayPreemptsHugeAllocation() {
        // 100-dim array with a large outer dimension trips the limit before MULTIANEWARRAY.
        int dims = 100;
        StringBuilder type = new StringBuilder("int");
        for (int i = 0; i < dims; i++)
            type.append("[]");
        StringBuilder src = new StringBuilder(type).append(" a = new int[Integer.MAX_VALUE]");
        for (int i = 1; i < dims; i++)
            src.append("[1]");
        src.append("; return \"x\";");
        assertTripsLimit(src.toString());
    }

    public void testTwoDimVariableLengthsCharged() {
        assertEquals(
            AllocSizes.arraySize(int.class, 3 * 4),
            allocatedBytes("int x = 3; int y = 4; int[][] a = new int[x][y]; return \"x\";")
        );
    }

    private static Map<ScriptContext<?>, List<Whitelist>> scriptContexts() {
        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PainlessPlugin.baseWhiteList());
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.painless.test"));
        contexts.put(PainlessTestScript.CONTEXT, whitelists);
        return contexts;
    }
}
