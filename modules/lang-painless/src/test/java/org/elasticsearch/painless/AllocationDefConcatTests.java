/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

/**
 * End-to-end tests for allocation charging on a {@code def}-dispatched {@code +} that resolves to a string concatenation. The
 * operand types are unknown at compile time, so the charge is emitted at the def {@code +} site and applied at runtime only when
 * an operand is actually a {@link String} (via {@link AllocationGuard#checkDefConcatAlloc}); numeric {@code def + def} is not
 * charged, matching the PR 7.5 decision to leave rebox untracked.
 */
public class AllocationDefConcatTests extends AllocationTestCase {

    private static long concatBytes(Object left, Object right) {
        return AllocSizes.STRING_CONCAT_RESULT_OVERHEAD + AllocSizes.stringConcatOperandBytes(left) + AllocSizes.stringConcatOperandBytes(
            right
        );
    }

    public void testDefConcatCharged() {
        assertEquals(concatBytes("hello", "world"), allocatedBytes("def a = \"hello\"; def b = \"world\"; def c = a + b; return \"x\";"));
    }

    public void testDefConcatChargeVariesWithLength() {
        assertEquals(
            concatBytes("hello", "worldworld"),
            allocatedBytes("def a = \"hello\"; def b = \"worldworld\"; def c = a + b; return \"x\";")
        );
    }

    public void testDefConcatTripsLimit() {
        assertTripsLimit("def a = \"hello\"; def b = \"world\"; def c = a + b; return \"x\";");
    }

    public void testStringPlusDefConcatCharged() {
        // Left operand is statically typed String, right is def — the whole op is still def-dispatched.
        assertEquals(concatBytes("x", "yy"), allocatedBytes("String a = \"x\"; def b = \"yy\"; def c = a + b; return \"z\";"));
    }

    public void testDefConcatWithPrimitiveOperandCharged() {
        // A primitive operand is boxed before the runtime check; a non-String operand contributes the conservative constant.
        assertEquals(concatBytes("n=", Integer.valueOf(5)), allocatedBytes("def a = \"n=\"; def c = a + 5; return \"z\";"));
    }

    public void testDefConcatNullOperandCharged() {
        // A null operand stringifies to "null" (8 bytes); the concat is still detected via the non-null String operand.
        assertEquals(concatBytes(null, "x"), allocatedBytes("def a = null; def b = \"x\"; def c = a + b; return \"z\";"));
    }

    public void testDefNumericAddNotCharged() {
        // A numeric def '+' must charge nothing beyond the operands' own autoboxing: the add itself adds zero (rebox untracked).
        long withoutAdd = allocatedBytes("def a = 5; def b = 6; return \"x\";");
        long withAdd = allocatedBytes("def a = 5; def b = 6; def c = a + b; return \"x\";");
        assertEquals(withoutAdd, withAdd);
    }

    public void testDefConcatChargesEachIteration() {
        long perConcat = concatBytes("ab", "cd");
        long total = allocatedBytes("""
            def a = "ab";
            def b = "cd";
            for (int i = 0; i < 3; ++i) {
                def c = a + b;
            }
            return "x";
            """);
        assertEquals(3 * perConcat, total);
    }
}
