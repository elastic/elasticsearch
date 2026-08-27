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
 * Tests for static-type string-concatenation allocation pre-checks. Each concat charges
 * {@link AllocSizes#STRING_CONCAT_RESULT_OVERHEAD} plus a per-operand byte bound: the real UTF-16 length for {@code String}
 * operands, a max-length constant for primitives, and a runtime {@code instanceof String} bound for {@code def}/{@link Object}
 * operands. Operands are kept non-constant (read from locals) so the concat is not folded away at compile time.
 */
public class StringConcatSizingTests extends AllocationTestCase {

    /** Expected charge for a concat whose operands contribute {@code operandBytes}. */
    private static long concatBytes(long... operandBytes) {
        long total = AllocSizes.STRING_CONCAT_RESULT_OVERHEAD;
        for (long b : operandBytes) {
            total += b;
        }
        return total;
    }

    public void testTwoStringsChargedExactly() {
        // 32 overhead + 2*2 ("ab") + 3*2 ("cde") = 42.
        assertEquals(concatBytes(4, 6), allocatedBytes("String a = 'ab'; String b = 'cde'; return a + b;"));
    }

    public void testStringPlusIntCharged() {
        // 32 + 4 ("ab") + 22 (int max length) = 58.
        assertEquals(
            concatBytes(4, AllocSizes.stringConcatPrimitiveBytes(int.class)),
            allocatedBytes("String a = 'ab'; int n = 5; return a + n;")
        );
    }

    public void testStringPlusLongCharged() {
        assertEquals(
            concatBytes(4, AllocSizes.stringConcatPrimitiveBytes(long.class)),
            allocatedBytes("String a = 'ab'; long n = 5L; return a + n;")
        );
    }

    public void testStringPlusCharCharged() {
        // char concat (not arithmetic): 32 + 4 + 2 = 38. The (char) cast is required since 'x' is a String literal in Painless.
        assertEquals(
            concatBytes(4, AllocSizes.stringConcatPrimitiveBytes(char.class)),
            allocatedBytes("String a = 'ab'; char c = (char)'x'; return a + c;")
        );
    }

    public void testNullStringOperandCharged() {
        // A null String operand stringifies to "null" => 8 bytes.
        assertEquals(
            concatBytes(4, AllocSizes.NULL_STRING_CONCAT_BYTES),
            allocatedBytes("String a = 'ab'; String b = null; return a + b;")
        );
    }

    public void testDefStringOperandUsesRealLength() {
        // A def operand holding a String at runtime contributes its real UTF-16 length (4 bytes for "cd").
        assertEquals(concatBytes(4, 4), allocatedBytes("String a = 'ab'; def b = 'cd'; return a + b;"));
    }

    public void testCharArithmeticNotChargedAsConcat() {
        // char + char is integer arithmetic, not concat: no StringConcatenationNode, so nothing is charged. The result is
        // kept in an int local (not returned) so it is not autoboxed, and the script returns a constant-pool String.
        assertEquals(0L, allocatedBytes("char a = (char)'a'; char b = (char)'b'; int r = a + b; return 'x';"));
    }

    public void testConcatTripsLimit() {
        assertTripsLimit("String a = 'ab'; String b = 'cde'; return a + b;");
    }
}
