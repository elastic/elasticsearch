/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.startsWith;

public class DefEncodingTests extends ESTestCase {

    public void testParse() {
        assertEquals(
            new Def.Encoding(true, false, "java.util.Comparator", "thenComparing", 1, false),
            new Def.Encoding("Sfjava.util.Comparator.thenComparing,1")
        );

        assertEquals(
            new Def.Encoding(false, false, "ft0", "augmentInjectMultiTimesX", 1, false),
            new Def.Encoding("Dfft0.augmentInjectMultiTimesX,1")
        );

        assertEquals(new Def.Encoding(false, false, "x", "concat", 1, false), new Def.Encoding("Dfx.concat,1"));

        assertEquals(
            new Def.Encoding(true, false, "java.lang.StringBuilder", "setLength", 1, false),
            new Def.Encoding("Sfjava.lang.StringBuilder.setLength,1")
        );

        assertEquals(
            new Def.Encoding(true, false, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0, false),
            new Def.Encoding("Sforg.elasticsearch.painless.FeatureTestObject.overloadedStatic,0")
        );

        assertEquals(
            new Def.Encoding(true, false, "this", "lambda$synthetic$0", 1, false),
            new Def.Encoding("Sfthis.lambda$synthetic$0,1")
        );

        assertEquals(new Def.Encoding(true, true, "this", "lambda$synthetic$0", 2, false), new Def.Encoding("Stthis.lambda$synthetic$0,2"));

        assertEquals(new Def.Encoding(true, true, "this", "mycompare", 0, false), new Def.Encoding("Stthis.mycompare,0"));

        // Non-charging reference with needsInstance=true on a non-'this' symbol: valid and distinct from a charging one —
        // chargesAllocation is a separate flag, not inferred from needsInstance+symbol.
        assertEquals(
            new Def.Encoding(true, true, "java.lang.String", "toUpperCase", 0, false),
            new Def.Encoding("Stjava.lang.String.toUpperCase,0")
        );

        // Charging static reference (external @allocates target): needsInstance=true captures the script, chargesAllocation
        // adds the trailing 'c'. Distinct encoding from the non-charging needsInstance case above.
        assertEquals(
            new Def.Encoding(true, true, "java.lang.String", "toUpperCase", 0, true),
            new Def.Encoding("Stjava.lang.String.toUpperCase,0c")
        );
        assertTrue(new Def.Encoding("Stjava.lang.String.toUpperCase,0c").chargesAllocation);

        // Charging dynamic reference (PR 8.6): trailing 'c' after numCaptures marks a def-receiver bound ref that charges.
        // The script is a trailing capture, so numCaptures counts it (here: receiver + script = 2).
        Def.Encoding charging = new Def.Encoding(false, false, "s", "concat", 2, true);
        assertEquals("Dfs.concat,2c", charging.toString());
        assertEquals(charging, new Def.Encoding("Dfs.concat,2c"));
        assertTrue(charging.chargesAllocation);
        assertEquals(2, charging.numCaptures);

        // A plain (non-charging) encoding parses with chargesAllocation=false and no 'c' in its string.
        Def.Encoding plain = new Def.Encoding("Dfs.concat,2");
        assertFalse(plain.chargesAllocation);
        assertEquals("Dfs.concat,2", plain.toString());
        assertNotEquals(charging, plain);
    }

    public void testValidate() {
        IllegalArgumentException expected = expectThrows(
            IllegalArgumentException.class,
            () -> new Def.Encoding(false, false, "this", "myMethod", 0, false)
        );

        assertThat(expected.getMessage(), startsWith("Def.Encoding must be static if symbol is 'this', encoding [Dfthis.myMethod,0]"));

        // needsInstance on a non-'this' symbol is allowed — allocation tracking uses it to capture the script for an
        // external @allocates reference (the charging bootstrap drops the capture). This must not throw.
        new Def.Encoding(true, true, "org.elasticsearch.painless.FeatureTestObject", "overloadedStatic", 0, false);

        expected = expectThrows(IllegalArgumentException.class, () -> new Def.Encoding(false, false, "x", "", 1, false));

        assertThat(expected.getMessage(), startsWith("methodName must be non-empty, encoding [Dfx.,1]"));
    }
}
