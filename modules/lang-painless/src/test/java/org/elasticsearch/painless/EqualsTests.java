/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static java.util.Collections.singletonMap;

public class EqualsTests extends ScriptTestCase {

    public void testTypesEquals() {
        assertEquals(true, exec("return false === false;"));
        assertEquals(false, exec("boolean x = false; boolean y = true; return x === y;"));
        assertEquals(true, exec("boolean x = false; boolean y = false; return x === y;"));
        assertEquals(false, exec("return (byte)3 === (byte)4;"));
        assertEquals(true, exec("byte x = 3; byte y = 3; return x === y;"));
        assertEquals(false, exec("return (char)3 === (char)4;"));
        assertEquals(true, exec("char x = 3; char y = 3; return x === y;"));
        assertEquals(false, exec("return (short)3 === (short)4;"));
        assertEquals(true, exec("short x = 3; short y = 3; return x === y;"));
        assertEquals(false, exec("return (int)3 === (int)4;"));
        assertEquals(true, exec("int x = 3; int y = 3; return x === y;"));
        assertEquals(false, exec("return (long)3 === (long)4;"));
        assertEquals(true, exec("long x = 3; long y = 3; return x === y;"));
        assertEquals(false, exec("return (float)3 === (float)4;"));
        assertEquals(true, exec("float x = 3; float y = 3; return x === y;"));
        assertEquals(false, exec("return (double)3 === (double)4;"));
        assertEquals(true, exec("double x = 3; double y = 3; return x === y;"));

        assertEquals(true, exec("return false == false;"));
        assertEquals(false, exec("boolean x = false; boolean y = true; return x == y;"));
        assertEquals(true, exec("boolean x = false; boolean y = false; return x == y;"));
        assertEquals(false, exec("return (byte)3 == (byte)4;"));
        assertEquals(true, exec("byte x = 3; byte y = 3; return x == y;"));
        assertEquals(false, exec("return (char)3 == (char)4;"));
        assertEquals(true, exec("char x = 3; char y = 3; return x == y;"));
        assertEquals(false, exec("return (short)3 == (short)4;"));
        assertEquals(true, exec("short x = 3; short y = 3; return x == y;"));
        assertEquals(false, exec("return (int)3 == (int)4;"));
        assertEquals(true, exec("int x = 3; int y = 3; return x == y;"));
        assertEquals(false, exec("return (long)3 == (long)4;"));
        assertEquals(true, exec("long x = 3; long y = 3; return x == y;"));
        assertEquals(false, exec("return (float)3 == (float)4;"));
        assertEquals(true, exec("float x = 3; float y = 3; return x == y;"));
        assertEquals(false, exec("return (double)3 == (double)4;"));
        assertEquals(true, exec("double x = 3; double y = 3; return x == y;"));
    }

    public void testTypesNotEquals() {
        assertEquals(false, exec("return true !== true;"));
        assertEquals(true, exec("boolean x = true; boolean y = false; return x !== y;"));
        assertEquals(false, exec("boolean x = false; boolean y = false; return x !== y;"));
        assertEquals(true, exec("return (byte)3 !== (byte)4;"));
        assertEquals(false, exec("byte x = 3; byte y = 3; return x !== y;"));
        assertEquals(true, exec("return (char)3 !== (char)4;"));
        assertEquals(false, exec("char x = 3; char y = 3; return x !== y;"));
        assertEquals(true, exec("return (short)3 !== (short)4;"));
        assertEquals(false, exec("short x = 3; short y = 3; return x !== y;"));
        assertEquals(true, exec("return (int)3 !== (int)4;"));
        assertEquals(false, exec("int x = 3; int y = 3; return x !== y;"));
        assertEquals(true, exec("return (long)3 !== (long)4;"));
        assertEquals(false, exec("long x = 3; long y = 3; return x !== y;"));
        assertEquals(true, exec("return (float)3 !== (float)4;"));
        assertEquals(false, exec("float x = 3; float y = 3; return x !== y;"));
        assertEquals(true, exec("return (double)3 !== (double)4;"));
        assertEquals(false, exec("double x = 3; double y = 3; return x !== y;"));

        assertEquals(false, exec("return true != true;"));
        assertEquals(true, exec("boolean x = true; boolean y = false; return x != y;"));
        assertEquals(false, exec("boolean x = false; boolean y = false; return x != y;"));
        assertEquals(true, exec("return (byte)3 != (byte)4;"));
        assertEquals(false, exec("byte x = 3; byte y = 3; return x != y;"));
        assertEquals(true, exec("return (char)3 != (char)4;"));
        assertEquals(false, exec("char x = 3; char y = 3; return x != y;"));
        assertEquals(true, exec("return (short)3 != (short)4;"));
        assertEquals(false, exec("short x = 3; short y = 3; return x != y;"));
        assertEquals(true, exec("return (int)3 != (int)4;"));
        assertEquals(false, exec("int x = 3; int y = 3; return x != y;"));
        assertEquals(true, exec("return (long)3 != (long)4;"));
        assertEquals(false, exec("long x = 3; long y = 3; return x != y;"));
        assertEquals(true, exec("return (float)3 != (float)4;"));
        assertEquals(false, exec("float x = 3; float y = 3; return x != y;"));
        assertEquals(true, exec("return (double)3 != (double)4;"));
        assertEquals(false, exec("double x = 3; double y = 3; return x != y;"));
    }

    public void testEquals() {
        assertEquals(true, exec("return 3 == 3;"));
        assertEquals(false, exec("int x = 4; int y = 5; x == y"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = x; return x == y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = x; return x === y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = new int[1]; return x == y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = new int[1]; return x === y;"));
        assertEquals(false, exec("Map x = new HashMap(); List y = new ArrayList(); return x == y;"));
        assertEquals(false, exec("Map x = new HashMap(); List y = new ArrayList(); return x === y;"));
    }

    public void testNotEquals() {
        assertEquals(false, exec("return 3 != 3;"));
        assertEquals(true, exec("int x = 4; int y = 5; x != y"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = x; return x != y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = x; return x !== y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = new int[1]; return x != y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = new int[1]; return x !== y;"));
        assertEquals(true, exec("Map x = new HashMap(); List y = new ArrayList(); return x != y;"));
        assertEquals(true, exec("Map x = new HashMap(); List y = new ArrayList(); return x !== y;"));
    }

    public void testBranchEquals() {
        assertEquals(0, exec("def a = (char)'a'; def b = (char)'b'; if (a == b) return 1; else return 0;"));
        assertEquals(1, exec("def a = (char)'a'; def b = (char)'a'; if (a == b) return 1; else return 0;"));
        assertEquals(1, exec("def a = 1; def b = 1; if (a === b) return 1; else return 0;"));
        assertEquals(1, exec("def a = (char)'a'; def b = (char)'a'; if (a === b) return 1; else return 0;"));
        assertEquals(1, exec("def a = (char)'a'; Object b = a; if (a === b) return 1; else return 0;"));
        assertEquals(1, exec("def a = 1; Number b = a; Number c = a; if (c === b) return 1; else return 0;"));
        assertEquals(0, exec("def a = 1; Object b = new HashMap(); if (a === (Object)b) return 1; else return 0;"));
    }

    public void testEqualsDefAndPrimitive() {
        /* This test needs an Integer that isn't cached by Integer.valueOf so we draw one randomly. We can't use any fixed integer because
         * we can never be sure that the JVM hasn't configured itself to cache that Integer. It is sneaky like that. */
        int uncachedAutoboxedInt = randomValueOtherThanMany(i -> Integer.valueOf(i) == Integer.valueOf(i), ESTestCase::randomInt);
        assertEquals(true, exec("def x = params.i; int y = params.i; return x == y;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return x === y;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return y == x;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return y === x;", singletonMap("i", uncachedAutoboxedInt), true));

        /* Now check that we use valueOf with the boxing used for comparing primitives to def. For this we need an
         * integer that is cached by Integer.valueOf. The JLS says 0 should always be cached. */
        int cachedAutoboxedInt = 0;
        assertSame(Integer.valueOf(cachedAutoboxedInt), Integer.valueOf(cachedAutoboxedInt));
        assertEquals(true, exec("def x = params.i; int y = params.i; return x == y;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return x === y;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return y == x;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return y === x;", singletonMap("i", cachedAutoboxedInt), true));
    }

    public void testBranchNotEquals() {
        assertEquals(1, exec("def a = (char)'a'; def b = (char)'b'; if (a != b) return 1; else return 0;"));
        assertEquals(0, exec("def a = (char)'a'; def b = (char)'a'; if (a != b) return 1; else return 0;"));
        assertEquals(0, exec("def a = 1; def b = 1; if (a !== b) return 1; else return 0;"));
        assertEquals(0, exec("def a = (char)'a'; def b = (char)'a'; if (a !== b) return 1; else return 0;"));
        assertEquals(0, exec("def a = (char)'a'; Object b = a; if (a !== b) return 1; else return 0;"));
        assertEquals(0, exec("def a = 1; Number b = a; Number c = a; if (c !== b) return 1; else return 0;"));
        assertEquals(1, exec("def a = 1; Object b = new HashMap(); if (a !== (Object)b) return 1; else return 0;"));
    }

    public void testNotEqualsDefAndPrimitive() {
        /* This test needs an Integer that isn't cached by Integer.valueOf so we draw one randomly. We can't use any fixed integer because
         * we can never be sure that the JVM hasn't configured itself to cache that Integer. It is sneaky like that. */
        int uncachedAutoboxedInt = randomValueOtherThanMany(i -> Integer.valueOf(i) == Integer.valueOf(i), ESTestCase::randomInt);
        assertEquals(false, exec("def x = params.i; int y = params.i; return x != y;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return x !== y;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return y != x;", singletonMap("i", uncachedAutoboxedInt), true));
        assertEquals(true, exec("def x = params.i; int y = params.i; return y !== x;", singletonMap("i", uncachedAutoboxedInt), true));

        /* Now check that we use valueOf with the boxing used for comparing primitives to def. For this we need an
         * integer that is cached by Integer.valueOf. The JLS says 0 should always be cached. */
        int cachedAutoboxedInt = 0;
        assertSame(Integer.valueOf(cachedAutoboxedInt), Integer.valueOf(cachedAutoboxedInt));
        assertEquals(false, exec("def x = params.i; int y = params.i; return x != y;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return x !== y;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return y != x;", singletonMap("i", cachedAutoboxedInt), true));
        assertEquals(false, exec("def x = params.i; int y = params.i; return y !== x;", singletonMap("i", cachedAutoboxedInt), true));
    }

    public void testRightHandNull() {
        assertEquals(false, exec("HashMap a = new HashMap(); return a == null;"));
        assertEquals(false, exec("HashMap a = new HashMap(); return a === null;"));
        assertEquals(true, exec("HashMap a = new HashMap(); return a != null;"));
        assertEquals(true, exec("HashMap a = new HashMap(); return a !== null;"));
    }

    public void testLeftHandNull() {
        assertEquals(false, exec("HashMap a = new HashMap(); return null == a;"));
        assertEquals(false, exec("HashMap a = new HashMap(); return null === a;"));
        assertEquals(true, exec("HashMap a = new HashMap(); return null != a;"));
        assertEquals(true, exec("HashMap a = new HashMap(); return null !== a;"));
    }

    public void testStringEquals() {
        assertEquals(false, exec("def x = null; return \"a\" == x"));
        assertEquals(true, exec("def x = \"a\"; return \"a\" == x"));
        assertEquals(true, exec("def x = null; return \"a\" != x"));
        assertEquals(false, exec("def x = \"a\"; return \"a\" != x"));

        assertEquals(false, exec("def x = null; return x == \"a\""));
        assertEquals(true, exec("def x = \"a\"; return x == \"a\""));
        assertEquals(true, exec("def x = null; return x != \"a\""));
        assertEquals(false, exec("def x = \"a\"; return x != \"a\""));
    }

    public void testStringEqualsMethodCall() {
        assertBytecodeExists("def x = \"a\"; return \"a\" == x", "INVOKEVIRTUAL java/lang/Object.equals (Ljava/lang/Object;)Z");
        assertBytecodeExists("def x = \"a\"; return \"a\" != x", "INVOKEVIRTUAL java/lang/Object.equals (Ljava/lang/Object;)Z");
        assertBytecodeExists("def x = \"a\"; return x == \"a\"", "INVOKEVIRTUAL java/lang/Object.equals (Ljava/lang/Object;)Z");
    }

    public void testEqualsNullCheck() {
        // get the same callsite working once, then with a null
        // need to specify call site depth as 0 to force MIC to execute
        assertEquals(false, exec("""
            def list = [2, 2, 3, 3, 4, null];
            boolean b;
            for (int i=0; i<list.length; i+=2) {
                b = list[i] == list[i+1];
                b = list[i+1] == 10;
                b = 10 == list[i+1];
            }
            return b;
            """, Map.of(), Map.of(CompilerSettings.INITIAL_CALL_SITE_DEPTH, "0"), false));
    }
}
