package org.elasticsearch.painless;

import java.util.Collections;

import static java.util.Collections.singletonMap;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class BasicExpressionTests extends ScriptTestCase {

    /** simple tests returning a constant value */
    public void testReturnConstant() {
        assertEquals(5, exec("return 5"));
        assertEquals(6L, exec("return 6l"));
        assertEquals(7L, exec("return 7L"));
        assertEquals(7.0d, exec("return 7.0"));
        assertEquals(18.0d, exec("return 18d"));
        assertEquals(19.0d, exec("return 19.0d"));
        assertEquals(20.0d, exec("return 20D"));
        assertEquals(21.0d, exec("return 21.0D"));
        assertEquals(32.0F, exec("return 32.0f"));
        assertEquals(33.0F, exec("return 33f"));
        assertEquals(34.0F, exec("return 34.0F"));
        assertEquals(35.0F, exec("return 35F"));
        assertEquals((byte)255, exec("return (byte)255"));
        assertEquals((short)5, exec("return (short)5"));
        assertEquals("string", exec("return \"string\""));
        assertEquals("string", exec("return 'string'"));
        assertEquals(true, exec("return true"));
        assertEquals(false, exec("return false"));
        assertNull(exec("return null"));
    }

    public void testReturnConstantChar() {
        assertEquals('x', exec("return (char)'x';"));
    }

    public void testConstantCharTruncation() {
        assertEquals('蚠', exec("return (char)100000;"));
    }

    public void testStringEscapes() {
        // The readability of this test suffers from having to escape `\` and `"` in java strings. Please be careful. Sorry!
        // `\\` is a `\`
        assertEquals("\\string", exec("\"\\\\string\""));
        assertEquals("\\string", exec("'\\\\string'"));
        // `\"` is a `"` if surrounded by `"`s
        assertEquals("\"string", exec("\"\\\"string\""));
        Exception e = expectScriptThrows(IllegalArgumentException.class, () -> exec("'\\\"string'", false));
        assertEquals("unexpected character ['\\\"]. The only valid escape sequences in strings starting with ['] are [\\\\] and [\\'].",
                e.getMessage());
        // `\'` is a `'` if surrounded by `'`s
        e = expectScriptThrows(IllegalArgumentException.class, () -> exec("\"\\'string\"", false));
        assertEquals("unexpected character [\"\\']. The only valid escape sequences in strings starting with [\"] are [\\\\] and [\\\"].",
                e.getMessage());
        assertEquals("'string", exec("'\\'string'"));
        // We don't break native escapes like new line
        assertEquals("\nstring", exec("\"\nstring\""));
        assertEquals("\nstring", exec("'\nstring'"));

        // And we're ok with strings with multiple escape sequences
        assertEquals("\\str\"in\\g", exec("\"\\\\str\\\"in\\\\g\""));
        assertEquals("st\\r'i\\ng", exec("'st\\\\r\\'i\\\\ng'"));
    }

    public void testStringTermination() {
        // `'` inside of a string delimited with `"` should be ok
        assertEquals("test'", exec("\"test'\""));
        // `"` inside of a string delimited with `'` should be ok
        assertEquals("test\"", exec("'test\"'"));
    }

    /** declaring variables for primitive types */
    public void testDeclareVariable() {
        assertEquals(5, exec("int i = 5; return i;"));
        assertEquals(7L, exec("long l = 7; return l;"));
        assertEquals(7.0, exec("double d = 7; return d;"));
        assertEquals(32.0F, exec("float f = 32F; return f;"));
        assertEquals((byte)255, exec("byte b = (byte)255; return b;"));
        assertEquals((short)5, exec("short s = (short)5; return s;"));
        assertEquals("string", exec("String s = \"string\"; return s;"));
        assertEquals(true, exec("boolean v = true; return v;"));
        assertEquals(false, exec("boolean v = false; return v;"));
    }

    public void testCast() {
        assertEquals(1, exec("return (int)1.0;"));
        assertEquals((byte)100, exec("double x = 100; return (byte)x;"));

        assertEquals(3, exec(
                "Map x = new HashMap();\n" +
                "Object y = x;\n" +
                "((Map)y).put(2, 3);\n" +
                "return x.get(2);\n"));
    }

    public void testIllegalDefCast() {
        Exception exception = expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 1.0; int y = x; return y;");
        });
        assertTrue(exception.getMessage().contains("cannot be cast"));

        exception = expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = (short)1; byte y = x; return y;");
        });
        assertTrue(exception.getMessage().contains("cannot be cast"));
    }

    public void testCat() {
        assertEquals("aaabbb", exec("return \"aaa\" + \"bbb\";"));
        assertEquals("aaabbb", exec("String aaa = \"aaa\", bbb = \"bbb\"; return aaa + bbb;"));

        assertEquals("aaabbbbbbbbb", exec(
                "String aaa = \"aaa\", bbb = \"bbb\"; int x;\n" +
                "for (; x < 3; ++x) \n" +
                "    aaa += bbb;\n" +
                "return aaa;"));
    }

    public void testComp() {
        assertEquals(true, exec("return 2 < 3;"));
        assertEquals(false, exec("int x = 4; char y = 2; return x < y;"));
        assertEquals(true, exec("return 3 <= 3;"));
        assertEquals(true, exec("int x = 3; char y = 3; return x <= y;"));
        assertEquals(false, exec("return 2 > 3;"));
        assertEquals(true, exec("int x = 4; long y = 2; return x > y;"));
        assertEquals(false, exec("return 3 >= 4;"));
        assertEquals(true, exec("double x = 3; float y = 3; return x >= y;"));
        assertEquals(false, exec("return 3 == 4;"));
        assertEquals(true, exec("double x = 3; float y = 3; return x == y;"));
        assertEquals(true, exec("return 3 != 4;"));
        assertEquals(false, exec("double x = 3; float y = 3; return x != y;"));
    }

    /**
     * Test boxed def objects in various places
     */
    public void testBoxing() {
        // return
        assertEquals(4, exec("return params.get(\"x\");", Collections.singletonMap("x", 4), true));
        // assignment
        assertEquals(4, exec("int y = params.get(\"x\"); return y;", Collections.singletonMap("x", 4), true));
        // comparison
        assertEquals(true, exec("return 5 > params.get(\"x\");", Collections.singletonMap("x", 4), true));
    }

    public void testBool() {
        assertEquals(true, exec("return true && true;"));
        assertEquals(false, exec("boolean a = true, b = false; return a && b;"));
        assertEquals(true, exec("return true || true;"));
        assertEquals(true, exec("boolean a = true, b = false; return a || b;"));
    }

    public void testConditional() {
        assertEquals(1, exec("int x = 5; return x > 3 ? 1 : 0;"));
        assertEquals(0, exec("String a = null; return a != null ? 1 : 0;"));
    }

    public void testPrecedence() {
        assertEquals(2, exec("int x = 5; return (x+x)/x;"));
        assertEquals(true, exec("boolean t = true, f = false; return t && (f || t);"));
    }

    public void testNullSafeDeref() {
        // Objects in general
        //   Call
        assertNull(         exec("String a = null;  return a?.toString()"));
        assertEquals("foo", exec("String a = 'foo'; return a?.toString()"));
        assertNull(         exec("def    a = null;  return a?.toString()"));
        assertEquals("foo", exec("def    a = 'foo'; return a?.toString()"));
        //   Call with primitive result
        assertMustBeNullable(    "String a = null;  return a?.length()");
        assertMustBeNullable(    "String a = 'foo'; return a?.length()");
        assertNull(         exec("def    a = null;  return a?.length()"));
        assertEquals(3,     exec("def    a = 'foo'; return a?.length()"));
        //   Read shortcut
        assertMustBeNullable(    "org.elasticsearch.painless.FeatureTest a = null; return a?.x");
        assertMustBeNullable(
                "org.elasticsearch.painless.FeatureTest a = new org.elasticsearch.painless.FeatureTest(); return a?.x");
        assertNull(         exec("def    a = null;  return a?.x"));
        assertEquals(0,     exec("def    a = new org.elasticsearch.painless.FeatureTest(); return a?.x"));

        // Maps
        //   Call
        assertNull(        exec("Map a = null;        return a?.toString()"));
        assertEquals("{}", exec("Map a = [:];         return a?.toString()"));
        assertNull(        exec("def a = null;        return a?.toString()"));
        assertEquals("{}", exec("def a = [:];         return a?.toString()"));
        //   Call with primitive result
        assertMustBeNullable(   "Map a = [:];  return a?.size()");
        assertMustBeNullable(   "Map a = null; return a?.size()");
        assertNull(        exec("def a = null;        return a?.size()"));
        assertEquals(0,    exec("def a = [:];         return a?.size()"));
        //   Read shortcut
        assertNull(        exec("Map a = null;        return a?.other"));       // Read shortcut
        assertEquals(1,    exec("Map a = ['other':1]; return a?.other"));       // Read shortcut
        assertNull(        exec("def a = null;        return a?.other"));       // Read shortcut
        assertEquals(1,    exec("def a = ['other':1]; return a?.other"));       // Read shortcut

        // Array
        // Since you can't invoke methods on arrays we skip the toString and hashCode tests
        assertMustBeNullable("int[] a = null;             return a?.length");
        assertMustBeNullable("int[] a = new int[] {2, 3}; return a?.length");
        assertNull(     exec("def a = null;               return a?.length"));
        assertEquals(2, exec("def a = new int[] {2, 3};   return a?.length"));

        // Results from maps (should just work but let's test anyway)
        FeatureTest t = new FeatureTest();
        assertNull(     exec("Map a = ['thing': params.t]; return a.other?.getX()", singletonMap("t", t), true));
        assertNull(     exec("Map a = ['thing': params.t]; return a.other?.x",      singletonMap("t", t), true));
        assertNull(     exec("def a = ['thing': params.t]; return a.other?.getX()", singletonMap("t", t), true));
        assertNull(     exec("def a = ['thing': params.t]; return a.other?.x",      singletonMap("t", t), true));
        assertEquals(0, exec("Map a = ['other': params.t]; return a.other?.getX()", singletonMap("t", t), true));
        assertEquals(0, exec("Map a = ['other': params.t]; return a.other?.x",      singletonMap("t", t), true));
        assertEquals(0, exec("def a = ['other': params.t]; return a.other?.getX()", singletonMap("t", t), true));
        assertEquals(0, exec("def a = ['other': params.t]; return a.other?.x",      singletonMap("t", t), true));

        // Chains
        assertNull(     exec("Map a = ['thing': ['cat': params.t]]; return a.other?.cat?.getX()", singletonMap("t", t), true));
        assertNull(     exec("Map a = ['thing': ['cat': params.t]]; return a.other?.cat?.x",      singletonMap("t", t), true));
        assertNull(     exec("def a = ['thing': ['cat': params.t]]; return a.other?.cat?.getX()", singletonMap("t", t), true));
        assertNull(     exec("def a = ['thing': ['cat': params.t]]; return a.other?.cat?.x",      singletonMap("t", t), true));
        assertEquals(0, exec("Map a = ['other': ['cat': params.t]]; return a.other?.cat?.getX()", singletonMap("t", t), true));
        assertEquals(0, exec("Map a = ['other': ['cat': params.t]]; return a.other?.cat?.x",      singletonMap("t", t), true));
        assertEquals(0, exec("def a = ['other': ['cat': params.t]]; return a.other?.cat?.getX()", singletonMap("t", t), true));
        assertEquals(0, exec("def a = ['other': ['cat': params.t]]; return a.other?.cat?.x",      singletonMap("t", t), true));

        // Assignments
        assertNull(exec(
                    "def a = [:];\n"
                  + "a.missing_length = a.missing?.length();\n"
                  + "return a.missing_length", true));
        assertEquals(3, exec(
                    "def a = [:];\n"
                  + "a.missing = 'foo';\n"
                  + "a.missing_length = a.missing?.length();\n"
                  + "return a.missing_length", true));

        // Writes, all unsupported at this point
//        assertEquals(null, exec("org.elasticsearch.painless.FeatureTest a = null; return a?.x"));            // Read field
//        assertEquals(null, exec("org.elasticsearch.painless.FeatureTest a = null; a?.x = 7; return a?.x"));  // Write field
//        assertEquals(null, exec("Map a = null; a?.other = 'wow'; return a?.other")); // Write shortcut
//        assertEquals(null, exec("def a = null; a?.other = 'cat'; return a?.other")); // Write shortcut
//        assertEquals(null, exec("Map a = ['thing': 'bar']; a.other?.cat = 'no'; return a.other?.cat"));
//        assertEquals(null, exec("def a = ['thing': 'bar']; a.other?.cat = 'no'; return a.other?.cat"));
//        assertEquals(null, exec("Map a = ['thing': 'bar']; a.other?.cat?.dog = 'wombat'; return a.other?.cat?.dog"));
//        assertEquals(null, exec("def a = ['thing': 'bar']; a.other?.cat?.dog = 'wombat'; return a.other?.cat?.dog"));
    }

    private void assertMustBeNullable(String script) {
        Exception e = expectScriptThrows(IllegalArgumentException.class, false, () -> exec(script));
        assertEquals("Result of null safe operator must be nullable", e.getMessage());
    }
}
