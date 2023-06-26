/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.core.Strings;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.painless.WriterConstants.MAX_STRING_CONCAT_ARGS;

public class StringTests extends ScriptTestCase {

    public void testAppend() {
        // boolean
        assertEquals("cat" + true, exec("String s = \"cat\"; return s + true;"));
        // byte
        assertEquals("cat" + (byte) 3, exec("String s = \"cat\"; return s + (byte)3;"));
        // short
        assertEquals("cat" + (short) 3, exec("String s = \"cat\"; return s + (short)3;"));
        // char
        assertEquals("cat" + 't', exec("String s = \"cat\"; return s + 't';"));
        assertEquals("cat" + (char) 40, exec("String s = \"cat\"; return s + (char)40;"));
        // int
        assertEquals("cat" + 2, exec("String s = \"cat\"; return s + 2;"));
        // long
        assertEquals("cat" + 2L, exec("String s = \"cat\"; return s + 2L;"));
        // float
        assertEquals("cat" + 2F, exec("String s = \"cat\"; return s + 2F;"));
        // double
        assertEquals("cat" + 2.0, exec("String s = \"cat\"; return s + 2.0;"));
        // String
        assertEquals("cat" + "cat", exec("String s = \"cat\"; return s + s;"));

        // boolean
        assertEquals("cat" + true, exec("String s = 'cat'; return s + true;"));
        // byte
        assertEquals("cat" + (byte) 3, exec("String s = 'cat'; return s + (byte)3;"));
        // short
        assertEquals("cat" + (short) 3, exec("String s = 'cat'; return s + (short)3;"));
        // char
        assertEquals("cat" + 't', exec("String s = 'cat'; return s + 't';"));
        assertEquals("cat" + (char) 40, exec("String s = 'cat'; return s + (char)40;"));
        // int
        assertEquals("cat" + 2, exec("String s = 'cat'; return s + 2;"));
        // long
        assertEquals("cat" + 2L, exec("String s = 'cat'; return s + 2L;"));
        // float
        assertEquals("cat" + 2F, exec("String s = 'cat'; return s + 2F;"));
        // double
        assertEquals("cat" + 2.0, exec("String s = 'cat'; return s + 2.0;"));
        // String
        assertEquals("cat" + "cat", exec("String s = 'cat'; return s + s;"));
    }

    public void testAppendMultiple() {
        assertEquals("cat" + true + "abc" + null, exec("String s = \"cat\"; return s + true + 'abc' + null;"));
    }

    public void testAppendMany() {
        for (int i = MAX_STRING_CONCAT_ARGS - 5; i < MAX_STRING_CONCAT_ARGS + 5; i++) {
            doTestAppendMany(i);
        }
    }

    private void doTestAppendMany(int count) {
        StringBuilder script = new StringBuilder("String s = \"cat\"; return s");
        StringBuilder result = new StringBuilder("cat");
        for (int i = 1; i < count; i++) {
            final String s = Strings.format("%03d", i);
            script.append(" + '").append(s).append("'.toString()");
            result.append(s);
        }
        final String s = script.toString();
        assertTrue(
            "every string part should be separately pushed to stack.",
            Debugger.toString(s).contains(Strings.format("LDC \"%03d\"", count / 2))
        );
        assertEquals(result.toString(), exec(s));
    }

    public void testNestedConcats() {
        assertEquals("foo1010foo", exec("String s = 'foo'; String x = '10'; return s + Integer.parseInt(x + x) + s;"));
    }

    public void testStringAPI() {
        assertEquals("", exec("return new String();"));
        assertEquals('x', exec("String s = \"x\"; return s.charAt(0);"));
        assertEquals(120, exec("String s = \"x\"; return s.codePointAt(0);"));
        assertEquals(0, exec("String s = \"x\"; return s.compareTo(\"x\");"));
        assertEquals("xx", exec("String s = \"x\"; return s.concat(\"x\");"));
        assertEquals(true, exec("String s = \"xy\"; return s.endsWith(\"y\");"));
        assertEquals(2, exec("String t = \"abcde\"; return t.indexOf(\"cd\", 1);"));
        assertEquals(false, exec("String t = \"abcde\"; return t.isEmpty();"));
        assertEquals(5, exec("String t = \"abcde\"; return t.length();"));
        assertEquals("cdcde", exec("String t = \"abcde\"; return t.replace(\"ab\", \"cd\");"));
        assertEquals(false, exec("String s = \"xy\"; return s.startsWith(\"y\");"));
        assertEquals("e", exec("String t = \"abcde\"; return t.substring(4, 5);"));
        assertEquals(97, ((char[]) exec("String s = \"a\"; return s.toCharArray();"))[0]);
        assertEquals("a", exec("String s = \" a \"; return s.trim();"));
        assertEquals('x', exec("return \"x\".charAt(0);"));
        assertEquals(120, exec("return \"x\".codePointAt(0);"));
        assertEquals(0, exec("return \"x\".compareTo(\"x\");"));
        assertEquals("xx", exec("return \"x\".concat(\"x\");"));
        assertEquals(true, exec("return \"xy\".endsWith(\"y\");"));
        assertEquals(2, exec("return \"abcde\".indexOf(\"cd\", 1);"));
        assertEquals(false, exec("return \"abcde\".isEmpty();"));
        assertEquals(5, exec("return \"abcde\".length();"));
        assertEquals("cdcde", exec("return \"abcde\".replace(\"ab\", \"cd\");"));
        assertEquals(false, exec("return \"xy\".startsWith(\"y\");"));
        assertEquals("e", exec("return \"abcde\".substring(4, 5);"));
        assertEquals(97, ((char[]) exec("return \"a\".toCharArray();"))[0]);
        assertEquals("a", exec("return \" a \".trim();"));

        assertEquals("", exec("return new String();"));
        assertEquals('x', exec("String s = 'x'; return s.charAt(0);"));
        assertEquals(120, exec("String s = 'x'; return s.codePointAt(0);"));
        assertEquals(0, exec("String s = 'x'; return s.compareTo('x');"));
        assertEquals("xx", exec("String s = 'x'; return s.concat('x');"));
        assertEquals(true, exec("String s = 'xy'; return s.endsWith('y');"));
        assertEquals(2, exec("String t = 'abcde'; return t.indexOf('cd', 1);"));
        assertEquals(false, exec("String t = 'abcde'; return t.isEmpty();"));
        assertEquals(5, exec("String t = 'abcde'; return t.length();"));
        assertEquals("cdcde", exec("String t = 'abcde'; return t.replace('ab', 'cd');"));
        assertEquals(false, exec("String s = 'xy'; return s.startsWith('y');"));
        assertEquals("e", exec("String t = 'abcde'; return t.substring(4, 5);"));
        assertEquals(97, ((char[]) exec("String s = 'a'; return s.toCharArray();"))[0]);
        assertEquals("a", exec("String s = ' a '; return s.trim();"));
        assertEquals('x', exec("return 'x'.charAt(0);"));
        assertEquals(120, exec("return 'x'.codePointAt(0);"));
        assertEquals(0, exec("return 'x'.compareTo('x');"));
        assertEquals("xx", exec("return 'x'.concat('x');"));
        assertEquals(true, exec("return 'xy'.endsWith('y');"));
        assertEquals(2, exec("return 'abcde'.indexOf('cd', 1);"));
        assertEquals(false, exec("return 'abcde'.isEmpty();"));
        assertEquals(5, exec("return 'abcde'.length();"));
        assertEquals("cdcde", exec("return 'abcde'.replace('ab', 'cd');"));
        assertEquals(false, exec("return 'xy'.startsWith('y');"));
        assertEquals("e", exec("return 'abcde'.substring(4, 5);"));
        assertEquals(97, ((char[]) exec("return 'a'.toCharArray();"))[0]);
        assertEquals("a", exec("return ' a '.trim();"));
    }

    public void testStringAndCharacter() {
        assertEquals('c', exec("return (char)\"c\""));
        assertEquals('c', exec("return (char)'c'"));
        assertEquals("c", exec("return (String)(char)\"c\""));
        assertEquals("c", exec("return (String)(char)'c'"));

        assertEquals('c', exec("String s = \"c\"; (char)s"));
        assertEquals('c', exec("String s = 'c'; (char)s"));

        ClassCastException expected = expectScriptThrows(ClassCastException.class, false, () -> {
            assertEquals("cc", exec("return (String)(char)\"cc\""));
        });
        assertTrue(expected.getMessage().contains("cannot cast java.lang.String with length not equal to one to char"));

        expected = expectScriptThrows(ClassCastException.class, false, () -> { assertEquals("cc", exec("return (String)(char)'cc'")); });
        assertTrue(expected.getMessage().contains("cannot cast java.lang.String with length not equal to one to char"));

        expected = expectScriptThrows(ClassCastException.class, () -> { assertEquals('c', exec("String s = \"cc\"; (char)s")); });
        assertTrue(expected.getMessage().contains("cannot cast java.lang.String with length not equal to one to char"));

        expected = expectScriptThrows(ClassCastException.class, () -> { assertEquals('c', exec("String s = 'cc'; (char)s")); });
        assertTrue(expected.getMessage().contains("cannot cast java.lang.String with length not equal to one to char"));
    }

    public void testDefConcat() {
        assertEquals("a" + (byte) 2, exec("def x = 'a'; def y = (byte)2; return x + y"));
        assertEquals("a" + (short) 2, exec("def x = 'a'; def y = (short)2; return x + y"));
        assertEquals("a" + (char) 2, exec("def x = 'a'; def y = (char)2; return x + y"));
        assertEquals("a" + 2, exec("def x = 'a'; def y = (int)2; return x + y"));
        assertEquals("a" + 2L, exec("def x = 'a'; def y = (long)2; return x + y"));
        assertEquals("a" + 2F, exec("def x = 'a'; def y = (float)2; return x + y"));
        assertEquals("a" + 2D, exec("def x = 'a'; def y = (double)2; return x + y"));
        assertEquals("ab", exec("def x = 'a'; def y = 'b'; return x + y"));
        assertEquals((byte) 2 + "a", exec("def x = 'a'; def y = (byte)2; return y + x"));
        assertEquals((short) 2 + "a", exec("def x = 'a'; def y = (short)2; return y + x"));
        assertEquals((char) 2 + "a", exec("def x = 'a'; def y = (char)2; return y + x"));
        assertEquals(2 + "a", exec("def x = 'a'; def y = (int)2; return y + x"));
        assertEquals(2L + "a", exec("def x = 'a'; def y = (long)2; return y + x"));
        assertEquals(2F + "a", exec("def x = 'a'; def y = (float)2; return y + x"));
        assertEquals(2D + "a", exec("def x = 'a'; def y = (double)2; return y + x"));
        assertEquals("anull", exec("def x = 'a'; def y = null; return x + y"));
        assertEquals("nullb", exec("def x = null; def y = 'b'; return x + y"));
        expectScriptThrows(NullPointerException.class, () -> { exec("def x = null; def y = null; return x + y"); });
    }

    public void testDefCompoundAssignment() {
        assertEquals("a" + (byte) 2, exec("def x = 'a'; x += (byte)2; return x"));
        assertEquals("a" + (short) 2, exec("def x = 'a'; x  += (short)2; return x"));
        assertEquals("a" + (char) 2, exec("def x = 'a'; x += (char)2; return x"));
        assertEquals("a" + 2, exec("def x = 'a'; x += (int)2; return x"));
        assertEquals("a" + 2L, exec("def x = 'a'; x += (long)2; return x"));
        assertEquals("a" + 2F, exec("def x = 'a'; x += (float)2; return x"));
        assertEquals("a" + 2D, exec("def x = 'a'; x += (double)2; return x"));
        assertEquals("ab", exec("def x = 'a'; def y = 'b'; x += y; return x"));
        assertEquals("anull", exec("def x = 'a'; x += null; return x"));
        assertEquals("nullb", exec("def x = null; x += 'b'; return x"));
        expectScriptThrows(NullPointerException.class, () -> { exec("def x = null; def y = null; x += y"); });
    }

    public void testComplexCompoundAssignment() {
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("_id", "somerandomid");
        params.put("ctx", ctx);

        assertEquals("somerandomid.somerandomid", exec("params.ctx._id += '.' + params.ctx._id", params, false));
        assertEquals("somerandomid.somerandomid", exec("String x = 'somerandomid'; x += '.' + x"));
        assertEquals("somerandomid.somerandomid", exec("def x = 'somerandomid'; x += '.' + x"));
    }

    public void testAppendStringIntoMap() {
        assertEquals("nullcat", exec("def a = new HashMap(); a.cat += 'cat'"));
    }

    public void testBase64Augmentations() {
        assertEquals("Y2F0", exec("'cat'.encodeBase64()"));
        assertEquals("cat", exec("'Y2F0'.decodeBase64()"));
        assertEquals("6KiA6Kqe", exec("'\u8A00\u8A9E'.encodeBase64()"));
        assertEquals("\u8A00\u8A9E", exec("'6KiA6Kqe'.decodeBase64()"));

        String rando = randomRealisticUnicodeOfLength(between(5, 1000));
        assertEquals(rando, exec("params.rando.encodeBase64().decodeBase64()", singletonMap("rando", rando), true));
    }

    public void testConstantStringConcatBytecode() {
        assertBytecodeExists(
            "String s = \"cat\"; return s + true + 'abc' + null;",
            "INVOKEDYNAMIC concat(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;"
        );
    }

    public void testStringConcatBytecode() {
        assertBytecodeExists(
            "String s = \"cat\"; boolean t = true; Object u = null; return s + t + 'abc' + u;",
            "INVOKEDYNAMIC concat(Ljava/lang/String;ZLjava/lang/String;Ljava/lang/Object;)Ljava/lang/String;"
        );
    }

    public void testNullStringConcat() {
        assertEquals("" + null + null, exec("'' + null + null"));
        assertEquals("" + 2 + null, exec("'' + 2 + null"));
        assertEquals("" + null + 2, exec("'' + null + 2"));
        assertEquals("" + null + null, exec("null + '' + null"));
        assertEquals("" + 2 + null, exec("2 + '' + null"));
        assertEquals("" + null + 2, exec("null + '' + 2"));
    }
}
