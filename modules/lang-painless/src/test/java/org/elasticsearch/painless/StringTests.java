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

package org.elasticsearch.painless;

import static org.elasticsearch.painless.WriterConstants.MAX_INDY_STRING_CONCAT_ARGS;

import java.util.Locale;

public class StringTests extends ScriptTestCase {

    public void testAppend() {
        // boolean
        assertEquals("cat" + true, exec("String s = \"cat\"; return s + true;"));
        // byte
        assertEquals("cat" + (byte)3, exec("String s = \"cat\"; return s + (byte)3;"));
        // short
        assertEquals("cat" + (short)3, exec("String s = \"cat\"; return s + (short)3;"));
        // char
        assertEquals("cat" + 't', exec("String s = \"cat\"; return s + 't';"));
        assertEquals("cat" + (char)40, exec("String s = \"cat\"; return s + (char)40;"));
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
        assertEquals("cat" + (byte)3, exec("String s = 'cat'; return s + (byte)3;"));
        // short
        assertEquals("cat" + (short)3, exec("String s = 'cat'; return s + (short)3;"));
        // char
        assertEquals("cat" + 't', exec("String s = 'cat'; return s + 't';"));
        assertEquals("cat" + (char)40, exec("String s = 'cat'; return s + (char)40;"));
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
        for (int i = MAX_INDY_STRING_CONCAT_ARGS - 5; i < MAX_INDY_STRING_CONCAT_ARGS + 5; i++) {
            doTestAppendMany(i);
        }
    }

    private void doTestAppendMany(int count) {
        StringBuilder script = new StringBuilder("String s = \"cat\"; return s");
        StringBuilder result = new StringBuilder("cat");
        for (int i = 1; i < count; i++) {
            final String s = String.format(Locale.ROOT, "%03d", i);
            script.append(" + '").append(s).append("'.toString()");
            result.append(s);
        }
        final String s = script.toString();
        assertTrue("every string part should be separatly pushed to stack.",
                Debugger.toString(s).contains(String.format(Locale.ROOT, "LDC \"%03d\"", count/2)));
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
        assertEquals(97, ((char[])exec("String s = \"a\"; return s.toCharArray();"))[0]);
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
        assertEquals(97, ((char[])exec("return \"a\".toCharArray();"))[0]);
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
        assertEquals(97, ((char[])exec("String s = 'a'; return s.toCharArray();"))[0]);
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
        assertEquals(97, ((char[])exec("return 'a'.toCharArray();"))[0]);
        assertEquals("a", exec("return ' a '.trim();"));
    }

    public void testStringAndCharacter() {
        assertEquals('c', exec("return (char)\"c\""));
        assertEquals('c', exec("return (char)'c'"));
        assertEquals("c", exec("return (String)(char)\"c\""));
        assertEquals("c", exec("return (String)(char)'c'"));

        assertEquals('c', exec("String s = \"c\"; (char)s"));
        assertEquals('c', exec("String s = 'c'; (char)s"));

        ClassCastException expected = expectScriptThrows(ClassCastException.class, () -> {
            assertEquals("cc", exec("return (String)(char)\"cc\""));
        });
        assertTrue(expected.getMessage().contains("Cannot cast [String] with length greater than one to [char]."));

        expected = expectScriptThrows(ClassCastException.class, () -> {
            assertEquals("cc", exec("return (String)(char)'cc'"));
        });
        assertTrue(expected.getMessage().contains("Cannot cast [String] with length greater than one to [char]."));

        expected = expectScriptThrows(ClassCastException.class, () -> {
            assertEquals('c', exec("String s = \"cc\"; (char)s"));
        });
        assertTrue(expected.getMessage().contains("Cannot cast [String] with length greater than one to [char]."));

        expected = expectScriptThrows(ClassCastException.class, () -> {
            assertEquals('c', exec("String s = 'cc'; (char)s"));
        });
        assertTrue(expected.getMessage().contains("Cannot cast [String] with length greater than one to [char]."));
    }
    
    public void testDefConcat() {
        assertEquals("a" + (byte)2, exec("def x = 'a'; def y = (byte)2; return x + y"));
        assertEquals("a" + (short)2, exec("def x = 'a'; def y = (short)2; return x + y"));
        assertEquals("a" + (char)2, exec("def x = 'a'; def y = (char)2; return x + y"));
        assertEquals("a" + 2, exec("def x = 'a'; def y = (int)2; return x + y"));
        assertEquals("a" + 2L, exec("def x = 'a'; def y = (long)2; return x + y"));
        assertEquals("a" + 2F, exec("def x = 'a'; def y = (float)2; return x + y"));
        assertEquals("a" + 2D, exec("def x = 'a'; def y = (double)2; return x + y"));
        assertEquals("ab", exec("def x = 'a'; def y = 'b'; return x + y"));
        assertEquals((byte)2 + "a", exec("def x = 'a'; def y = (byte)2; return y + x"));
        assertEquals((short)2 + "a", exec("def x = 'a'; def y = (short)2; return y + x"));
        assertEquals((char)2 + "a", exec("def x = 'a'; def y = (char)2; return y + x"));
        assertEquals(2 + "a", exec("def x = 'a'; def y = (int)2; return y + x"));
        assertEquals(2L + "a", exec("def x = 'a'; def y = (long)2; return y + x"));
        assertEquals(2F + "a", exec("def x = 'a'; def y = (float)2; return y + x"));
        assertEquals(2D + "a", exec("def x = 'a'; def y = (double)2; return y + x"));
        assertEquals("anull", exec("def x = 'a'; def y = null; return x + y"));
        assertEquals("nullb", exec("def x = null; def y = 'b'; return x + y"));
        expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = null; def y = null; return x + y");
        });
    }
    
    public void testDefCompoundAssignment() {
        assertEquals("a" + (byte)2, exec("def x = 'a'; x += (byte)2; return x"));
        assertEquals("a" + (short)2, exec("def x = 'a'; x  += (short)2; return x"));
        assertEquals("a" + (char)2, exec("def x = 'a'; x += (char)2; return x"));
        assertEquals("a" + 2, exec("def x = 'a'; x += (int)2; return x"));
        assertEquals("a" + 2L, exec("def x = 'a'; x += (long)2; return x"));
        assertEquals("a" + 2F, exec("def x = 'a'; x += (float)2; return x"));
        assertEquals("a" + 2D, exec("def x = 'a'; x += (double)2; return x"));
        assertEquals("ab", exec("def x = 'a'; def y = 'b'; x += y; return x"));
        assertEquals("anull", exec("def x = 'a'; x += null; return x"));
        assertEquals("nullb", exec("def x = null; x += 'b'; return x"));
        expectScriptThrows(NullPointerException.class, () -> {
            exec("def x = null; def y = null; x += y");
        });
    }

    public void testAppendStringIntoMap() {
        assertEquals("nullcat", exec("def a = new HashMap(); a.cat += 'cat'"));
    }
}
