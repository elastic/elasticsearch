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

package org.elasticsearch.plan.a;

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
    }
}
