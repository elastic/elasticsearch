package org.elasticsearch.plan.a;

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

// TODO: Figure out a way to test autobox caching properly from methods such as Integer.valueOf(int);
public class EqualsTests extends ScriptTestCase {
    public void testTypesEquals() {
        assertEquals(true, exec("return false === false;"));
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
        assertEquals(true, exec("return new Long(3) == new Long(3);"));
        assertEquals(false, exec("return new Long(3) === new Long(3);"));
        assertEquals(true, exec("Integer x = new Integer(3); Object y = x; return x == y;"));
        assertEquals(true, exec("Integer x = new Integer(3); Object y = x; return x === y;"));
        assertEquals(true, exec("Integer x = new Integer(3); Object y = new Integer(3); return x == y;"));
        assertEquals(false, exec("Integer x = new Integer(3); Object y = new Integer(3); return x === y;"));
        assertEquals(true, exec("Integer x = new Integer(3); int y = 3; return x == y;"));
        assertEquals(true, exec("Integer x = new Integer(3); short y = 3; return x == y;"));
        assertEquals(true, exec("Integer x = new Integer(3); Short y = (short)3; return x == y;"));
        assertEquals(false, exec("Integer x = new Integer(3); int y = 3; return x === y;"));
        assertEquals(false, exec("Integer x = new Integer(3); double y = 3; return x === y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = x; return x == y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = x; return x === y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = new int[1]; return x == y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = new int[1]; return x === y;"));
        assertEquals(false, exec("Map x = new HashMap(); List y = new ArrayList(); return x == y;"));
        assertEquals(false, exec("Map x = new HashMap(); List y = new ArrayList(); return x === y;"));
    }

    public void testNotEquals() {
        assertEquals(false, exec("return new Long(3) != new Long(3);"));
        assertEquals(true, exec("return new Long(3) !== new Long(3);"));
        assertEquals(false, exec("Integer x = new Integer(3); Object y = x; return x != y;"));
        assertEquals(false, exec("Integer x = new Integer(3); Object y = x; return x !== y;"));
        assertEquals(false, exec("Integer x = new Integer(3); Object y = new Integer(3); return x != y;"));
        assertEquals(true, exec("Integer x = new Integer(3); Object y = new Integer(3); return x !== y;"));
        assertEquals(true, exec("Integer x = new Integer(3); int y = 3; return x !== y;"));
        assertEquals(true, exec("Integer x = new Integer(3); double y = 3; return x !== y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = x; return x != y;"));
        assertEquals(false, exec("int[] x = new int[1]; Object y = x; return x !== y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = new int[1]; return x != y;"));
        assertEquals(true, exec("int[] x = new int[1]; Object y = new int[1]; return x !== y;"));
        assertEquals(true, exec("Map x = new HashMap(); List y = new ArrayList(); return x != y;"));
        assertEquals(true, exec("Map x = new HashMap(); List y = new ArrayList(); return x !== y;"));
    }

    public void testBranchEquals() {
        assertEquals(0, exec("Character a = 'a'; Character b = 'b'; if (a == b) return 1; else return 0;"));
        assertEquals(1, exec("Character a = 'a'; Character b = 'a'; if (a == b) return 1; else return 0;"));
        assertEquals(0, exec("Integer a = new Integer(1); Integer b = 1; if (a === b) return 1; else return 0;"));
        assertEquals(0, exec("Character a = 'a'; Character b = new Character('a'); if (a === b) return 1; else return 0;"));
        assertEquals(1, exec("Character a = 'a'; Object b = a; if (a === b) return 1; else return 0;"));
        assertEquals(1, exec("Integer a = 1; Number b = a; Number c = a; if (c === b) return 1; else return 0;"));
        assertEquals(0, exec("Integer a = 1; Character b = 'a'; if (a === (Object)b) return 1; else return 0;"));
    }

    public void testBranchNotEquals() {
        assertEquals(1, exec("Character a = 'a'; Character b = 'b'; if (a != b) return 1; else return 0;"));
        assertEquals(0, exec("Character a = 'a'; Character b = 'a'; if (a != b) return 1; else return 0;"));
        assertEquals(1, exec("Integer a = new Integer(1); Integer b = 1; if (a !== b) return 1; else return 0;"));
        assertEquals(1, exec("Character a = 'a'; Character b = new Character('a'); if (a !== b) return 1; else return 0;"));
        assertEquals(0, exec("Character a = 'a'; Object b = a; if (a !== b) return 1; else return 0;"));
        assertEquals(0, exec("Integer a = 1; Number b = a; Number c = a; if (c !== b) return 1; else return 0;"));
        assertEquals(1, exec("Integer a = 1; Character b = 'a'; if (a !== (Object)b) return 1; else return 0;"));
    }

    public void testRightHandNull() {
        assertEquals(false, exec("Character a = 'a'; return a == null;"));
        assertEquals(false, exec("Character a = 'a'; return a === null;"));
        assertEquals(true, exec("Character a = 'a'; return a != null;"));
        assertEquals(true, exec("Character a = 'a'; return a !== null;"));
        assertEquals(true, exec("Character a = null; return a == null;"));
        assertEquals(false, exec("Character a = null; return a != null;"));
        assertEquals(false, exec("Character a = 'a'; Character b = null; return a == b;"));
        assertEquals(true, exec("Character a = null; Character b = null; return a === b;"));
        assertEquals(true, exec("Character a = 'a'; Character b = null; return a != b;"));
        assertEquals(false, exec("Character a = null; Character b = null; return a !== b;"));
        assertEquals(false, exec("Integer x = null; double y = 2.0; return x == y;"));
        assertEquals(true, exec("Integer x = null; Short y = null; return x == y;"));
    }

    public void testLeftHandNull() {
        assertEquals(false, exec("Character a = 'a'; return null == a;"));
        assertEquals(false, exec("Character a = 'a'; return null === a;"));
        assertEquals(true, exec("Character a = 'a'; return null != a;"));
        assertEquals(true, exec("Character a = 'a'; return null !== a;"));
        assertEquals(true, exec("Character a = null; return null == a;"));
        assertEquals(false, exec("Character a = null; return null != a;"));
        assertEquals(false, exec("Character a = null; Character b = 'a'; return a == b;"));
        assertEquals(true, exec("Character a = null; Character b = null; return a == b;"));
        assertEquals(true, exec("Character a = null; Character b = null; return b === a;"));
        assertEquals(true, exec("Character a = null; Character b = 'a'; return a != b;"));
        assertEquals(false, exec("Character a = null; Character b = null; return b != a;"));
        assertEquals(false, exec("Character a = null; Character b = null; return b !== a;"));
        assertEquals(false, exec("Integer x = null; double y = 2.0; return y == x;"));
        assertEquals(true, exec("Integer x = null; Short y = null; return y == x;"));
    }
}
