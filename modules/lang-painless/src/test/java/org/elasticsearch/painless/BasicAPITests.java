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

import java.util.HashMap;
import java.util.Map;

public class BasicAPITests extends ScriptTestCase {

    public void testListIterator() {
        assertEquals(3, exec("List x = new ArrayList(); x.add(2); x.add(3); x.add(-2); Iterator y = x.iterator(); " +
            "int total = 0; while (y.hasNext()) total += y.next(); return total;"));
        assertEquals("abc", exec("List x = new ArrayList(); x.add(\"a\"); x.add(\"b\"); x.add(\"c\"); " +
            "Iterator y = x.iterator(); String total = \"\"; while (y.hasNext()) total += y.next(); return total;"));
        assertEquals(3, exec("def x = new ArrayList(); x.add(2); x.add(3); x.add(-2); def y = x.iterator(); " +
            "def total = 0; while (y.hasNext()) total += y.next(); return total;"));
    }

    public void testSetIterator() {
        assertEquals(3, exec("Set x = new HashSet(); x.add(2); x.add(3); x.add(-2); Iterator y = x.iterator(); " +
            "int total = 0; while (y.hasNext()) total += y.next(); return total;"));
        assertEquals("abc", exec("Set x = new HashSet(); x.add(\"a\"); x.add(\"b\"); x.add(\"c\"); " +
            "Iterator y = x.iterator(); String total = \"\"; while (y.hasNext()) total += y.next(); return total;"));
        assertEquals(3, exec("def x = new HashSet(); x.add(2); x.add(3); x.add(-2); def y = x.iterator(); " +
            "def total = 0; while (y.hasNext()) total += (int)y.next(); return total;"));
    }

    public void testMapIterator() {
        assertEquals(3, exec("Map x = new HashMap(); x.put(2, 2); x.put(3, 3); x.put(-2, -2); Iterator y = x.keySet().iterator(); " +
            "int total = 0; while (y.hasNext()) total += (int)y.next(); return total;"));
        assertEquals(3, exec("Map x = new HashMap(); x.put(2, 2); x.put(3, 3); x.put(-2, -2); Iterator y = x.values().iterator(); " +
            "int total = 0; while (y.hasNext()) total += (int)y.next(); return total;"));
    }

    /** Test loads and stores with a map */
    public void testMapLoadStore() {
        assertEquals(5, exec("def x = new HashMap(); x.abc = 5; return x.abc;"));
        assertEquals(5, exec("def x = new HashMap(); x['abc'] = 5; return x['abc'];"));
    }

    /** Test loads and stores with update script equivalent */
    public void testUpdateMapLoadStore() {
        Map<String, Object> load = new HashMap<>();
        Map<String, Object> _source = new HashMap<>();
        Map<String, Object> ctx = new HashMap<>();
        Map<String, Object> params = new HashMap<>();

        load.put("load5", "testvalue");
        _source.put("load", load);
        ctx.put("_source", _source);
        params.put("ctx", ctx);

        assertEquals("testvalue", exec("ctx._source['load'].5 = ctx._source['load'].remove('load5')", params, true));
    }

    /** Test loads and stores with a list */
    public void testListLoadStore() {
        assertEquals(5, exec("def x = new ArrayList(); x.add(3); x.0 = 5; return x.0;"));
        assertEquals(5, exec("def x = new ArrayList(); x.add(3); x[0] = 5; return x[0];"));
    }

    /** Test shortcut for getters with isXXXX */
    public void testListEmpty() {
        assertEquals(true, exec("def x = new ArrayList(); return x.empty;"));
        assertEquals(true, exec("def x = new HashMap(); return x.empty;"));
    }

    /** Test list method invocation */
    public void testListGet() {
        assertEquals(5, exec("def x = new ArrayList(); x.add(5); return x.get(0);"));
        assertEquals(5, exec("def x = new ArrayList(); x.add(5); def index = 0; return x.get(index);"));
    }

    public void testListAsArray() {
        assertEquals(1, exec("def x = new ArrayList(); x.add(5); return x.length"));
        assertEquals(5, exec("def x = new ArrayList(); x.add(5); return x[0]"));
        assertEquals(1, exec("List x = new ArrayList(); x.add('Hallo'); return x.length"));
    }

    public void testDefAssignments() {
        assertEquals(2, exec("int x; def y = 2.0; x = (int)y;"));
    }

    public void testInternalBoxing() {
        assertBytecodeExists("def x = true", "INVOKESTATIC java/lang/Boolean.valueOf (Z)Ljava/lang/Boolean;");
        assertBytecodeExists("def x = (byte)1", "INVOKESTATIC java/lang/Byte.valueOf (B)Ljava/lang/Byte;");
        assertBytecodeExists("def x = (short)1", "INVOKESTATIC java/lang/Short.valueOf (S)Ljava/lang/Short;");
        assertBytecodeExists("def x = (char)1", "INVOKESTATIC java/lang/Character.valueOf (C)Ljava/lang/Character;");
        assertBytecodeExists("def x = 1", "INVOKESTATIC java/lang/Integer.valueOf (I)Ljava/lang/Integer;");
        assertBytecodeExists("def x = 1L", "INVOKESTATIC java/lang/Long.valueOf (J)Ljava/lang/Long;");
        assertBytecodeExists("def x = 1F", "INVOKESTATIC java/lang/Float.valueOf (F)Ljava/lang/Float;");
        assertBytecodeExists("def x = 1D", "INVOKESTATIC java/lang/Double.valueOf (D)Ljava/lang/Double;");
    }

    public void testInterfaceDefaultMethods() {
        assertEquals(1, exec("Map map = new HashMap(); return map.getOrDefault(5,1);"));
        assertEquals(1, exec("def map = new HashMap(); return map.getOrDefault(5,1);"));
    }

    public void testInterfacesHaveObject() {
        assertEquals("{}", exec("Map map = new HashMap(); return map.toString();"));
        assertEquals("{}", exec("def map = new HashMap(); return map.toString();"));
    }

    public void testPrimitivesHaveMethods() {
        assertEquals(5, exec("int x = 5; return x.intValue();"));
        assertEquals("5", exec("int x = 5; return x.toString();"));
        assertEquals(0, exec("int x = 5; return x.compareTo(5);"));
    }
}
