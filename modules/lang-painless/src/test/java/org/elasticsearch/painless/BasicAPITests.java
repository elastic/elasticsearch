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

public class BasicAPITests extends ScriptTestCase {

    public void testListIterator() {
        assertEquals(3, exec("List x = new ArrayList(); x.add(2); x.add(3); x.add(-2); Iterator y = x.iterator(); " +
            "int total = 0; while (y.hasNext()) total += y.next(); return total;"));
    }

    public void testSetIterator() {
        assertEquals(3, exec("Set x = new HashSet(); x.add(2); x.add(3); x.add(-2); Iterator y = x.iterator(); " +
            "int total = 0; while (y.hasNext()) total += y.next(); return total;"));
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
        assertEquals(1, exec("List x = new ArrayList(); x.add('Hallo'); return x.length"));
        assertEquals(1, exec("List x = new ArrayList(); x.add('Hallo'); return x.length"));
    }

    public void testDefAssignments() {
        assertEquals(2, exec("int x; def y = 2.0; x = (int)y;"));
    }

}
