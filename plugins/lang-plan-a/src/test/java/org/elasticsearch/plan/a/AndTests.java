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

/** Tests for and operator across all types */
public class AndTests extends ScriptTestCase {
    
    public void testInt() throws Exception {
        assertEquals(5 & 12, exec("int x = 5; int y = 12; return x & y;"));
        assertEquals(5 & -12, exec("int x = 5; int y = -12; return x & y;"));
        assertEquals(7 & 15 & 3, exec("int x = 7; int y = 15; int z = 3; return x & y & z;"));
    }
    
    public void testIntConst() throws Exception {
        assertEquals(5 & 12, exec("return 5 & 12;"));
        assertEquals(5 & -12, exec("return 5 & -12;"));
        assertEquals(7 & 15 & 3, exec("return 7 & 15 & 3;"));
    }
    
    public void testLong() throws Exception {
        assertEquals(5L & 12L, exec("long x = 5; long y = 12; return x & y;"));
        assertEquals(5L & -12L, exec("long x = 5; long y = -12; return x & y;"));
        assertEquals(7L & 15L & 3L, exec("long x = 7; long y = 15; long z = 3; return x & y & z;"));
    }
    
    public void testLongConst() throws Exception {
        assertEquals(5L & 12L, exec("return 5L & 12L;"));
        assertEquals(5L & -12L, exec("return 5L & -12L;"));
        assertEquals(7L & 15L & 3L, exec("return 7L & 15L & 3L;"));
    }
}
