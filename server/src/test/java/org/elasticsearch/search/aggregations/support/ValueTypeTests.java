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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.test.ESTestCase;

public class ValueTypeTests extends ESTestCase {

    public void testResolve() {
        assertEquals(ValueType.STRING, ValueType.resolveForScript("string"));
        assertEquals(ValueType.DOUBLE, ValueType.resolveForScript("float"));
        assertEquals(ValueType.DOUBLE, ValueType.resolveForScript("double"));
        assertEquals(ValueType.LONG, ValueType.resolveForScript("byte"));
        assertEquals(ValueType.LONG, ValueType.resolveForScript("short"));
        assertEquals(ValueType.LONG, ValueType.resolveForScript("integer"));
        assertEquals(ValueType.LONG, ValueType.resolveForScript("long"));
        assertEquals(ValueType.DATE, ValueType.resolveForScript("date"));
        assertEquals(ValueType.IP, ValueType.resolveForScript("ip"));
        assertEquals(ValueType.BOOLEAN, ValueType.resolveForScript("boolean"));
    }

    public void testCompatibility() {
        assertTrue(ValueType.DOUBLE.isA(ValueType.NUMERIC));
        assertTrue(ValueType.LONG.isA(ValueType.NUMERIC));
        assertTrue(ValueType.DATE.isA(ValueType.LONG));
        assertTrue(ValueType.NUMERIC.isA(ValueType.NUMBER));
        assertTrue(ValueType.BOOLEAN.isA(ValueType.NUMBER));
        assertFalse(ValueType.STRING.isA(ValueType.NUMBER));
        assertFalse(ValueType.DATE.isA(ValueType.IP));
    }

}
