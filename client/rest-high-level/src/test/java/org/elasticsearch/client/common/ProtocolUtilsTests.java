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

package org.elasticsearch.client.common;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class ProtocolUtilsTests  extends ESTestCase {

    public void testMapStringEqualsAndHash() {
        assertTrue(ProtocolUtils.equals(null, null));
        assertFalse(ProtocolUtils.equals(null, new HashMap<>()));
        assertFalse(ProtocolUtils.equals(new HashMap<>(), null));

        Map<String, String[]> a = new HashMap<>();
        a.put("foo", new String[] { "a", "b" });
        a.put("bar", new String[] { "b", "c" });

        Map<String, String[]> b = new HashMap<>();
        b.put("foo", new String[] { "a", "b" });

        assertFalse(ProtocolUtils.equals(a, b));
        assertFalse(ProtocolUtils.equals(b, a));

        b.put("bar", new String[] { "c", "b" });

        assertFalse(ProtocolUtils.equals(a, b));
        assertFalse(ProtocolUtils.equals(b, a));

        b.put("bar", new String[] { "b", "c" });

        assertTrue(ProtocolUtils.equals(a, b));
        assertTrue(ProtocolUtils.equals(b, a));
        assertEquals(ProtocolUtils.hashCode(a), ProtocolUtils.hashCode(b));

        b.put("baz", new String[] { "b", "c" });

        assertFalse(ProtocolUtils.equals(a, b));
        assertFalse(ProtocolUtils.equals(b, a));

        a.put("non", null);

        assertFalse(ProtocolUtils.equals(a, b));
        assertFalse(ProtocolUtils.equals(b, a));

        b.put("non", null);
        b.remove("baz");

        assertTrue(ProtocolUtils.equals(a, b));
        assertTrue(ProtocolUtils.equals(b, a));
        assertEquals(ProtocolUtils.hashCode(a), ProtocolUtils.hashCode(b));
    }
}
