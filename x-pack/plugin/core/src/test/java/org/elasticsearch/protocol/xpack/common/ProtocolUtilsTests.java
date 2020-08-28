/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.common;

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
