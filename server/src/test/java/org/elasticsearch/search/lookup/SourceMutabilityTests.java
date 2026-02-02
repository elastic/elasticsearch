/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests documenting the current mutability characteristics of Map objects returned by Source.fromBytes()
 * and Source.fromMap()
 *
 */
public class SourceMutabilityTests extends ESTestCase {

    public void testFromBytes() {

        Source s1 = Source.fromBytes(null);
        assertTrue("Should be empty", s1.source().isEmpty());
        assertMapIsImmutable(s1.source());

        Source s2 = Source.fromBytes(new BytesArray(""));
        assertTrue("Should be empty", s2.source().isEmpty());
        assertMapIsImmutable(s2.source());

        //empty map returns a mutable map?!
        Source s3 = Source.fromBytes(new BytesArray("{}"));
        assertTrue("Should be empty", s3.source().isEmpty());
        assertMapIsMutable("fromBytes('{}')", s3.source());

        Source s4 = Source.fromBytes(new BytesArray("{\"field\":\"value\"}"));
        assertFalse("Should not be empty", s4.source().isEmpty());
        assertMapIsMutable("fromBytes(non-empty)", s4.source());

        //unrelated issue...empty map still returns mutable map even with trailing content...
        //no parse exception?!?!?!
        Source s5 = Source.fromBytes(new BytesArray("{}trailing-content-is-ignored{\"field\":\"value\"}"));
        assertTrue("Should be empty", s5.source().isEmpty());
        assertMapIsMutable("fromBytes('{}')", s5.source());

    }

    public void testFromMap() {
        Source s1 = Source.fromMap(null, XContentType.JSON);
        assertTrue("Should be empty", s1.source().isEmpty());
        assertMapIsImmutable(s1.source());

        Source s2 = Source.fromMap(Map.of(), XContentType.JSON);
        assertTrue("Should be empty", s2.source().isEmpty());
        assertMapIsImmutable(s2.source());

        //mutable map can be converted to immutable
        Source s3 = Source.fromMap(new LinkedHashMap<>(), XContentType.JSON);
        assertTrue("Should be empty", s3.source().isEmpty());
        assertMapIsImmutable(s3.source());

        //immutable map remains immutable
        Source s4 = Source.fromMap(Map.of("field", "value"), XContentType.JSON);
        assertFalse("Should not be empty", s4.source().isEmpty());
        assertMapIsImmutable(s4.source());

        Map<String, Object> mutableMap = new LinkedHashMap<>();
        mutableMap.put("field", "value");
        Source source5 = Source.fromMap(mutableMap, XContentType.JSON);
        assertFalse("Should not be empty", source5.source().isEmpty());
        assertMapIsMutable("fromMap(LinkedHashMap with data)", source5.source());

    }

    private void assertMapIsImmutable(Map<String, Object> map) {
        expectThrows(UnsupportedOperationException.class, () -> map.put("test_key", "test_value"));
    }

    private void assertMapIsMutable(String scenario, Map<String, Object> map) {
        try {
            map.put("test_key", "test_value");
            assertEquals("test put in mutable map", "test_value", map.get("test_key"));
            map.remove("test_key");
            assertNull("test remove from mutable map", map.get("test_key"));
        } catch (UnsupportedOperationException e) {
            fail(scenario + " should return a mutable map but got: " + e.getMessage());
        }
    }
}
