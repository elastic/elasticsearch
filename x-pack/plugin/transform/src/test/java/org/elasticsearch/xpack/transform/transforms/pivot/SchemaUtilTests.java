/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class SchemaUtilTests extends ESTestCase {

    public void testInsertNestedObjectMappings() {
        Map<String, String> fieldMappings = new HashMap<>() {
            {
                // creates: a.b, a
                put("a.b.c", "long");
                put("a.b.d", "double");
                // creates: c.b, c
                put("c.b.a", "double");
                // creates: c.d
                put("c.d.e", "object");
                put("d", "long");
                put("e.f.g", "long");
                // cc: already there
                put("e.f", "object");
                // cc: already there but different type (should not be possible)
                put("e", "long");
                // cc: start with . (should not be possible)
                put(".x", "long");
                // cc: start and ends with . (should not be possible), creates: .y
                put(".y.", "long");
                // cc: ends with . (should not be possible), creates: .z
                put(".z.", "long");
            }
        };

        SchemaUtil.insertNestedObjectMappings(fieldMappings);

        assertEquals(18, fieldMappings.size());
        assertEquals("long", fieldMappings.get("a.b.c"));
        assertEquals("object", fieldMappings.get("a.b"));
        assertEquals("double", fieldMappings.get("a.b.d"));
        assertEquals("object", fieldMappings.get("a"));
        assertEquals("object", fieldMappings.get("c.d"));
        assertEquals("object", fieldMappings.get("e.f"));
        assertEquals("long", fieldMappings.get("e"));
        assertEquals("object", fieldMappings.get(".y"));
        assertEquals("object", fieldMappings.get(".z"));
        assertFalse(fieldMappings.containsKey("."));
        assertFalse(fieldMappings.containsKey(""));
    }

}
