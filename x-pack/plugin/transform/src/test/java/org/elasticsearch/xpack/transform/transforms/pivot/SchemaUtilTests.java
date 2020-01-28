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
        Map<String, String> fieldMappings = new HashMap<>();

        // creates: a.b, a
        fieldMappings.put("a.b.c", "long");
        fieldMappings.put("a.b.d", "double");
        // creates: c.b, c
        fieldMappings.put("c.b.a", "double");
        // creates: c.d
        fieldMappings.put("c.d.e", "object");
        fieldMappings.put("d", "long");
        fieldMappings.put("e.f.g", "long");
        // cc: already there
        fieldMappings.put("e.f", "object");
        // cc: already there but different type (should not be possible)
        fieldMappings.put("e", "long");
        // cc: start with . (should not be possible)
        fieldMappings.put(".x", "long");
        // cc: start and ends with . (should not be possible), creates: .y
        fieldMappings.put(".y.", "long");
        // cc: ends with . (should not be possible), creates: .z
        fieldMappings.put(".z.", "long");

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
