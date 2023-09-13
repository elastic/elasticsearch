/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class MapParsingUtilsTests extends ESTestCase {

    public void testRemoveAsTypeWithTheCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 1.0));

        Integer i = MapParsingUtils.removeAsType(map, "a", Integer.class);
        assertEquals(Integer.valueOf(5), i);
        assertNull(map.get("a")); // field has been removed

        String str = MapParsingUtils.removeAsType(map, "b", String.class);
        assertEquals("a string", str);
        assertNull(map.get("b"));

        Boolean b = MapParsingUtils.removeAsType(map, "c", Boolean.class);
        assertEquals(Boolean.TRUE, b);
        assertNull(map.get("c"));

        Double d = MapParsingUtils.removeAsType(map, "d", Double.class);
        assertEquals(Double.valueOf(1.0), d);
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsTypeWithInCorrectType() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE, "d", 5.0, "e", 5));

        var e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "a", String.class));
        assertThat(
            e.getMessage(),
            containsString("field [a] is not of the expected type. The value [5] cannot be converted to a [String]")
        );

        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "b", Boolean.class));
        assertThat(
            e.getMessage(),
            containsString("field [b] is not of the expected type. The value [a string] cannot be converted to a [Boolean]")
        );
        assertNull(map.get("b"));

        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "c", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [c] is not of the expected type. The value [true] cannot be converted to a [Integer]")
        );
        assertNull(map.get("c"));

        // cannot convert double to integer
        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "d", Integer.class));
        assertThat(
            e.getMessage(),
            containsString("field [d] is not of the expected type. The value [5.0] cannot be converted to a [Integer]")
        );
        assertNull(map.get("d"));

        // cannot convert integer to double
        e = expectThrows(ElasticsearchStatusException.class, () -> MapParsingUtils.removeAsType(map, "e", Double.class));
        assertThat(
            e.getMessage(),
            containsString("field [e] is not of the expected type. The value [5] cannot be converted to a [Double]")
        );
        assertNull(map.get("d"));

        assertThat(map.entrySet(), empty());
    }

    public void testRemoveAsTypeMissingReturnsNull() {
        Map<String, Object> map = new HashMap<>(Map.of("a", 5, "b", "a string", "c", Boolean.TRUE));
        assertNull(MapParsingUtils.removeAsType(new HashMap<>(), "missing", Integer.class));
        assertThat(map.entrySet(), hasSize(3));
    }
}
