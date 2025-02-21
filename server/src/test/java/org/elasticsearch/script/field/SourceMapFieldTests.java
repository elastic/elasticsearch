/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SourceMapFieldTests extends ESTestCase {

    public void testResolveDepthFlat() {
        Map<String, Object> map = new HashMap<>();
        map.put("abc.d.ef", "flat");

        Map<String, Object> abc = new HashMap<>();
        map.put("abc", abc);
        abc.put("d.ef", "mixed");

        Map<String, Object> d = new HashMap<>();
        abc.put("d", d);
        d.put("ef", "nested");

        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed", "d": { "ef": "nested" } } }
        SourceMapField field = new SourceMapField("abc.d.ef", () -> map);
        assertTrue(field.exists());

        assertEquals("nested", field.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed", "d": { } } }
        d.remove("ef");
        assertEquals("missing", field.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed" }
        // TODO(stu): this should be inaccessible
        abc.remove("d");
        assertEquals("missing", field.get("missing"));

        // resolution at construction time
        field = new SourceMapField("abc.d.ef", () -> map);
        assertEquals("mixed", field.get("missing"));
        abc.remove("d.ef");
        assertEquals("missing", field.get("missing"));

        field = new SourceMapField("abc.d.ef", () -> map);
        // abc is still there
        assertEquals("missing", field.get("missing"));
        map.remove("abc");
        assertEquals("missing", field.get("missing"));

        field = new SourceMapField("abc.d.ef", () -> map);
        assertEquals("flat", field.get("missing"));
    }

    public void testExists() {
        Map<String, Object> a = new HashMap<>();
        a.put("b.c", null);
        assertTrue(new SourceMapField("a.b.c", () -> Map.of("a", a)).exists());

        a.clear();
        Map<String, Object> level1 = new HashMap<>();
        level1.put("null", null);
        a.put("level1", level1);
        a.put("null", null);
        // SourceMapField.leaf is null
        assertFalse(new SourceMapField("missing.leaf", () -> a).exists());

        // SourceMapField.leaf non-null but missing
        assertFalse(new SourceMapField("missing", () -> a).exists());

        // Check mappings with null values exist
        assertTrue(new SourceMapField("level1.null", () -> a).exists());
        assertTrue(new SourceMapField("null", () -> a).exists());
    }

    public void testSizeIsEmpty() {
        Map<String, Object> root = new HashMap<>();
        SourceMapField field = new SourceMapField("a.b.c", () -> root);
        assertTrue(field.isEmpty());
        assertEquals(0, field.size());

        root.put("a.b.c", List.of(1, 2));
        field = new SourceMapField("a.b.c", () -> root);
        assertFalse(field.isEmpty());
        assertEquals(2, field.size());

        Map<String, Object> d = new HashMap<>();
        root.put("d", d);
        field = new SourceMapField("d.e", () -> root);
        assertTrue(field.isEmpty());
        assertEquals(0, field.size());
        d.put("e", "foo");
        assertFalse(field.isEmpty());
        assertEquals(1, field.size());
    }

    public void testIterator() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);

        SourceMapField field = new SourceMapField("a.b.c", () -> root);
        assertFalse(field.iterator().hasNext());

        b.put("c", "value");
        Iterator<Object> it = field.iterator();
        assertTrue(it.hasNext());
        assertEquals("value", it.next());
        assertFalse(it.hasNext());

        b.put("c", List.of(1, 2, 3));
        it = field.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        assertTrue(it.hasNext());
        assertEquals(2, it.next());
        assertTrue(it.hasNext());
        assertEquals(3, it.next());
        assertFalse(it.hasNext());

        assertFalse(new SourceMapField("dne.dne", () -> root).iterator().hasNext());
    }

    @SuppressWarnings("unchecked")
    public void testHasValue() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        b.put("c", new ArrayList<>(List.of(10, 11, 12)));
        SourceMapField field = new SourceMapField("a.b.c", () -> root);
        assertFalse(field.hasValue(v -> (Integer) v < 10));
        assertTrue(field.hasValue(v -> (Integer) v <= 10));

        root.clear();
        a.clear();
        a.put("null", null);
        a.put("b", List.of(1, 2, 3, 4));
        root.put("a", a);
        field = new SourceMapField("a.b", () -> root);
        assertTrue(field.hasValue(x -> (Integer) x % 2 == 0));
        assertFalse(field.hasValue(x -> (Integer) x > 4));
        assertFalse(new SourceMapField("d.e", () -> root).hasValue(Objects::isNull));
        assertTrue(new SourceMapField("a.null", () -> root).hasValue(Objects::isNull));
        assertFalse(new SourceMapField("a.null2", () -> root).hasValue(Objects::isNull));
    }

    public void testGetIndex() {
        Map<String, Object> root = new HashMap<>();
        root.put("a", Map.of("b", List.of(1, 2, 3, 5), "c", "foo"));
        SourceMapField field = new SourceMapField("a.b", () -> root);
        assertEquals(5, field.get(3, 100));
        assertEquals(100, new SourceMapField("c.d", () -> root).get(3, 100));
        assertEquals("bar", new SourceMapField("a.c", () -> root).get(1, "bar"));
        assertEquals("foo", new SourceMapField("a.c", () -> root).get(0, "bar"));
    }
}
