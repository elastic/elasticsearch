/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class WriteFieldTests extends ESTestCase {

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
        WriteField wf = new WriteField("abc.d.ef", () -> map);
        assertTrue(wf.exists());

        assertEquals("nested", wf.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed", "d": { } } }
        d.remove("ef");
        assertEquals("missing", wf.get("missing"));
        // { "abc.d.ef", "flat", "abc": { "d.ef": "mixed" }
        // TODO(stu): this should be inaccessible
        abc.remove("d");
        assertEquals("missing", wf.get("missing"));

        // resolution at construction time
        wf = new WriteField("abc.d.ef", () -> map);
        assertEquals("mixed", wf.get("missing"));
        abc.remove("d.ef");
        assertEquals("missing", wf.get("missing"));

        wf = new WriteField("abc.d.ef", () -> map);
        // abc is still there
        assertEquals("missing", wf.get("missing"));
        map.remove("abc");
        assertEquals("missing", wf.get("missing"));

        wf = new WriteField("abc.d.ef", () -> map);
        assertEquals("flat", wf.get("missing"));
    }

    public void testExists() {
        Map<String, Object> a = new HashMap<>();
        a.put("b.c", null);
        assertTrue(new WriteField("a.b.c", () -> Map.of("a", a)).exists());

        a.clear();
        Map<String, Object> level1 = new HashMap<>();
        level1.put("null", null);
        a.put("level1", level1);
        a.put("null", null);
        // WriteField.leaf is null
        assertFalse(new WriteField("missing.leaf", () -> a).exists());

        // WriteField.leaf non-null but missing
        assertFalse(new WriteField("missing", () -> a).exists());

        // Check mappings with null values exist
        assertTrue(new WriteField("level1.null", () -> a).exists());
        assertTrue(new WriteField("null", () -> a).exists());
    }

    public void testMove() {
        String src = "a.b.c";
        String dst = "d.e.f";
        Map<String, Object> root = new HashMap<>();
        MapOfMaps branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));

        // All of dst exists, expect failure
        WriteField wf = new WriteField(src, () -> root);
        assertEquals("dst", new WriteField(dst, () -> root).get("dne"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> wf.move(dst));
        assertEquals("Cannot move to non-empty destination [" + dst + "]", err.getMessage());

        // All of dst other than leaf exists
        root.clear();
        branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));
        // dst missing value
        branches.get("e").remove("f");
        WriteField wf2 = new WriteField(src, () -> root);
        wf2.move(dst);
        assertEquals("src", wf2.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertFalse(branches.get("b").containsKey("c"));

        // Construct all of dst
        root.clear();
        branches = addPath(root, src, "src");
        WriteField wf3 = new WriteField(src, () -> root);
        wf3.move(dst);
        assertEquals("src", wf3.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertFalse(branches.get("b").containsKey("c"));
    }

    public void testOverwrite() {
        String src = "a.b.c";
        String dst = "d.e.f";
        Map<String, Object> root = new HashMap<>();
        MapOfMaps branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));

        WriteField wf = new WriteField(src, () -> root);
        assertEquals("dst", new WriteField(dst, () -> root).get("dne"));
        wf.overwrite(dst);
        assertEquals("src", wf.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertFalse(branches.get("b").containsKey("c"));

        root.clear();
        branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));
        // src missing value
        branches.get("b").remove("c");
        wf = new WriteField(src, () -> root);
        wf.overwrite(dst);
        assertEquals("dne", wf.get("dne"));
        assertEquals("dne", new WriteField(dst, () -> root).get("dne"));
        assertFalse(branches.get("e").containsKey("f"));
    }

    public void testRemove() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        b.put("c", "foo");
        a.put("b", b);
        root.put("a", a);
        WriteField wf = new WriteField("a.b.c", () -> root);
        assertEquals("foo", wf.get("dne"));
        wf.remove();
        assertEquals("dne", wf.get("dne"));
        assertThat(b.containsKey("c"), equalTo(false));

        root.clear();
        wf = new WriteField("a.b.c", () -> root);
        wf.remove();
        assertEquals("dne", wf.get("dne"));
    }

    @SuppressWarnings("unchecked")
    public void testSet() {
        Map<String, Object> root = new HashMap<>();
        root.put("a.b.c", "foo");
        WriteField wf = new WriteField("a.b.c", () -> root);
        wf.set("bar");
        assertEquals("bar", root.get("a.b.c"));

        root.clear();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        wf = new WriteField("a.b.c", () -> root);
        wf.set("bar");
        assertEquals("bar", b.get("c"));

        a.clear();
        wf = new WriteField("a.b.c", () -> root);
        wf.set("bar");
        assertEquals("bar", ((Map<String, Object>) a.get("b")).get("c"));

        a.clear();
        a.put("b", "foo");
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> new WriteField("a.b.c", () -> root).set("bar"));
        assertEquals("Segment [1:'b'] has value [foo] of type [java.lang.String]", err.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testSetCreate() {
        Map<String, Object> root = new HashMap<>();
        WriteField wf = new WriteField("a.b", () -> root);
        wf.set("foo");
        assertThat(root.keySet(), contains("a"));
        assertThat(((Map<String, Object>) root.get("a")).keySet(), contains("b"));
    }

    public void testAppend() {
        Map<String, Object> root = new HashMap<>();
        root.put("a.b.c", "foo");
        WriteField wf = new WriteField("a.b.c", () -> root);
        wf.append("bar");
        assertEquals(List.of("foo", "bar"), root.get("a.b.c"));

        root.clear();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        wf = new WriteField("a.b.c", () -> root);
        wf.append("bar");
        assertEquals(new ArrayList<>(List.of("bar")), b.get("c"));
    }

    public void testSizeIsEmpty() {
        Map<String, Object> root = new HashMap<>();
        WriteField wf = new WriteField("a.b.c", () -> root);
        assertTrue(wf.isEmpty());
        assertEquals(0, wf.size());

        root.put("a.b.c", List.of(1, 2));
        wf = new WriteField("a.b.c", () -> root);
        assertFalse(wf.isEmpty());
        assertEquals(2, wf.size());

        Map<String, Object> d = new HashMap<>();
        root.put("d", d);
        wf = new WriteField("d.e", () -> root);
        assertTrue(wf.isEmpty());
        assertEquals(0, wf.size());
        d.put("e", "foo");
        assertFalse(wf.isEmpty());
        assertEquals(1, wf.size());
    }

    public void testIterator() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);

        WriteField wf = new WriteField("a.b.c", () -> root);
        assertFalse(wf.iterator().hasNext());

        b.put("c", "value");
        Iterator<Object> it = wf.iterator();
        assertTrue(it.hasNext());
        assertEquals("value", it.next());
        assertFalse(it.hasNext());

        b.put("c", List.of(1, 2, 3));
        it = wf.iterator();
        assertTrue(it.hasNext());
        assertEquals(1, it.next());
        assertTrue(it.hasNext());
        assertEquals(2, it.next());
        assertTrue(it.hasNext());
        assertEquals(3, it.next());
        assertFalse(it.hasNext());

        assertFalse(new WriteField("dne.dne", () -> root).iterator().hasNext());
    }

    @SuppressWarnings("unchecked")
    public void testDeduplicate() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        b.put("c", new ArrayList<>(List.of(1, 1, 1, 2, 2, 2)));
        WriteField wf = new WriteField("a.b.c", () -> root);
        assertEquals(6, wf.size());
        wf.deduplicate();
        assertEquals(2, wf.size());
        List<Object> list = (List<Object>) wf.get(Collections.emptyList());
        assertTrue(list.contains(1));
        assertTrue(list.contains(2));

        assertEquals("missing", new WriteField("d.e", () -> root).deduplicate().get("missing"));
        assertEquals("missing", new WriteField("a.b.d", () -> root).deduplicate().get("missing"));
    }

    @SuppressWarnings("unchecked")
    public void testTransform() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        b.put("c", new ArrayList<>(List.of(1, 1, 1, 2, 2, 2)));
        b.put("x", "Doctor");
        WriteField wf = new WriteField("a.b.c", () -> root);
        wf.transform(v -> ((Integer) v) + 10);
        List<Object> list = (List<Object>) wf.get(Collections.emptyList());
        assertEquals(List.of(11, 11, 11, 12, 12, 12), list);

        assertTrue(new WriteField("d.e", () -> root).transform(x -> x + ", I presume").isEmpty());
        assertTrue(new WriteField("a.b.d", () -> root).transform(x -> x + ", I presume").isEmpty());
        assertEquals("Doctor, I presume", new WriteField("a.b.x", () -> root).transform(x -> x + ", I presume").get(null));
    }

    public void testRemoveValuesIf() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        b.put("c", new ArrayList<>(List.of(10, 10, 10, 20, 20, 20)));
        WriteField wf = new WriteField("a.b.c", () -> root);
        wf.removeValue(2);
        assertEquals(20, wf.get(2, 1000));

        wf.removeValuesIf(v -> (Integer) v > 10);
        assertEquals(2, wf.size());
        assertEquals(List.of(10, 10), wf.get(null));

        b.clear();
        wf = new WriteField("a.b.c", () -> root);
        wf.removeValuesIf(v -> (Integer) v > 10);
        assertNull(wf.get(null));
        wf.removeValue(10);
        assertNull(wf.get(null));

        b.put("c", 11);
        wf = new WriteField("a.b.c", () -> root);
        wf.removeValuesIf(v -> (Integer) v > 10);
        assertNull(wf.get(null));

        b.put("c", 5);
        wf.removeValuesIf(v -> (Integer) v > 10);
        assertEquals(5, wf.get(null));
        wf.removeValue(1);
        assertEquals(5, wf.get(null));
        wf.removeValue(0);
        assertNull(wf.get(null));

        root.clear();
        wf = new WriteField("a.b.c", () -> root);
        wf.removeValuesIf(v -> (Integer) v > 10);
        assertNull(wf.get(null));
        wf.removeValue(10);
        assertNull(wf.get(null));
    }

    public void testHasValue() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        a.put("b", b);
        root.put("a", a);
        b.put("c", new ArrayList<>(List.of(10, 11, 12)));
        WriteField wf = new WriteField("a.b.c", () -> root);
        assertFalse(wf.hasValue(v -> (Integer) v < 10));
        assertTrue(wf.hasValue(v -> (Integer) v <= 10));
        wf.append(9);
        assertTrue(wf.hasValue(v -> (Integer) v < 10));

        root.clear();
        a.clear();
        a.put("null", null);
        a.put("b", List.of(1, 2, 3, 4));
        root.put("a", a);
        wf = new WriteField("a.b", () -> root);
        assertTrue(wf.hasValue(x -> (Integer) x % 2 == 0));
        assertFalse(wf.hasValue(x -> (Integer) x > 4));
        assertFalse(new WriteField("d.e", () -> root).hasValue(Objects::isNull));
        assertTrue(new WriteField("a.null", () -> root).hasValue(Objects::isNull));
        assertFalse(new WriteField("a.null2", () -> root).hasValue(Objects::isNull));
    }

    public void testGetIndex() {
        Map<String, Object> root = new HashMap<>();
        root.put("a", Map.of("b", List.of(1, 2, 3, 5), "c", "foo"));
        WriteField wf = new WriteField("a.b", () -> root);
        assertEquals(5, wf.get(3, 100));
        assertEquals(100, new WriteField("c.d", () -> root).get(3, 100));
        assertEquals("bar", new WriteField("a.c", () -> root).get(1, "bar"));
        assertEquals("foo", new WriteField("a.c", () -> root).get(0, "bar"));
    }

    public MapOfMaps addPath(Map<String, Object> root, String path, Object value) {
        String[] elements = path.split("\\.");

        MapOfMaps containers = new MapOfMaps();
        Map<String, Object> container = root;

        for (int i = 0; i < elements.length - 1; i++) {
            Map<String, Object> next = new HashMap<>();
            assertNull(container.put(elements[i], next));
            assertNull(containers.put(elements[i], next));
            container = next;
        }

        container.put(elements[elements.length - 1], value);
        return containers;
    }

    private static class MapOfMaps {
        Map<String, Map<String, Object>> maps = new HashMap<>();

        public Object put(String key, Map<String, Object> value) {
            return maps.put(key, value);
        }

        public Map<String, Object> get(String key) {
            return maps.get(key);
        }

        public void putAll(MapOfMaps all) {
            maps.putAll(all.maps);
        }
    }
}
