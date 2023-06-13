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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

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

    public void testResolveNestedThenDotted() {
        Map<String, Object> root = Map.of("a", Map.of("b.c", 1));
        assertEquals(1, new WriteField("a.b.c", () -> root).get(null));
    }

    public void testResolveDottedThenNested() {
        Map<String, Object> root = Map.of("a.b", Map.of("c", 1));
        assertEquals(1, new WriteField("a.b.c", () -> root).get(null));
    }

    public void testResolvePrecedence() {
        Map<String, Object> root = new HashMap<>(
            Map.of("a", new HashMap<>(Map.of("b", Map.of("c", 1), "b.c", 2)), "a.b.c", 3, "a.b", Map.of("c", 4))
        );
        assertEquals(1, new WriteField("a.b.c", () -> root).get(null));
        ((Map<?, ?>) root.get("a")).remove("b");
        assertEquals(2, new WriteField("a.b.c", () -> root).get(null));
        root.remove("a");
        assertEquals(3, new WriteField("a.b.c", () -> root).get(null));
        root.remove("a.b.c");
        assertEquals(4, new WriteField("a.b.c", () -> root).get(null));
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

    public void testMoveString() {
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

    public void testMoveField() {
        String src = "a.b.c";
        String dst = "d.e.f";
        Map<String, Object> root = new HashMap<>();
        MapOfMaps branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));

        // All of dst exists, expect failure
        WriteField wf = new WriteField(src, () -> root);
        assertEquals("dst", new WriteField(dst, () -> root).get("dne"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> wf.move(new WriteField(dst, () -> root)));
        assertEquals("Cannot move to non-empty destination [" + dst + "]", err.getMessage());

        // All of dst other than leaf exists
        root.clear();
        branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));
        // dst missing value
        branches.get("e").remove("f");
        WriteField wf2 = new WriteField(src, () -> root);
        WriteField dstField = new WriteField(dst, () -> root);
        wf2.move(dstField);
        assertEquals("src", wf2.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertEquals("src", dstField.get("dne"));
        assertFalse(branches.get("b").containsKey("c"));

        // Construct all of dst
        root.clear();
        branches = addPath(root, src, "src");
        WriteField wf3 = new WriteField(src, () -> root);
        dstField = new WriteField(dst, () -> root);
        wf3.move(dstField);
        assertEquals("src", wf3.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertEquals("src", dstField.get("dne"));
        assertFalse(branches.get("b").containsKey("c"));
    }

    public void testMoveDocField() {
        Map<String, Object> root = new HashMap<>();
        WriteField rootFieldC = new WriteField("a.b.c", () -> root);
        rootFieldC.append(1).append(2);
        WriteField emptyRootField = new WriteField("a.b.x", () -> root);
        WriteField docs = new WriteField("a.d", () -> root);

        NestedDocument fooDoc = docs.doc();
        WriteField fooField = fooDoc.field("foo");
        fooField.set("myFoo");

        NestedDocument barDoc = docs.doc();
        WriteField barField = barDoc.field("bar");
        barField.set("myBar");

        // move between docs
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> fooField.move(barField));
        assertEquals("Cannot move to non-empty destination [bar]", err.getMessage());
        WriteField bar2Field = barDoc.field("bar2");
        fooField.move(bar2Field);
        assertEquals("myFoo", bar2Field.get("dne"));
        assertEquals(1, bar2Field.size());
        assertEquals("bar2", bar2Field.getName());
        assertTrue(fooDoc.isEmpty());
        assertEquals(2, barDoc.size());
        assertEquals("myFoo", barDoc.field("bar2").get("dne"));

        // move root field into doc
        WriteField bar3Field = docs.doc(1).field("bar3");
        rootFieldC.move(bar3Field);
        assertEquals(2, bar3Field.size());
        assertEquals(3, barDoc.size());

        // move doc field into root
        docs.doc(1).field("bar3").move(emptyRootField);
        assertThat(new WriteField("a.b.x", () -> root).get(null), equalTo(List.of(1, 2)));
    }

    public void testMoveObject() {
        Map<String, Object> root = new HashMap<>();
        WriteField rootFieldC = new WriteField("a.b.c", () -> root);
        rootFieldC.append(1).append(2);
        WriteField rootFieldD = new WriteField("a.b.d", () -> root);
        rootFieldC.move((Object) rootFieldD);
        assertThat(rootFieldC.get(null), equalTo(List.of(1, 2)));
        rootFieldD.move((Object) "a.b.e");
        WriteField rootFieldE = new WriteField("a.b.e", () -> root);
        assertThat(rootFieldE.get(null), equalTo(List.of(1, 2)));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> rootFieldE.move(1234));
        assertEquals("Cannot call move with [1234], must be String or WriteField, not [java.lang.Integer]", err.getMessage());
    }

    public void testOverwriteString() {
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

    public void testOverwriteField() {
        String src = "a.b.c";
        String dst = "d.e.f";
        Map<String, Object> root = new HashMap<>();
        MapOfMaps branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));

        WriteField wf = new WriteField(src, () -> root);
        WriteField dstField = new WriteField(dst, () -> root);
        assertEquals("dst", dstField.get("dne"));
        wf.overwrite(dstField);
        assertEquals("src", wf.get("dne"));
        assertEquals("src", new WriteField(dst, () -> root).get("dne"));
        assertEquals("src", dstField.get("dne"));
        assertFalse(branches.get("b").containsKey("c"));

        root.clear();
        branches = addPath(root, src, "src");
        branches.putAll(addPath(root, dst, "dst"));
        dstField = new WriteField(dst, () -> root);
        // src missing value
        branches.get("b").remove("c");
        wf = new WriteField(src, () -> root);
        wf.overwrite(dstField);
        assertEquals("dne", wf.get("dne"));
        assertEquals("dne", new WriteField(dst, () -> root).get("dne"));
        assertEquals("dne", dstField.get("dne"));
        assertTrue(dstField.isEmpty());
        assertFalse(branches.get("e").containsKey("f"));
    }

    public void testOverwriteDocField() {
        Map<String, Object> root = new HashMap<>();
        WriteField rootFieldC = new WriteField("a.b.c", () -> root);
        rootFieldC.append(1).append(2);
        WriteField rootFieldX = new WriteField("a.b.x", () -> root);
        rootFieldX.append(4).append(5);
        WriteField docs = new WriteField("a.d", () -> root);

        NestedDocument fooDoc = docs.doc();
        WriteField fooField = fooDoc.field("foo");
        fooField.set("myFoo");

        NestedDocument barDoc = docs.doc();
        WriteField barField = barDoc.field("bar");
        barField.set("myBar");

        // move between docs
        fooField.overwrite(barField);
        assertEquals("myFoo", barField.get("dne"));
        assertEquals(1, fooField.size());
        assertEquals("bar", fooField.getName());
        assertTrue(fooDoc.isEmpty());
        assertEquals(1, barDoc.size());
        assertEquals("myFoo", barDoc.field("bar").get("dne"));

        // move root field into doc
        rootFieldX.overwrite(docs.doc(1).field("bar"));
        assertEquals(2, barField.size());

        // move doc field into root
        docs.doc(1).field("bar").overwrite(rootFieldC);
        assertThat(new WriteField("a.b.c", () -> root).get(null), equalTo(List.of(4, 5)));
    }

    public void testOverwriteObject() {
        Map<String, Object> root = new HashMap<>();
        WriteField rootFieldC = new WriteField("a.b.c", () -> root);
        rootFieldC.append(1).append(2);
        WriteField rootFieldD = new WriteField("a.b.d", () -> root);
        rootFieldD.set(8);
        rootFieldC.overwrite((Object) rootFieldD);
        assertThat(rootFieldC.get(null), equalTo(List.of(1, 2)));
        WriteField rootFieldE = new WriteField("a.b.e", () -> root);
        rootFieldE.set(10);
        rootFieldD.overwrite((Object) "a.b.e");
        assertThat(rootFieldE.get(null), equalTo(List.of(1, 2)));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> rootFieldE.overwrite(1234));
        assertEquals("Cannot call overwrite with [1234], must be String or WriteField, not [java.lang.Integer]", err.getMessage());
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

    public void testRemoveListElement() {
        Map<String, Object> root = Map.of("a", new ArrayList<>(List.of(1, 2)));
        WriteField wf = new WriteField("a.0", () -> root);
        assertEquals(1, wf.get(null));

        wf.remove();
        assertEquals(2, wf.get(null));

        wf.remove();
        assertNull(wf.get(null));
        assertFalse(wf.exists());

        wf.remove();
        assertNull(wf.get(null));
        assertFalse(wf.exists());
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
        assertEquals(
            "cannot create child of [1:'b'] with value [foo] of type [java.lang.String] as part of path [a.b.c]",
            err.getMessage()
        );
    }

    public void testSetList() {
        Map<String, Object> root = new HashMap<>(Map.of("a.b", new ArrayList<>(List.of(1))));
        new WriteField("a.b.0", () -> root).set(42);
        assertEquals(List.of(42), root.get("a.b"));

        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> new WriteField("a.b.1", () -> root).set("foo"));
        assertEquals("[1] is out of bounds for array with length [1] as part of path [a.b.1]", err.getMessage());

        assertEquals(List.of(42), root.get("a.b"));
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
        wf.append("bar", false);
        assertEquals(new ArrayList<>(List.of("bar")), b.get("c"));
    }

    public void testAppendList() {
        Map<String, Object> root = new HashMap<>();
        root.put("a.b.c", new ArrayList<>(List.of("foo")));
        WriteField wf = new WriteField("a.b.c", () -> root);
        wf.append(List.of("bar", "baz"));
        assertEquals(List.of("foo", "bar", "baz"), root.get("a.b.c"));

        wf.append(List.of("bar", "baz", "qux"), false);
        assertEquals(List.of("foo", "bar", "baz", "qux"), root.get("a.b.c"));
    }

    public void testAppendListElement() {
        Map<String, Object> root = new HashMap<>();
        root.put("a.b.c", new ArrayList<>(List.of("foo")));
        WriteField wf = new WriteField("a.b.c.0", () -> root);
        wf.append("bar");
        assertEquals(List.of(List.of("foo", "bar")), root.get("a.b.c"));
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
        assertEquals(5, new WriteField("a.b", () -> root).get(3, 100));
        assertEquals(5, new WriteField("a.b.3", () -> root).get(100));
        assertEquals(100, new WriteField("c.d", () -> root).get(3, 100));
        assertEquals(100, new WriteField("c.d.3", () -> root).get(100));
        assertEquals("bar", new WriteField("a.c", () -> root).get(1, "bar"));
        assertEquals("bar", new WriteField("a.c.1", () -> root).get("bar"));
        assertEquals("foo", new WriteField("a.c", () -> root).get(0, "bar"));
        assertEquals("bar", new WriteField("a.c.0", () -> root).get("bar"));
    }

    @SuppressWarnings("unchecked")
    public void testDoc() {
        Map<String, Object> root = new HashMap<>();
        String path = "abc.def.hij";
        WriteField wf = new WriteField(path, () -> root);
        NestedDocument doc = wf.doc();
        doc.field("lmn.opq").set(5);
        doc.field("lmn.r").append("foo").append("bar");

        doc = wf.doc();
        doc.field("rst.uvw").set(6);
        doc.field("rst.x.y").append("baz").append("qux");

        doc = wf.doc();
        doc.field("alpha").set(7);

        List<Object> docs = getList(root, path);
        assertThat(docs, hasSize(3));

        assertThat(docs.get(0), instanceOf(Map.class));
        Map<String, Object> lmn = (Map<String, Object>) docs.get(0);
        assertEquals(5, getMap(lmn, "lmn").get("opq"));
        assertThat(getList(lmn, "lmn.r"), is(Arrays.asList("foo", "bar")));
        assertEquals(lmn, wf.doc(0).getDoc());

        assertThat(docs.get(1), instanceOf(Map.class));
        Map<String, Object> rst = (Map<String, Object>) docs.get(1);
        assertEquals(6, getMap(rst, "rst").get("uvw"));
        assertThat(getList(rst, "rst.x.y"), is(Arrays.asList("baz", "qux")));
        assertEquals(rst, wf.doc(1).getDoc());

        assertThat(docs.get(2), instanceOf(Map.class));
        Map<String, Object> alpha = (Map<String, Object>) docs.get(2);
        assertEquals(7, alpha.get("alpha"));
        assertEquals(alpha, wf.doc(2).getDoc());

        Iterator<NestedDocument> it = wf.docs().iterator();

        assertTrue(it.hasNext());
        doc = it.next();
        assertThat(doc.getDoc(), equalTo(lmn));

        assertTrue(it.hasNext());
        doc = it.next();
        assertThat(doc.getDoc(), equalTo(rst));

        assertTrue(it.hasNext());
        doc = it.next();
        assertThat(doc.getDoc(), equalTo(alpha));

        assertFalse(it.hasNext());
        expectThrows(NoSuchElementException.class, it::next);
    }

    public void testNonDoc() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        root.put("a", a);
        a.put("b", "foo");
        WriteField wf = new WriteField("a.b", () -> root);

        IllegalStateException err = expectThrows(IllegalStateException.class, wf::doc);
        assertEquals("Unexpected value [foo] of type [java.lang.String] at path [a.b], expected Map or List of Map", err.getMessage());

        err = expectThrows(IllegalStateException.class, () -> wf.doc(0));
        assertEquals("Unexpected value [foo] of type [java.lang.String] at path [a.b], expected Map or List of Map", err.getMessage());

        err = expectThrows(IllegalStateException.class, () -> wf.doc(2));
        assertEquals("Unexpected value [foo] of type [java.lang.String] at path [a.b], expected Map or List of Map", err.getMessage());

        err = expectThrows(IllegalStateException.class, wf::docs);
        assertEquals("Unexpected value [foo] of type [java.lang.String] at [a.b] for docs()", err.getMessage());

        wf.set(new HashMap<>());
        wf.append("foo");
        Iterator<NestedDocument> it = wf.docs().iterator();
        it.next();
        err = expectThrows(IllegalStateException.class, it::next);
        assertEquals("Unexpected value [foo] of type [java.lang.String] at [a.b] and index [1] for docs() iterator", err.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testDocList() {
        Map<String, Object> root = new HashMap<>();
        String path = "a.b";
        WriteField wf = new WriteField(path, () -> root);
        wf.doc().field("c.d").set(123);
        wf.doc().field("e.f").set(456);
        Object value = wf.get(null);

        assertThat(value, instanceOf(List.class));
        List<Map<String, Object>> list = (List<Map<String, Object>>) value;
        assertEquals(123, wf.doc(0).field("c.d").get(null));
        assertThat(list.get(0), equalTo(wf.doc(0).getDoc()));
        assertEquals(456, wf.doc(1).field("e.f").get(null));
        assertThat(list.get(1), equalTo(wf.doc(1).getDoc()));

        assertThat(list, hasSize(2));
        wf.doc(5).field("h.i").set(789);
        assertThat(list, hasSize(6));
        assertEquals(789, wf.doc(5).field("h.i").get(null));
        assertThat(wf.doc(5).getDoc(), equalTo(list.get(5)));
    }

    public void testDocMissing() {
        Map<String, Object> root = new HashMap<>();
        String path = "a.b";
        WriteField wf = new WriteField(path, () -> root);
        assertFalse(wf.docs().iterator().hasNext());
        NestedDocument doc = wf.doc(3);
        doc.field("c").set("foo");
        assertEquals("foo", wf.doc(3).field("c").get("dne"));
        assertThat(getList(root, "a.b").get(3), equalTo(Map.of("c", "foo")));
    }

    public void testDocWithOneMap() {
        Map<String, Object> root = new HashMap<>();
        Map<String, Object> a = new HashMap<>();
        Map<String, Object> b = new HashMap<>();
        root.put("a", a);
        a.put("b", b);
        b.put("c", "foo");
        String path = "a.b";
        WriteField wf = new WriteField(path, () -> root);
        Iterator<NestedDocument> it = wf.docs().iterator();
        assertTrue(it.hasNext());
        assertThat(it.next().getDoc(), equalTo(b));
        expectThrows(NoSuchElementException.class, it::next);

        wf.doc(2);
        NestedDocument doc = wf.doc(0);
        assertThat(getList(root, "a.b").get(0), equalTo(b));
    }

    public void testIllegalSetDoc() {
        Map<String, Object> root = new HashMap<>();
        WriteField wf = new WriteField("a", () -> root);
        NestedDocument doc = wf.doc();
        doc.field("foo").set("bar");
        WriteField wfb = new WriteField("b", () -> root);
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> wfb.set(doc));
        assertEquals("cannot set NestedDocument [{foo=bar}] as path [b]", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> wfb.append(doc));
        assertEquals("cannot append NestedDocument [{foo=bar}] to path [b]", err.getMessage());
    }

    public void testGetDottedFieldName() {
        Map<String, Object> root = Map.of("a.b.c", "foo");
        WriteField wf = new WriteField("a.b.c", () -> root);
        assertTrue(wf.exists());
        assertThat(wf.get(null), equalTo("foo"));

        assertThat(new WriteField("a.b", () -> root).get(null), equalTo(null));
    }

    public void testGetListElement() {
        Map<String, Object> root = Map.of("a", List.of("a1", "a2"));
        WriteField wf = new WriteField("a.0", () -> root);
        assertTrue(wf.exists());
        assertThat(wf.get(null), equalTo("a1"));

        assertThat(new WriteField("a.b", () -> root).get(null), equalTo(null));
    }

    public void testGetNestedListElement() {
        Map<String, Object> root = Map.of("a", Map.of("b", List.of("b1", "b2")));
        WriteField wf = new WriteField("a.b.0", () -> root);
        assertTrue(wf.exists());
        assertThat(wf.get(null), equalTo("b1"));
    }

    public void testGetObjectInList() {
        Map<String, Object> root = Map.of("a", List.of(Map.of("b", 1)));
        WriteField wf = new WriteField("a.0.b", () -> root);
        assertTrue(wf.exists());
        assertThat(wf.get(null), equalTo(1));
    }

    public void testGetDottedListElement() {
        Map<String, Object> root = Map.of("a.b", List.of("b1", "b2"));
        WriteField wf = new WriteField("a.b.0", () -> root);
        assertTrue(wf.exists());
        assertThat(wf.get(null), equalTo("b1"));
    }

    public void testGetNonExisting() {
        Map<String, Object> root = new HashMap<>();
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> new WriteField("a", () -> root).get());
        assertEquals("field [a] not present as part of path [a]", err.getMessage());
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

    private Map<String, Object> getMap(Map<String, Object> map, String path) {
        String[] elements = path.split("\\.");
        return getMap(map, elements, elements.length);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(Map<String, Object> map, String[] elements, int stop) {
        Object value;
        Map<String, Object> next = map;
        for (int i = 0; i < stop; i++) {
            value = next.get(elements[i]);
            assertThat(Arrays.toString(elements) + ":" + elements[i] + ":" + i, value, instanceOf(Map.class));
            next = (Map<String, Object>) value;
        }
        return next;
    }

    @SuppressWarnings("unchecked")
    public List<Object> getList(Map<String, Object> map, String path) {
        String[] elements = path.split("\\.");
        int last = elements.length - 1;
        Map<String, Object> inner = getMap(map, elements, last);
        Object value = inner.get(elements[last]);
        assertThat(path, value, instanceOf(List.class));
        return (List<Object>) value;
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
