/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FailFastWriteFieldTest extends ESTestCase {

    public void testGetNonExisting() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> new FailFastWriteField("foo", Map::of).get());
        assertEquals("field [foo] not present as part of path [foo]", err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> new FailFastWriteField("foo.bar", () -> Map.of("foo", "bar")).get());
        assertEquals("field [bar] not present as part of path [foo.bar]", err.getMessage());
        err = expectThrows(
            IllegalArgumentException.class,
            () -> new FailFastWriteField("foo.bar.baz", () -> Map.of("foo.bar", Map.of("bar", "baz"))).get()
        );
        assertEquals("field [foo] not present as part of path [foo.bar.baz]", err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> new FailFastWriteField("foo.bar", () -> Map.of("foo", List.of())).get());
        assertEquals("[bar] is not an integer, cannot be used as an index as part of path [foo.bar]", err.getMessage());
    }

    public void testCreate() {
        HashMap<String, Object> root = new HashMap<>();
        FailFastWriteField field = new FailFastWriteField("foo", () -> root);
        field.set("bar");
        assertEquals("bar", field.get());
        field.remove();

        field.setPath("foo.bar");
        field.set("baz");
        assertEquals("baz", field.get());
    }

    public void testExistsOutOfBounds() {
        Map<String, Object> root = Map.of("foo", List.of("bar"));
        assertFalse(new FailFastWriteField("foo.1", () -> root).exists());

        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new FailFastWriteField("foo.1", () -> root).exists(true)
        );
        assertEquals("[1] is out of bounds for array with length [1] as part of path [foo.1]", err.getMessage());
    }

    public void testExistsIndexNotANumber() {
        Map<String, Object> root = Map.of("foo", List.of("bar"));
        assertFalse(new FailFastWriteField("foo.bar", () -> root).exists());
        assertFalse(new FailFastWriteField("foo.bar", () -> root).exists(true));
    }

    public void testSetOutOfBounds() {
        Map<String, Object> root = Map.of("foo", List.of("bar"));

        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new FailFastWriteField("foo.1", () -> root).set("baz")
        );
        assertEquals("[bar] is not an integer, cannot be used as an index as part of path [foo.bar]", err.getMessage());
    }

    public void testSetNotANumber() {
        Map<String, Object> root = Map.of("foo", List.of("bar"));

        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new FailFastWriteField("foo.bar", () -> root).set("baz")
        );
        assertEquals("[1] is out of bounds for array with length [1] as part of path [foo.1]", err.getMessage());
    }

    public void testRemoveOutOfBounds() {
        Map<String, Object> root = Map.of("foo", List.of("bar"));

        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new FailFastWriteField("foo.1", () -> root).remove()
        );
        assertEquals("[1] is out of bounds for array with length [1] as part of path [foo.1]", err.getMessage());
    }
}
