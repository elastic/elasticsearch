/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class MetadataTests extends ESTestCase {
    Metadata md;
    private static final Metadata.FieldProperty<String> STRING_PROP = new Metadata.FieldProperty<>(String.class, true, true, null);

    public void testGetString() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("a", "A");
        metadata.put("b", new Object() {
            @Override
            public String toString() {
                return "myToString()";
            }
        });
        metadata.put("c", null);
        metadata.put("d", 1234);
        md = new Metadata(metadata, allowAllValidators("a", "b", "c", "d"));
        assertNull(md.getString("c"));
        assertNull(md.getString("no key"));
        assertEquals("myToString()", md.getString("b"));
        assertEquals("A", md.getString("a"));
        assertEquals("1234", md.getString("d"));
    }

    public void testGetNumber() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("a", Long.MAX_VALUE);
        metadata.put("b", Double.MAX_VALUE);
        metadata.put("c", "NaN");
        metadata.put("d", null);
        md = new Metadata(metadata, allowAllValidators("a", "b", "c", "d"));
        assertEquals(Long.MAX_VALUE, md.getNumber("a"));
        assertEquals(Double.MAX_VALUE, md.getNumber("b"));
        IllegalStateException err = expectThrows(IllegalStateException.class, () -> md.getNumber("c"));
        assertEquals("unexpected type for [c] with value [NaN], expected Number, got [java.lang.String]", err.getMessage());
        assertNull(md.getNumber("d"));
        assertNull(md.getNumber("no key"));
    }

    private static Map<String, Metadata.FieldProperty<?>> allowAllValidators(String... keys) {
        Map<String, Metadata.FieldProperty<?>> validators = new HashMap<>();
        for (String key : keys) {
            validators.put(key, Metadata.FieldProperty.ALLOW_ALL);
        }
        return Map.copyOf(validators);
    }

    public void testValidateMetadata() {
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new Metadata(Map.of("foo", 1), Map.of("foo", STRING_PROP))
        );
        assertEquals("foo [1] is wrong type, expected assignable to [java.lang.String], not [java.lang.Integer]", err.getMessage());

        err = expectThrows(
            IllegalArgumentException.class,
            () -> new Metadata(Map.of("foo", "abc", "bar", "def"), Map.of("foo", STRING_PROP))
        );
        assertEquals("Unexpected metadata keys [bar:def]", err.getMessage());
    }

    public void testIsAvailable() {
        md = new Metadata(Map.of("bar", "baz"), Map.of("foo", STRING_PROP, "bar", STRING_PROP));
        assertTrue(md.isAvailable("bar"));
        assertTrue(md.isAvailable("foo"));
    }

    public void testPut() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar")), Map.of("foo", STRING_PROP));
        assertEquals("bar", md.get("foo"));
        md.put("foo", "baz");
        assertEquals("baz", md.get("foo"));
        md.put("foo", null);
        assertNull(md.get("foo"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> md.put("foo", 1));
        assertEquals("foo [1] is wrong type, expected assignable to [java.lang.String], not [java.lang.Integer]", err.getMessage());
    }

    public void testContainsKey() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar")), Map.of("foo", STRING_PROP, "baz", STRING_PROP));
        assertTrue(md.containsKey("foo"));
        assertFalse(md.containsKey("baz"));
        assertTrue(md.isAvailable("baz"));
    }

    public void testContainsValue() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar")), Map.of("foo", STRING_PROP, "baz", STRING_PROP));
        assertTrue(md.containsValue("bar"));
        assertFalse(md.containsValue("foo"));
    }

    public void testRemove() {
        md = new Metadata(
            new HashMap<>(Map.of("foo", "bar", "baz", "qux")),
            Map.of("foo", STRING_PROP, "baz", new Metadata.FieldProperty<>(String.class, false, true, null))
        );
        assertTrue(md.containsKey("foo"));
        md.remove("foo");
        assertFalse(md.containsKey("foo"));

        assertTrue(md.containsKey("baz"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> md.remove("baz"));
        assertEquals("baz cannot be removed", err.getMessage());
    }

    public void testKeySetAndSize() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar", "baz", "qux")), Map.of("foo", STRING_PROP, "baz", STRING_PROP));
        Set<String> expected = Set.of("foo", "baz");
        assertEquals(expected, md.keySet());
        assertEquals(2, md.size());
        md.remove("foo");
        assertEquals(Set.of("baz"), md.keySet());
        assertEquals(1, md.size());
        md.put("foo", "abc");
        assertEquals(expected, md.keySet());
        assertEquals(2, md.size());
        md.remove("foo");
        md.remove("baz");
        assertEquals(Collections.emptySet(), md.keySet());
        assertEquals(0, md.size());
    }

    public void testTestClone() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar", "baz", "qux")), Map.of("foo", STRING_PROP, "baz", STRING_PROP));
        Metadata md2 = md.clone();
        md2.remove("foo");
        md.remove("baz");
        assertEquals("bar", md.get("foo"));
        assertNull(md2.get("foo"));
        assertNull(md.get("baz"));
        assertEquals("qux", md2.get("baz"));
    }

    public void testFieldPropertyType() {
        Metadata.FieldProperty<A> aProp = new Metadata.FieldProperty<>(A.class, true, true, null);
        aProp.check(Metadata.MapOperation.UPDATE, "a", new A());
        aProp.check(Metadata.MapOperation.INIT, "a", new A());
        aProp.check(Metadata.MapOperation.UPDATE, "a", new B());
        aProp.check(Metadata.MapOperation.INIT, "a", new B());

        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> aProp.check(Metadata.MapOperation.UPDATE, "a", new C())
        );
        String expected = "a [I'm C] is wrong type, expected assignable to [org.elasticsearch.script.MetadataTests$A], not"
            + " [org.elasticsearch.script.MetadataTests$C]";
        assertEquals(expected, err.getMessage());
        err = expectThrows(IllegalArgumentException.class, () -> aProp.check(Metadata.MapOperation.INIT, "a", new C()));
        assertEquals(expected, err.getMessage());
    }

    static class A {}

    static class B extends A {}

    static class C {
        @Override
        public String toString() {
            return "I'm C";
        }
    }

    public void testFieldPropertyNullable() {
        Metadata.FieldProperty<String> cantNull = new Metadata.FieldProperty<>(String.class, false, true, null);
        Metadata.FieldProperty<String> canNull = new Metadata.FieldProperty<>(String.class, true, true, null);

        IllegalArgumentException err;
        {
            Metadata.MapOperation op = Metadata.MapOperation.INIT;
            err = expectThrows(IllegalArgumentException.class, () -> cantNull.check(op, "a", null));
            assertEquals("a cannot be null", err.getMessage());
            canNull.check(op, "a", null);
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.UPDATE;
            err = expectThrows(IllegalArgumentException.class, () -> cantNull.check(op, "a", null));
            assertEquals("a cannot be null", err.getMessage());
            canNull.check(Metadata.MapOperation.UPDATE, "a", null);
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.REMOVE;
            err = expectThrows(IllegalArgumentException.class, () -> cantNull.check(op, "a", null));
            assertEquals("a cannot be removed", err.getMessage());
            canNull.check(Metadata.MapOperation.REMOVE, "a", null);
        }
    }

    public void testFieldPropertyWritable() {
        Metadata.FieldProperty<String> writable = new Metadata.FieldProperty<>(String.class, true, true, null);
        Metadata.FieldProperty<String> readonly = new Metadata.FieldProperty<>(String.class, true, false, null);

        String key = "a";
        String value = "abc";

        IllegalArgumentException err;
        {
            Metadata.MapOperation op = Metadata.MapOperation.INIT;
            writable.check(op, key, value);
            readonly.check(op, key, value);
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.UPDATE;
            err = expectThrows(IllegalArgumentException.class, () -> readonly.check(op, key, value));
            assertEquals("a cannot be updated", err.getMessage());
            writable.check(op, key, value);
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.REMOVE;
            err = expectThrows(IllegalArgumentException.class, () -> readonly.check(op, key, value));
            assertEquals("a cannot be removed", err.getMessage());
            writable.check(op, key, value);
        }
    }

    public void testFieldPropertyExtendedValidation() {
        Metadata.FieldProperty<Number> any = new Metadata.FieldProperty<>(Number.class, true, true, null);
        Metadata.FieldProperty<Number> odd = new Metadata.FieldProperty<>(Number.class, true, true, (k, v) -> {
            if (v.intValue() % 2 == 0) {
                throw new IllegalArgumentException("not odd");
            }
        });

        String key = "a";
        int value = 2;

        IllegalArgumentException err;
        {
            Metadata.MapOperation op = Metadata.MapOperation.INIT;
            any.check(op, key, value);
            err = expectThrows(IllegalArgumentException.class, () -> odd.check(op, key, value));
            assertEquals("not odd", err.getMessage());
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.UPDATE;
            any.check(op, key, value);
            err = expectThrows(IllegalArgumentException.class, () -> odd.check(op, key, value));
            assertEquals("not odd", err.getMessage());
        }

        {
            Metadata.MapOperation op = Metadata.MapOperation.REMOVE;
            any.check(op, key, value);
            odd.check(op, key, value);
        }
    }

    public void testImmutablePropertiesMap() {
        // a mutable metadata properties map is not permitted
        AssertionError e1 = expectThrows(AssertionError.class, () -> new Metadata(Map.of(), new HashMap<>()));
        assertEquals("properties map must be constructed via Map.of(...) or Map.copyOf(...)", e1.getMessage());

        // furthermore, for optimization reasons, just calling Collections.unmodifiableMap() is not sufficient
        AssertionError e2 = expectThrows(AssertionError.class, () -> new Metadata(Map.of(), Collections.unmodifiableMap(new HashMap<>())));
        assertEquals("properties map must be constructed via Map.of(...) or Map.copyOf(...)", e1.getMessage());

        // Map.of and Map.copyOf are permissible (the former for code that should be fast, and the latter for e.g. tests)
        new Metadata(Map.of(), Map.of());
        new Metadata(Map.of(), Map.copyOf(new HashMap<>()));
    }

    public void testGetOrDefault() {
        md = new Metadata(new HashMap<>(Map.of("foo", "bar")), Map.of("foo", STRING_PROP, "baz", STRING_PROP));
        assertThat(md.getOrDefault("foo", "wat"), equalTo("bar"));
        assertThat(md.getOrDefault("bar", "wat"), equalTo("wat"));
        assertThat(md.getOrDefault("yo", "wat"), equalTo("wat"));
    }
}
