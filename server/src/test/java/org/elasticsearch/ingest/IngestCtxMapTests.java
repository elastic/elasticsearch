/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Metadata;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class IngestCtxMapTests extends ESTestCase {

    IngestCtxMap map;
    Metadata md;

    public void testSettersAndGetters() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_index", "myIndex");
        metadata.put("_id", "myId");
        metadata.put("_routing", "myRouting");
        metadata.put("_version", 20);
        metadata.put("_if_seq_no", 500);
        metadata.put("_if_primary_term", 10000);
        metadata.put("_version_type", "internal");
        metadata.put("_dynamic_templates", Map.of("foo", "bar"));
        map = new IngestCtxMap(new HashMap<>(), new IngestDocMetadata(metadata, null));
        md = map.getMetadata();
        assertEquals("myIndex", md.getIndex());
        md.setIndex("myIndex2");
        assertEquals("myIndex2", md.getIndex());

        assertEquals("myId", md.getId());
        md.setId("myId2");
        assertEquals("myId2", md.getId());

        assertEquals("myRouting", md.getRouting());
        md.setRouting("myRouting2");
        assertEquals("myRouting2", md.getRouting());

        assertEquals(20, md.getVersion());
        md.setVersion(10);
        assertEquals(10, md.getVersion());

        assertEquals("internal", md.getVersionType());
        md.setVersionType("external_gte");
        assertEquals("external_gte", md.getVersionType());

        assertEquals(Map.of("foo", "bar"), md.getDynamicTemplates());

        assertEquals(500, md.getIfSeqNo());
        assertEquals(10000, md.getIfPrimaryTerm());
    }

    public void testInvalidMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", Double.MAX_VALUE);
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestCtxMap(new HashMap<>(), new IngestDocMetadata(metadata, null))
        );
        assertThat(err.getMessage(), containsString("_version may only be set to an int or a long but was ["));
        assertThat(err.getMessage(), containsString("] with type [java.lang.Double]"));
    }

    public void testSourceInMetadata() {
        Map<String, Object> source = new HashMap<>();
        source.put("_version", 25);
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestCtxMap(source, new IngestDocMetadata(source, null))
        );
        assertEquals("unexpected metadata [_version:25] in source", err.getMessage());
    }

    public void testExtraMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", 123);
        metadata.put("version", 567);
        metadata.put("routing", "myRouting");
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestCtxMap(new HashMap<>(), new IngestDocMetadata(metadata, null))
        );
        assertEquals("Unexpected metadata keys [routing:myRouting, version:567]", err.getMessage());
    }

    public void testPutSource() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", 123);
        Map<String, Object> source = new HashMap<>();
        map = new IngestCtxMap(source, new IngestDocMetadata(metadata, null));
    }

    public void testRemoveSource() {
        Map<String, Object> source = new HashMap<>();
        source.put("abc", 123);
        source.put("def", 456);
        source.put("hij", 789);
        map = new IngestCtxMap(source, new IngestDocMetadata(new HashMap<>(Map.of("_version", 1L)), null));

        // Make sure there isn't a ConcurrentModificationException when removing a key from the iterator
        String removedKey = null;
        for (Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Object> entry = it.next();
            String key = entry.getKey();
            if (removedKey == null && source.containsKey(key)) {
                removedKey = key;
                it.remove();
            }
        }

        assertNotNull(removedKey);
        assertFalse(source.containsKey(removedKey));
        assertTrue(source.containsKey(removedKey.equals("abc") ? "def" : "abc"));
    }

    public void testRemove() {
        String cannotRemove = "_version"; // writable, but not *nullable*
        String canRemove = "_id"; // writable, and *nullable*
        map = new IngestCtxMap(new HashMap<>(), new IngestDocMetadata(new HashMap<>(Map.of(cannotRemove, 1L)), null));
        String msg = "_version cannot be removed";
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.remove(cannotRemove));
        assertEquals(msg, err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> map.put(cannotRemove, null));
        assertEquals("_version cannot be null", err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> map.entrySet().iterator().next().setValue(null));
        assertEquals("_version cannot be null", err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> {
            Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                it.next();
                it.remove();
            }
        });
        assertEquals(msg, err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> {
            Set<Map.Entry<String, Object>> set = map.entrySet();
            set.remove(map.entrySet().iterator().next());
        });
        assertEquals(msg, err.getMessage());

        map.put(canRemove, "value");
        assertEquals("value", map.get(canRemove));

        err = expectThrows(IllegalArgumentException.class, () -> map.clear());
        assertEquals(msg, err.getMessage());

        // depending on iteration order, this may have been removed, so put it back before checking the size
        map.put(canRemove, "value");
        assertEquals("value", map.get(canRemove));

        assertEquals(2, map.size());

        map.entrySet().remove(new TestEntry(canRemove, "value"));
        assertNull(map.get(canRemove));

        map.put(canRemove, "value");
        assertEquals("value", map.get(canRemove));
        map.remove(canRemove);
        assertNull(map.get(canRemove));

        map.put("sourceKey", "sourceValue");
        assertEquals("sourceValue", map.get("sourceKey"));
        map.entrySet().remove(new TestEntry("sourceKey", "sourceValue"));
        assertNull(map.get("sourceKey"));

        map.put("sourceKey", "sourceValue");
        assertEquals("sourceValue", map.get("sourceKey"));
        map.remove("sourceKey");
        assertNull(map.get("sourceKey"));
    }

    public void testEntryAndIterator() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", 123);
        metadata.put("_version_type", "external");
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        source.put("baz", "qux");
        source.put("noz", "zon");
        map = new IngestCtxMap(source, new IngestDocMetadata(metadata, null));
        md = map.getMetadata();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if ("foo".equals(entry.getKey())) {
                entry.setValue("changed");
            } else if ("_version_type".equals(entry.getKey())) {
                entry.setValue("external_gte");
            }
        }
        assertEquals("changed", map.get("foo"));
        assertEquals("external_gte", md.getVersionType());

        assertEquals(5, map.entrySet().size());
        assertEquals(5, map.size());

        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        expectThrows(IllegalStateException.class, it::remove);

        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            if ("baz".equals(entry.getKey())) {
                it.remove();
            } else if ("_version_type".equals(entry.getKey())) {
                it.remove();
            }
        }

        assertNull(md.getVersionType());
        assertFalse(map.containsKey("baz"));
        assertTrue(map.containsKey("_version"));
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("noz"));
        assertEquals(3, map.entrySet().size());
        assertEquals(3, map.size());

        // since an IngestCtxMap must have a _version (and the _version cannot be null), we can't just .clear()
        map.entrySet().removeIf(e -> e.getKey().equals("_version") == false);
        assertEquals(1, map.size());
    }

    public void testContainsValue() {
        map = new IngestCtxMap(Map.of("myField", "fieldValue"), new IngestDocMetadata(Map.of("_version", 5678), null));
        assertTrue(map.containsValue(5678));
        assertFalse(map.containsValue(5679));
        assertTrue(map.containsValue("fieldValue"));
        assertFalse(map.containsValue("fieldValue2"));
    }

    public void testValidators() {
        map = new IngestCtxMap("myIndex", "myId", 1234, "myRouting", VersionType.EXTERNAL, null, new HashMap<>());
        md = map.getMetadata();
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.put("_index", 555));
        assertEquals("_index [555] is wrong type, expected assignable to [java.lang.String], not [java.lang.Integer]", err.getMessage());
        assertEquals("myIndex", md.getIndex());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_id", 555));
        assertEquals("_id [555] is wrong type, expected assignable to [java.lang.String], not [java.lang.Integer]", err.getMessage());
        assertEquals("myId", md.getId());
        map.put("_id", "myId2");
        assertEquals("myId2", md.getId());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_routing", 555));
        assertEquals("_routing [555] is wrong type, expected assignable to [java.lang.String], not [java.lang.Integer]", err.getMessage());
        assertEquals("myRouting", md.getRouting());
        map.put("_routing", "myRouting2");
        assertEquals("myRouting2", md.getRouting());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version", "five-five-five"));
        assertEquals(
            "_version [five-five-five] is wrong type, expected assignable to [java.lang.Number], not [java.lang.String]",
            err.getMessage()
        );
        assertEquals(1234, md.getVersion());
        map.put("_version", 555);
        assertEquals(555, md.getVersion());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", "vt"));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [vt] with type [java.lang.String]",
            err.getMessage()
        );
        assertEquals("external", md.getVersionType());
        map.put("_version_type", "internal");
        assertEquals("internal", md.getVersionType());
        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", VersionType.EXTERNAL.toString()));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [EXTERNAL] with type [java.lang.String]",
            err.getMessage()
        );
        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", VersionType.EXTERNAL));
        assertEquals(
            "_version_type [EXTERNAL] is wrong type, expected assignable to [java.lang.String], not"
                + " [org.elasticsearch.index.VersionType$2]",
            err.getMessage()
        );
        assertEquals("internal", md.getVersionType());
        err = expectThrows(IllegalArgumentException.class, () -> md.setVersionType(VersionType.EXTERNAL.toString()));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [EXTERNAL] with type [java.lang.String]",
            err.getMessage()
        );

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_dynamic_templates", "5"));
        assertEquals(
            "_dynamic_templates [5] is wrong type, expected assignable to [java.util.Map], not [java.lang.String]",
            err.getMessage()
        );
        Map<String, String> dt = Map.of("a", "b");
        map.put("_dynamic_templates", dt);
        assertThat(dt, equalTo(md.getDynamicTemplates()));
    }

    public void testHandlesAllVersionTypes() {
        Map<String, Object> mdRawMap = new HashMap<>();
        mdRawMap.put("_version", 1234);
        map = new IngestCtxMap(new HashMap<>(), new IngestDocMetadata(mdRawMap, null));
        md = map.getMetadata();
        assertNull(md.getVersionType());
        for (VersionType vt : VersionType.values()) {
            md.setVersionType(VersionType.toString(vt));
            assertEquals(VersionType.toString(vt), map.get("_version_type"));
        }

        for (VersionType vt : VersionType.values()) {
            map.put("_version_type", VersionType.toString(vt));
            assertEquals(vt.toString().toLowerCase(Locale.ROOT), md.getVersionType());
        }

        md.setVersionType(null);
        assertNull(md.getVersionType());
    }

    public void testGetOrDefault() {
        map = new IngestCtxMap(Map.of("foo", "bar"), new IngestDocMetadata(Map.of("_version", 5L), null));

        // it does the expected thing for fields that are present
        assertThat(map.getOrDefault("_version", -1L), equalTo(5L));
        assertThat(map.getOrDefault("foo", "wat"), equalTo("bar"));

        // it does the expected thing for fields that are not present
        assertThat(map.getOrDefault("_version_type", "something"), equalTo("something"));
        assertThat(map.getOrDefault("baz", "quux"), equalTo("quux"));
    }

    public void testSourceHashMapIsNotCopied() {
        // a ctxMap will, as an optimization, just use the passed-in map reference
        Map<String, Object> source = Map.of("index", "id");

        map = new IngestCtxMap(source, new IngestDocMetadata(Map.of("_version", 5L), null));
        assertThat(map.getSource(), sameInstance(source));

        map = new IngestCtxMap(null, null, 10L, null, null, null, source);
        assertThat(map.getSource(), sameInstance(source));
    }

    private static class TestEntry implements Map.Entry<String, Object> {
        String key;
        Object value;

        TestEntry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
    }
}
