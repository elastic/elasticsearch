/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.TestIngestDocument.replaceValidator;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class IngestSourceAndMetadataTests extends ESTestCase {

    IngestSourceAndMetadata map;

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
        map = new IngestSourceAndMetadata(new HashMap<>(), metadata, null, null);
        assertEquals("myIndex", map.getIndex());
        map.setIndex("myIndex2");
        assertEquals("myIndex2", map.getIndex());

        assertEquals("myId", map.getId());
        map.setId("myId2");
        assertEquals("myId2", map.getId());

        assertEquals("myRouting", map.getRouting());
        map.setRouting("myRouting2");
        assertEquals("myRouting2", map.getRouting());

        assertEquals(20, map.getVersion());
        map.setVersion(10);
        assertEquals(10, map.getVersion());

        assertEquals("internal", map.getVersionType());
        map.setVersionType("external_gte");
        assertEquals("external_gte", map.getVersionType());

        assertEquals(Map.of("foo", "bar"), map.getDynamicTemplates());

        assertEquals(500, map.getIfSeqNo());
        assertEquals(10000, map.getIfPrimaryTerm());
    }

    public void testGetString() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_routing", "myRouting");
        Map<String, Object> source = new HashMap<>();
        source.put("str", "myStr");
        source.put("toStr", new Object() {
            @Override
            public String toString() {
                return "myToString()";
            }
        });
        source.put("missing", null);
        map = new IngestSourceAndMetadata(source, metadata, null, replaceValidator("_version", IngestSourceAndMetadata::longValidator));
        assertNull(map.getString("missing"));
        assertNull(map.getString("no key"));
        assertEquals("myToString()", map.getString("toStr"));
        assertEquals("myStr", map.getString("str"));
        assertEquals("myRouting", map.getString("_routing"));
    }

    public void testGetNumber() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", Long.MAX_VALUE);
        Map<String, Object> source = new HashMap<>();
        source.put("number", "NaN");
        source.put("missing", null);
        map = new IngestSourceAndMetadata(source, metadata, null, null);
        assertEquals(Long.MAX_VALUE, map.getNumber("_version"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.getNumber("number"));
        assertEquals("unexpected type for [number] with value [NaN], expected Number, got [java.lang.String]", err.getMessage());
        assertNull(map.getNumber("missing"));
        assertNull(map.getNumber("no key"));
    }

    public void testInvalidMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", Double.MAX_VALUE);
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestSourceAndMetadata(new HashMap<>(), metadata, null, null)
        );
        assertThat(err.getMessage(), containsString("_version may only be set to an int or a long but was ["));
        assertThat(err.getMessage(), containsString("] with type [java.lang.Double]"));
    }

    public void testSourceInMetadata() {
        Map<String, Object> source = new HashMap<>();
        source.put("_version", 25);
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestSourceAndMetadata(source, source, null, null)
        );
        assertEquals("Unexpected metadata key [_version] in source with value [25]", err.getMessage());
    }

    public void testExtraMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", 123);
        metadata.put("version", 567);
        metadata.put("routing", "myRouting");
        IllegalArgumentException err = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestSourceAndMetadata(new HashMap<>(), metadata, null, null)
        );
        assertEquals("Unexpected metadata keys [routing:myRouting, version:567]", err.getMessage());
    }

    public void testPutSource() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", 123);
        Map<String, Object> source = new HashMap<>();
        map = new IngestSourceAndMetadata(source, metadata, null, null);
    }

    public void testRemove() {
        String cannotRemove = "cannotRemove";
        String canRemove = "canRemove";
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(cannotRemove, "value");
        map = new IngestSourceAndMetadata(new HashMap<>(), metadata, null, Map.of(cannotRemove, (k, v) -> {
            if (v == null) {
                throw new IllegalArgumentException(k + " cannot be null or removed");
            }
        }, canRemove, (k, v) -> {}));
        String msg = "cannotRemove cannot be null or removed";
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.remove(cannotRemove));
        assertEquals(msg, err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> map.put(cannotRemove, null));
        assertEquals(msg, err.getMessage());

        err = expectThrows(IllegalArgumentException.class, () -> map.entrySet().iterator().next().setValue(null));
        assertEquals(msg, err.getMessage());

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
        map = new IngestSourceAndMetadata(source, metadata, null, replaceValidator("_version", IngestSourceAndMetadata::longValidator));

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if ("foo".equals(entry.getKey())) {
                entry.setValue("changed");
            } else if ("_version_type".equals(entry.getKey())) {
                entry.setValue("external_gte");
            }
        }
        assertEquals("changed", map.get("foo"));
        assertEquals("external_gte", map.getVersionType());

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

        assertNull(map.getVersionType());
        assertFalse(map.containsKey("baz"));
        assertTrue(map.containsKey("_version"));
        assertTrue(map.containsKey("foo"));
        assertTrue(map.containsKey("noz"));
        assertEquals(3, map.entrySet().size());
        assertEquals(3, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    public void testContainsValue() {
        map = new IngestSourceAndMetadata(Map.of("myField", "fieldValue"), Map.of("_version", 5678), null, null);
        assertTrue(map.containsValue(5678));
        assertFalse(map.containsValue(5679));
        assertTrue(map.containsValue("fieldValue"));
        assertFalse(map.containsValue("fieldValue2"));
    }

    public void testValidators() {
        map = new IngestSourceAndMetadata("myIndex", "myId", 1234, "myRouting", VersionType.EXTERNAL, null, new HashMap<>());
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.put("_index", 555));
        assertEquals("_index must be null or a String but was [555] with type [java.lang.Integer]", err.getMessage());
        assertEquals("myIndex", map.getIndex());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_id", 555));
        assertEquals("_id must be null or a String but was [555] with type [java.lang.Integer]", err.getMessage());
        assertEquals("myId", map.getId());
        map.put("_id", "myId2");
        assertEquals("myId2", map.getId());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_routing", 555));
        assertEquals("_routing must be null or a String but was [555] with type [java.lang.Integer]", err.getMessage());
        assertEquals("myRouting", map.getRouting());
        map.put("_routing", "myRouting2");
        assertEquals("myRouting2", map.getRouting());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version", "five-five-five"));
        assertEquals(
            "_version may only be set to an int or a long but was [five-five-five] with type [java.lang.String]",
            err.getMessage()
        );
        assertEquals(1234, map.getVersion());
        map.put("_version", 555);
        assertEquals(555, map.getVersion());

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", "vt"));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [vt] with type [java.lang.String]",
            err.getMessage()
        );
        assertEquals("external", map.getVersionType());
        map.put("_version_type", "internal");
        assertEquals("internal", map.getVersionType());
        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", VersionType.EXTERNAL.toString()));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [EXTERNAL] with type [java.lang.String]",
            err.getMessage()
        );
        err = expectThrows(IllegalArgumentException.class, () -> map.put("_version_type", VersionType.EXTERNAL));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [EXTERNAL] with type"
                + " [org.elasticsearch.index.VersionType$2]",
            err.getMessage()
        );
        assertEquals("internal", map.getVersionType());
        err = expectThrows(IllegalArgumentException.class, () -> map.setVersionType(VersionType.EXTERNAL.toString()));
        assertEquals(
            "_version_type must be a null or one of [internal, external, external_gte] but was [EXTERNAL] with type [java.lang.String]",
            err.getMessage()
        );

        err = expectThrows(IllegalArgumentException.class, () -> map.put("_dynamic_templates", "5"));
        assertEquals("_dynamic_templates must be a null or a Map but was [5] with type [java.lang.String]", err.getMessage());
        Map<String, String> dt = Map.of("a", "b");
        map.put("_dynamic_templates", dt);
        assertThat(dt, equalTo(map.getDynamicTemplates()));
    }

    public void testDefaultValidatorForAllMetadata() {
        for (IngestDocument.Metadata m : IngestDocument.Metadata.values()) {
            assertThat(IngestSourceAndMetadata.VALIDATORS, hasEntry(equalTo(m.getFieldName()), notNullValue()));
        }
        assertEquals(IngestDocument.Metadata.values().length, IngestSourceAndMetadata.VALIDATORS.size());
    }

    public void testHandlesAllVersionTypes() {
        Map<String, Object> md = new HashMap<>();
        md.put("_version", 1234);
        map = new IngestSourceAndMetadata(new HashMap<>(), md, null, null);
        assertNull(map.getVersionType());
        for (VersionType vt : VersionType.values()) {
            map.setVersionType(VersionType.toString(vt));
            assertEquals(VersionType.toString(vt), map.get("_version_type"));
        }

        for (VersionType vt : VersionType.values()) {
            map.put("_version_type", VersionType.toString(vt));
            assertEquals(vt.toString().toLowerCase(Locale.ROOT), map.getVersionType());
        }

        map.setVersionType(null);
        assertNull(map.getVersionType());
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
