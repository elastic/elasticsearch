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
import java.util.Map;

public class IngestSourceAndMetadataTests extends ESTestCase {

    IngestSourceAndMetadata map;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

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
        map = new IngestSourceAndMetadata(new HashMap<>(), metadata, null);
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
        map = new IngestSourceAndMetadata(source, metadata, null, IngestSourceAndMetadata.VALIDATORS);
        assertNull(map.getNumber("missing"));
        assertNull(map.getNumber("no key"));
        assertEquals("myToString()", map.getString("toStr"));
        assertEquals("myStr", map.getString("str"));
    }

    public void testGetNumber() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("_version", Long.MAX_VALUE);
        Map<String, Object> source = new HashMap<>();
        source.put("number", "NaN");
        source.put("missing", null);
        map = new IngestSourceAndMetadata(source, metadata, null, IngestSourceAndMetadata.VALIDATORS);
        assertEquals(Long.MAX_VALUE, map.getNumber("_version"));
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> map.getNumber("number"));
        assertEquals("unexpected type for [number] with value [NaN], expected Number, got [java.lang.String]", err.getMessage());
        assertNull(map.getNumber("missing"));
        assertNull(map.getNumber("no key"));
    }
}
