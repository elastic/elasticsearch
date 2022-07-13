/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.notNullValue;

public class MetadataTests extends ESTestCase {
    Metadata md;

    public void testDefaultValidatorForAllMetadata() {
        for (IngestDocument.Metadata m : IngestDocument.Metadata.values()) {
            assertThat(Metadata.VALIDATORS, hasEntry(equalTo(m.getFieldName()), notNullValue()));
        }
        assertEquals(IngestDocument.Metadata.values().length, Metadata.VALIDATORS.size());
    }

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
        md = new TestMetadata(metadata, allowAllValidators("a", "b", "c", "d"));
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
        md = new TestMetadata(metadata, allowAllValidators("a", "b", "c", "d"));
        assertEquals(Long.MAX_VALUE, md.getNumber("a"));
        assertEquals(Double.MAX_VALUE, md.getNumber("b"));
        IllegalStateException err = expectThrows(IllegalStateException.class, () -> md.getNumber("c"));
        assertEquals("unexpected type for [c] with value [NaN], expected Number, got [java.lang.String]", err.getMessage());
        assertNull(md.getNumber("d"));
        assertNull(md.getNumber("no key"));
    }

    private static Map<String, Metadata.Validator> allowAllValidators(String... keys) {
        Map<String, Metadata.Validator> validators = new HashMap<>();
        for (String key : keys) {
            validators.put(key, (o, k, v) -> {});
        }
        return validators;
    }
}
