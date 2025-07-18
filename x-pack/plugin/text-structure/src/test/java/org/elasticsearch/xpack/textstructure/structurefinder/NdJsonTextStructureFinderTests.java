/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Collections;
import java.util.Map;

public class NdJsonTextStructureFinderTests extends TextStructureTestCase {

    private final TextStructureFinderFactory factory = new NdJsonTextStructureFinderFactory();

    public void testCreateConfigsGivenGoodJson() throws Exception {
        testCreateConfigsGivenFlatJson();
        testCreateConfigsGivenNestedJson();
    }

    public void testCreateConfigsGivenFlatJson() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, NDJSON_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            NDJSON_SAMPLE,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();

        assertEquals(TextStructure.Format.NDJSON, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertNull(structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJodaTimestampFormats());
        assertEquals(Collections.singleton("properties"), structure.getMappings().keySet());
    }

    public void testCreateConfigsGivenNestedJson() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, NESTED_NDJSON_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            NESTED_NDJSON_SAMPLE,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();
        assertEquals(TextStructure.Format.NDJSON, structure.getFormat());
        assertEquals(charset, structure.getCharset());

        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }

        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJodaTimestampFormats());
        assertEquals(1, structure.getMappings().size());

        // Verify that the 'host' field is nested
        @SuppressWarnings("unchecked")
        Map<String, Object> props = (Map<String, Object>) structure.getMappings().get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> hostMapping = (Map<String, Object>) props.get("host");
        assertNotNull("Host should be a nested field", hostMapping);
        assertEquals("object", hostMapping.get("type"));

        // Verify 'host' properties
        @SuppressWarnings("unchecked")
        Map<String, Object> hostProperties = (Map<String, Object>) hostMapping.get("properties");
        assertTrue("Host should have 'id' property", hostProperties.containsKey("id"));
        assertTrue("Host should have 'category' property", hostProperties.containsKey("category"));
    }

}
