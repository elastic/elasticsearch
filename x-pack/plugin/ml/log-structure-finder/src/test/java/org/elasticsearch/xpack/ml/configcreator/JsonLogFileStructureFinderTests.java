/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import java.util.Collections;

public class JsonLogFileStructureFinderTests extends LogFileStructureTestCase {

    private LogFileStructureFinderFactory factory = new JsonLogFileStructureFinderFactory();

    public void testCreateConfigsGivenGoodJson() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, JSON_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        LogFileStructureFinder structureFinder = factory.createFromSample(explanation, JSON_SAMPLE, charset, hasByteOrderMarker);

        LogFileStructure structure = structureFinder.getStructure();

        assertEquals(LogFileStructure.Format.JSON, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertNull(structure.getMultilineStartPattern());
        assertNull(structure.getSeparator());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getTimestampFormats());
    }
}
