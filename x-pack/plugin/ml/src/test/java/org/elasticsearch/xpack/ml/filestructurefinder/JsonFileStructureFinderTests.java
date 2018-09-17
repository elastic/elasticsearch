/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.util.Collections;

public class JsonFileStructureFinderTests extends FileStructureTestCase {

    private FileStructureFinderFactory factory = new JsonFileStructureFinderFactory();

    public void testCreateConfigsGivenGoodJson() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, JSON_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        FileStructureFinder structureFinder = factory.createFromSample(explanation, JSON_SAMPLE, charset, hasByteOrderMarker,
            FileStructureOverrides.EMPTY_OVERRIDES);

        FileStructure structure = structureFinder.getStructure();

        assertEquals(FileStructure.Format.JSON, structure.getFormat());
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
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getTimestampFormats());
    }
}
