/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.util.Collections;

public class XmlFileStructureFinderTests extends FileStructureTestCase {

    private FileStructureFinderFactory factory = new XmlFileStructureFinderFactory();

    public void testCreateConfigsGivenGoodXml() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, XML_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        FileStructureFinder structureFinder = factory.createFromSample(explanation, XML_SAMPLE, charset, hasByteOrderMarker,
            FileStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT, FileStructureOverrides.EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);

        FileStructure structure = structureFinder.getStructure();

        assertEquals(FileStructure.Format.XML, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\s*<log4j:event", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJodaTimestampFormats());
    }
}
