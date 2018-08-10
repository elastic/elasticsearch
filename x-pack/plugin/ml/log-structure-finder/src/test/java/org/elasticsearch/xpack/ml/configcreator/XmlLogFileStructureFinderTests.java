/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import java.util.Collections;

public class XmlLogFileStructureFinderTests extends LogFileStructureTestCase {

    private LogFileStructureFinderFactory factory = new XmlLogFileStructureFinderFactory();

    public void testCreateConfigsGivenGoodXml() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, XML_SAMPLE));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        LogFileStructureFinder structureFinder = factory.createFromSample(explanation, XML_SAMPLE, charset, hasByteOrderMarker);

        LogFileStructure structure = structureFinder.getStructure();

        assertEquals(LogFileStructure.Format.XML, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\s*<log4j:event", structure.getMultilineStartPattern());
        assertNull(structure.getSeparator());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getTimestampFormats());
    }
}
