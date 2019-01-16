/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

public class XmlFileStructureFinderFactoryTests extends FileStructureTestCase {

    private FileStructureFinderFactory factory = new XmlFileStructureFinderFactory();

    // No need to check NDJSON because it comes earlier in the order we check formats

    public void testCanCreateFromSampleGivenXml() {

        assertTrue(factory.canCreateFromSample(explanation, XML_SAMPLE));
    }

    public void testCanCreateFromSampleGivenCsv() {

        assertFalse(factory.canCreateFromSample(explanation, CSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenTsv() {

        assertFalse(factory.canCreateFromSample(explanation, TSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenSemiColonDelimited() {

        assertFalse(factory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE));
    }

    public void testCanCreateFromSampleGivenPipeDelimited() {

        assertFalse(factory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
