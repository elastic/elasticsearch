/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

public class NdJsonTextStructureFinderFactoryTests extends TextStructureTestCase {

    private final TextStructureFinderFactory factory = new NdJsonTextStructureFinderFactory();

    public void testCanCreateFromSampleGivenNdJson() {

        assertTrue(factory.canCreateFromSample(explanation, NDJSON_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenXml() {

        assertFalse(factory.canCreateFromSample(explanation, XML_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenCsv() {

        assertFalse(factory.canCreateFromSample(explanation, CSV_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenTsv() {

        assertFalse(factory.canCreateFromSample(explanation, TSV_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenSemiColonDelimited() {

        assertFalse(factory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenPipeDelimited() {

        assertFalse(factory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }
}
