/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import java.util.Arrays;
import java.util.List;

public class XmlTextStructureFinderFactoryTests extends TextStructureTestCase {

    private final TextStructureFinderFactory factory = new XmlTextStructureFinderFactory();

    // No need to check NDJSON because it comes earlier in the order we check formats

    public void testCanCreateFromSampleGivenXml() {

        assertTrue(factory.canCreateFromSample(explanation, XML_SAMPLE, 0.0));
    }

    public void testCanCreateFromMessages() {
        List<String> messages = Arrays.asList(XML_SAMPLE.split("\n\n"));
        assertTrue(factory.canCreateFromMessages(explanation, messages, 0.0));
    }

    public void testCanCreateFromMessages_multipleXmlDocsPerMessage() {
        List<String> messages = List.of(XML_SAMPLE, XML_SAMPLE, XML_SAMPLE);
        assertFalse(factory.canCreateFromMessages(explanation, messages, 0.0));
    }

    public void testCanCreateFromMessages_emptyMessages() {
        List<String> messages = List.of("", "", "");
        assertFalse(factory.canCreateFromMessages(explanation, messages, 0.0));
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
