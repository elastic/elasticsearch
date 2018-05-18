/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class JsonLogFileStructureFactoryTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new JsonLogFileStructureFactory(TEST_TERMINAL);

    public void testCanCreateFromSampleGivenJson() {

        assertTrue(factory.canCreateFromSample(JSON_SAMPLE));
    }

    public void testCanCreateFromSampleGivenXml() {

        assertFalse(factory.canCreateFromSample(XML_SAMPLE));
    }

    public void testCanCreateFromSampleGivenCsv() {

        assertFalse(factory.canCreateFromSample(CSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenTsv() {

        assertFalse(factory.canCreateFromSample(TSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(TEXT_SAMPLE));
    }
}
