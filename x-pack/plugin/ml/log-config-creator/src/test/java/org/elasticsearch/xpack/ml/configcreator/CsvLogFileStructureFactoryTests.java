/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class CsvLogFileStructureFactoryTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new CsvLogFileStructureFactory(TEST_TERMINAL);

    // No need to check JSON or XML because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenCsv() {

        assertTrue(factory.canCreateFromSample(CSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenTsv() {

        assertFalse(factory.canCreateFromSample(TSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(TEXT_SAMPLE));
    }
}
