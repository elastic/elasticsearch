/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

public class TsvLogFileStructureFactoryTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new TsvLogFileStructureFactory(TEST_TERMINAL);

    // No need to check JSON, XML or CSV because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenTsv() {

        assertTrue(factory.canCreateFromSample(TSV_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(TEXT_SAMPLE));
    }
}
