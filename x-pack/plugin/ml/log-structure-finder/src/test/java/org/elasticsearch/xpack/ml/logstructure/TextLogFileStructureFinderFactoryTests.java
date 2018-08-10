/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructure;

public class TextLogFileStructureFinderFactoryTests extends LogFileStructureTestCase {

    private LogFileStructureFinderFactory factory = new TextLogFileStructureFinderFactory();

    // No need to check JSON, XML, CSV, TSV, semi-colon separated values or pipe
    // separated values because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenText() {

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
