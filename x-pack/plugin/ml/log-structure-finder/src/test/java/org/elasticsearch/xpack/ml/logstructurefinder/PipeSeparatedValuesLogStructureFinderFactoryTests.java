/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

public class PipeSeparatedValuesLogStructureFinderFactoryTests extends LogStructureTestCase {

    private LogStructureFinderFactory factory = new PipeSeparatedValuesLogStructureFinderFactory();

    // No need to check JSON, XML, CSV, TSV or semi-colon separated values because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenPipeSeparatedValues() {

        assertTrue(factory.canCreateFromSample(explanation, PIPE_SEPARATED_VALUES_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
