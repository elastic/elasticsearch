/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

public class SemiColonSeparatedValuesLogStructureFinderFactoryTests extends LogStructureTestCase {

    private LogStructureFinderFactory factory = new SemiColonSeparatedValuesLogStructureFinderFactory();

    // No need to check JSON, XML, CSV or TSV because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenSemiColonSeparatedValues() {

        assertTrue(factory.canCreateFromSample(explanation, SEMI_COLON_SEPARATED_VALUES_SAMPLE));
    }

    public void testCanCreateFromSampleGivenPipeSeparatedValues() {

        assertFalse(factory.canCreateFromSample(explanation, PIPE_SEPARATED_VALUES_SAMPLE));
    }

    public void testCanCreateFromSampleGivenText() {

        assertFalse(factory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
