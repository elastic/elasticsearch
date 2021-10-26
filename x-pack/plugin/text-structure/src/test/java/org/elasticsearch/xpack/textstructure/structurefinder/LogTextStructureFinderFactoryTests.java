/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

public class LogTextStructureFinderFactoryTests extends TextStructureTestCase {

    private final TextStructureFinderFactory factory = new LogTextStructureFinderFactory();

    // No need to check NDJSON, XML, CSV, TSV, semi-colon delimited values or pipe
    // delimited values because they come earlier in the order we check formats

    public void testCanCreateFromSampleGivenText() {

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }
}
