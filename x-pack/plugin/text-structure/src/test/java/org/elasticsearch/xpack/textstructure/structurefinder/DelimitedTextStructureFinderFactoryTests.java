/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

public class DelimitedTextStructureFinderFactoryTests extends TextStructureTestCase {

    private final TextStructureFinderFactory csvFactory = new DelimitedTextStructureFinderFactory(',', '"', 2, false);
    private final TextStructureFinderFactory tsvFactory = new DelimitedTextStructureFinderFactory('\t', '"', 2, false);
    private final TextStructureFinderFactory semiColonDelimitedfactory = new DelimitedTextStructureFinderFactory(';', '"', 4, false);
    private final TextStructureFinderFactory pipeDelimitedFactory = new DelimitedTextStructureFinderFactory('|', '"', 5, true);

    // CSV - no need to check NDJSON or XML because they come earlier in the order we check formats

    public void testCanCreateCsvFromSampleGivenCsv() {

        assertTrue(csvFactory.canCreateFromSample(explanation, CSV_SAMPLE, 0.0));
    }

    public void testCanCreateCsvFromSampleGivenTsv() {

        assertFalse(csvFactory.canCreateFromSample(explanation, TSV_SAMPLE, 0.0));
    }

    public void testCanCreateCsvFromSampleGivenSemiColonDelimited() {

        assertFalse(csvFactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateCsvFromSampleGivenPipeDelimited() {

        assertFalse(csvFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateCsvFromSampleGivenText() {

        assertFalse(csvFactory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }

    // TSV - no need to check NDJSON, XML or CSV because they come earlier in the order we check formats

    public void testCanCreateTsvFromSampleGivenTsv() {

        assertTrue(tsvFactory.canCreateFromSample(explanation, TSV_SAMPLE, 0.0));
    }

    public void testCanCreateTsvFromSampleGivenSemiColonDelimited() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateTsvFromSampleGivenPipeDelimited() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateTsvFromSampleGivenText() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }

    // Semi-colon delimited - no need to check NDJSON, XML, CSV or TSV because they come earlier in the order we check formats

    public void testCanCreateSemiColonDelimitedFromSampleGivenSemiColonDelimited() {

        assertTrue(semiColonDelimitedfactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateSemiColonDelimitedFromSampleGivenPipeDelimited() {

        assertFalse(semiColonDelimitedfactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreateSemiColonDelimitedFromSampleGivenText() {

        assertFalse(semiColonDelimitedfactory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }

    // Pipe delimited - no need to check NDJSON, XML, CSV, TSV or semi-colon delimited
    // values because they come earlier in the order we check formats

    public void testCanCreatePipeDelimitedFromSampleGivenPipeDelimited() {

        assertTrue(pipeDelimitedFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE, 0.0));
    }

    public void testCanCreatePipeDelimitedFromSampleGivenText() {

        assertFalse(pipeDelimitedFactory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));
    }
}
