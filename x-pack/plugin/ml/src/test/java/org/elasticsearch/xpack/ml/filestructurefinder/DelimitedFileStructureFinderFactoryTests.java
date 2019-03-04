/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

public class DelimitedFileStructureFinderFactoryTests extends FileStructureTestCase {

    private FileStructureFinderFactory csvFactory = new DelimitedFileStructureFinderFactory(',', '"', 2, false);
    private FileStructureFinderFactory tsvFactory = new DelimitedFileStructureFinderFactory('\t', '"', 2, false);
    private FileStructureFinderFactory semiColonDelimitedfactory = new DelimitedFileStructureFinderFactory(';', '"', 4, false);
    private FileStructureFinderFactory pipeDelimitedFactory = new DelimitedFileStructureFinderFactory('|', '"', 5, true);

    // CSV - no need to check NDJSON or XML because they come earlier in the order we check formats

    public void testCanCreateCsvFromSampleGivenCsv() {

        assertTrue(csvFactory.canCreateFromSample(explanation, CSV_SAMPLE));
    }

    public void testCanCreateCsvFromSampleGivenTsv() {

        assertFalse(csvFactory.canCreateFromSample(explanation, TSV_SAMPLE));
    }

    public void testCanCreateCsvFromSampleGivenSemiColonDelimited() {

        assertFalse(csvFactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE));
    }

    public void testCanCreateCsvFromSampleGivenPipeDelimited() {

        assertFalse(csvFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE));
    }

    public void testCanCreateCsvFromSampleGivenText() {

        assertFalse(csvFactory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }

    // TSV - no need to check NDJSON, XML or CSV because they come earlier in the order we check formats

    public void testCanCreateTsvFromSampleGivenTsv() {

        assertTrue(tsvFactory.canCreateFromSample(explanation, TSV_SAMPLE));
    }

    public void testCanCreateTsvFromSampleGivenSemiColonDelimited() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE));
    }

    public void testCanCreateTsvFromSampleGivenPipeDelimited() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE));
    }

    public void testCanCreateTsvFromSampleGivenText() {

        assertFalse(tsvFactory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }

    // Semi-colon delimited - no need to check NDJSON, XML, CSV or TSV because they come earlier in the order we check formats

    public void testCanCreateSemiColonDelimitedFromSampleGivenSemiColonDelimited() {

        assertTrue(semiColonDelimitedfactory.canCreateFromSample(explanation, SEMI_COLON_DELIMITED_SAMPLE));
    }

    public void testCanCreateSemiColonDelimitedFromSampleGivenPipeDelimited() {

        assertFalse(semiColonDelimitedfactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE));
    }

    public void testCanCreateSemiColonDelimitedFromSampleGivenText() {

        assertFalse(semiColonDelimitedfactory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }

    // Pipe delimited - no need to check NDJSON, XML, CSV, TSV or semi-colon delimited
    // values because they come earlier in the order we check formats

    public void testCanCreatePipeDelimitedFromSampleGivenPipeDelimited() {

        assertTrue(pipeDelimitedFactory.canCreateFromSample(explanation, PIPE_DELIMITED_SAMPLE));
    }

    public void testCanCreatePipeDelimitedFromSampleGivenText() {

        assertFalse(pipeDelimitedFactory.canCreateFromSample(explanation, TEXT_SAMPLE));
    }
}
