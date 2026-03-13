/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

/**
 * Base class for CSV format spec tests that need lookup indices (e.g. languages_lookup) for LOOKUP JOIN.
 * Extends {@link AbstractExternalSourceSpecTestCase} and loads a configurable set of indices in setup.
 */
public abstract class AbstractCsvFormatSpecTestCase extends AbstractExternalSourceSpecTestCase {

    private static boolean indicesLoaded = false;

    protected AbstractCsvFormatSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend,
        String format
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, format);
    }

    @Before
    public void loadIndices() throws IOException {
        if (indicesLoaded == false) {
            synchronized (AbstractCsvFormatSpecTestCase.class) {
                if (indicesLoaded == false) {
                    CsvTestsDataLoader.loadDatasetsIntoEs(client(), getLookupIndicesForExternalSourceTests());
                    indicesLoaded = true;
                }
            }
        }
    }

    /**
     * Indices to load for LOOKUP JOIN support in external source tests.
     * Subclasses may override to add more indices.
     */
    protected static List<String> getLookupIndicesForExternalSourceTests() {
        return List.of("languages_lookup");
    }
}
