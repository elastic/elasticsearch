/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureMatrix;

import java.util.List;

/**
 * Parameterized integration tests for compressed CSV files (.csv.gz, .csv.zst, .csv.zstd, .csv.bz2, .csv.bz).
 * Each csv-spec test is run against every configured storage backend and compression format.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class CsvCompressedFormatSpecIT extends AbstractDelimitedTextSpecTestCase {

    // Codecs come from the declaration, which also records that bzip2 is outside the GA text-format
    // codec surface and is therefore snapshot-only. See elastic/esql-planning#938.
    private static final List<String> COMPRESSED_FORMATS = FixtureMatrix.get().textCodecFormats("csv", Build.current().isSnapshot());

    public CsvCompressedFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String format,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, format);
    }

    /**
     * This suite routes its own spec set, so its exclusions are declared under its own token.
     * Without the override the lookup falls back to csv and would read another suite's
     * exclusion set, silently applying entries never written for this suite.
     */
    @Override
    protected String exclusionSuiteToken() {
        return "csv-compressed";
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        // external-basic / external-multifile read the multi-value employees fixture, which does not
        // parse as CSV under the default multi_value_syntax: none. Use the scalar twin (csv-basic),
        // csv-headerless, and csv-multifile (both opt into brackets explicitly where they read bracket
        // data) to restore the equivalent coverage.
        return readExternalSpecTestsWithFormatsForSuite(COMPRESSED_FORMATS, "csv-compressed");
    }
}
