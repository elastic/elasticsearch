/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceCodecEligibility;

import java.util.List;

/**
 * Parameterized integration tests for compressed TSV files (.tsv.gz, .tsv.zst, .tsv.zstd, .tsv.bz2, .tsv.bz).
 * Each csv-spec test is run against every configured storage backend and compression format.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class TsvCompressedFormatSpecIT extends AbstractCsvExternalSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = COMPRESSED_BWC_MATRIX_POLICY;
    private static final List<String> COMPRESSED_FORMATS = EsqlDataSourceCodecEligibility.textCompressionFormats("tsv");

    public TsvCompressedFormatSpecIT(
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

    @Override
    protected BwcMatrixPolicy bwcMatrixPolicy() {
        return BWC_MATRIX_POLICY;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        // external-basic's multi-value queries assume brackets parsing, no longer the default. Use the
        // scalar twin (csv-basic); the multifile specs project only scalar columns, so they parse under
        // the default for TSV (tab delimiter — no misalignment). tsv-multivalue covers the explicit
        // brackets opt-in on bracket data plus the literal-string read under the new default.
        return readExternalSpecTestsWithFormats(
            BWC_MATRIX_POLICY,
            COMPRESSED_FORMATS,
            "/csv-basic.csv-spec",
            "/csv-declared-schema.csv-spec",
            "/datasources/external-declared-schema.csv-spec",
            "/csv-declared-schema-multifile.csv-spec",
            "/datasources/external-multifile.csv-spec",
            "/datasources/external-multifile-resolution.csv-spec",
            "/tsv-multivalue.csv-spec"
        );
    }
}
