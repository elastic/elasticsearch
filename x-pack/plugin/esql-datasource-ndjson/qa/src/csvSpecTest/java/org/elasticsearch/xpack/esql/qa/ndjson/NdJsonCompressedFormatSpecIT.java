/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy.BwcTestId;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceCodecEligibility;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for compressed NDJSON files (.ndjson.gz, .ndjson.zst, .ndjson.zstd, .ndjson.bz2, .ndjson.bz).
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL, GCS) and compression format.
 * Each csv-spec test is run against every configured storage backend and compression format.
 * This class runs four csv-spec files and exceeds {@link EsqlSpecTestCase}'s 10-minute suite budget.
 */
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class NdJsonCompressedFormatSpecIT extends AbstractNdJsonExternalSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = BwcMatrixPolicy.compressed(
        StorageBackend.S3,
        "gzip",
        new BwcTestId("external-basic.csv-spec", "readAllEmployees")
    );
    private static final List<String> COMPRESSED_FORMATS = EsqlDataSourceCodecEligibility.textCompressionFormats("ndjson");

    /** Same SchemaAdaptingIterator limitation as the uncompressed NDJSON IT — see {@link NdJsonFormatSpecIT}. */
    private static final Set<String> SKIPPED_TESTS = Set.of(
        "strictCount",
        "strictFilterAndSort",
        "strictSalaryStats",
        "strictAggregateByGender",
        "ubnCount",
        "ubnExplicitCount",
        "readAllEmployeesMultiFile",
        "multiFileDistinctFileCount",
        "multiFileGroupByFile",
        "multiFileMetadataSizePositive"
    );

    public NdJsonCompressedFormatSpecIT(
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

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by NDJSON multi-file path (SchemaAdaptingIterator limitation)", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithFormats(
            BWC_MATRIX_POLICY,
            COMPRESSED_FORMATS,
            "/datasources/external-basic.csv-spec",
            "/datasources/external-declared-schema.csv-spec",
            "/datasources/external-multifile.csv-spec",
            "/datasources/external-multifile-resolution.csv-spec",
            "/datasources/external-multivalue.csv-spec",
            "/ndjson-declared-schema.csv-spec"
        );
    }
}
