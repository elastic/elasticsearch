/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for compressed NDJSON files (.ndjson.gz, .ndjson.zst, .ndjson.zstd, .ndjson.bz2, .ndjson.bz).
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL, GCS) and compression format.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class NdJsonCompressedFormatSpecIT extends AbstractNdJsonExternalSpecTestCase {

    // bzip2 is outside the GA text-format codec surface (uncompressed/gzip/zstd) and is rejected on release
    // builds, so .ndjson.bz2/.ndjson.bz are exercised on snapshot builds only. See elastic/esql-planning#938.
    private static final List<String> COMPRESSED_FORMATS = Build.current().isSnapshot()
        ? List.of("ndjson.gz", "ndjson.zst", "ndjson.zstd", "ndjson.bz2", "ndjson.bz")
        : List.of("ndjson.gz", "ndjson.zst", "ndjson.zstd");

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
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by NDJSON multi-file path (SchemaAdaptingIterator limitation)", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithFormats(
            COMPRESSED_FORMATS,
            "/external-basic.csv-spec",
            "/external-multifile.csv-spec",
            "/external-multifile-resolution.csv-spec",
            "/external-multivalue.csv-spec"
        );
    }
}
