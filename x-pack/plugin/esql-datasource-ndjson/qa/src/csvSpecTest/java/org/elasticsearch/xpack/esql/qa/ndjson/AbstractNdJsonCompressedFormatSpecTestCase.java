/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Shared base for the per-csv-spec-file compressed-NDJSON suites. Each concrete subclass covers a
 * single csv-spec file (mirroring the "one IT suite per csv-spec file" split in #152372) and should
 * complete well within the JVM suite timeout; the single {@code NdJsonCompressedFormatSpecIT} that
 * bundled every spec file crossed the 600s suite timeout because its
 * {@code formats × storage-backends} cross product produced a couple thousand tests.
 * <p>
 * This base holds the parts every compressed suite shares — the compressed-format matrix, the
 * multi-file skip list, the 8-arg constructor, and the {@link ThreadLeakFilters} — so each concrete
 * class is just a spec-file selector. All subclasses inherit the shared cluster declared on
 * {@link AbstractNdJsonExternalSpecTestCase}, so the cluster starts once and data is ingested once
 * for the whole JVM run.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
abstract class AbstractNdJsonCompressedFormatSpecTestCase extends AbstractNdJsonExternalSpecTestCase {

    // bzip2 is outside the GA text-format codec surface (uncompressed/gzip/zstd) and is rejected on release
    // builds, so .ndjson.bz2/.ndjson.bz are exercised on snapshot builds only. See elastic/esql-planning#938.
    protected static final List<String> COMPRESSED_FORMATS = Build.current().isSnapshot()
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

    protected AbstractNdJsonCompressedFormatSpecTestCase(
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
}
