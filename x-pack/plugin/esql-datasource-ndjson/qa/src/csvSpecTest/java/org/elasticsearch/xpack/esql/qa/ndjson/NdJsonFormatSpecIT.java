/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for standalone NDJSON files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class NdJsonFormatSpecIT extends AbstractNdJsonExternalSpecTestCase {

    /**
     * STRICT multi-file NDJSON tests still muted: the fixture's per-file schemas intentionally
     * diverge, which STRICT resolution rejects at resolution time ("Schema mismatch ... use
     * schema_resolution = union_by_name") before any page reaches the engine. A pre-existing
     * fixture/STRICT-semantics gap, unrelated to the empty-projection fix. The empty-projection
     * multi-file tests ({@code COUNT(*)} / {@code _file.*}-only) previously muted here now run.
     */
    private static final Set<String> SKIPPED_TESTS = Set.of(
        // STRICT resolution rejects the divergent-schema fixture at resolution time.
        "strictCount",
        "strictFilterAndSort",
        "strictSalaryStats",
        "strictAggregateByGender"
    );

    public NdJsonFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "ndjson");
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " not supported by NDJSON multi-file path (SchemaAdaptingIterator limitation)", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests(
            "/external-basic.csv-spec",
            "/external-heavy-aggregates.csv-spec",
            "/external-multifile.csv-spec",
            "/external-multifile-resolution.csv-spec",
            "/external-multivalue.csv-spec"
        );
    }
}
