/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for standalone Parquet files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetFormatSpecIT extends AbstractParquetExternalSpecTestCase {

    public ParquetFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend);
    }

    // Migrated specs run via FROM <dataset> on S3 and via the rebuilt EXTERNAL query on the other backends.
    // The reader: "java" this IT injects is redundant with the .parquet extension default (FormatNameResolver
    // maps a .parquet resource to the Java reader with no reader key), so FROM-on-S3 still uses the Java reader;
    // the explicit reader injection stays exercised on the rebuilt-EXTERNAL backends.

    private static final Set<String> SKIPPED_TESTS = Set.of(
        // Filtering a column that schema reconciliation widened to keyword pushes the comparison
        // down and evaluates it against the file's PRE-widening type, so in the file where `code`
        // is declared integer the pushdown holds a Number while the literal is a BytesRef and the
        // cast fails with a 500 (ParquetPushedExpressions.evaluateComparison). Reading and
        // aggregating the same widened column both work -- only the pushed filter fails -- and ORC
        // passes this case against the same rows. Re-enable once a pushed comparison is evaluated
        // against the reconciled column type rather than the per-file declared type.
        "typeDriftFilterIsStringComparison"
    );

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        if (SKIPPED_TESTS.contains(testName)) {
            assumeTrue(testName + " blocked on the widened-column pushdown defect", false);
        }
        super.shouldSkipTest(testName);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec", "/parquet-*.csv-spec");
    }
}
