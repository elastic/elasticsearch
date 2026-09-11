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

import java.util.List;

/**
 * Parameterized integration tests for standalone TSV files.
 * Each csv-spec test is run against every configured storage backend.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class TsvFormatSpecIT extends AbstractCsvExternalSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = UNCOMPRESSED_BWC_MATRIX_POLICY;

    public TsvFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "tsv");
    }

    @Override
    protected BwcMatrixPolicy bwcMatrixPolicy() {
        return BWC_MATRIX_POLICY;
    }

    // external-basic.csv-spec is dropped for TSV: its multi-value queries (MV_EXPAND / MV_COUNT on the
    // employees bracket columns) assume brackets parsing, which is no longer the default. Scalar
    // coverage comes from csv-basic.csv-spec (bracket-free employees twin) and multi-value coverage
    // from tsv-multivalue.csv-spec. The multifile specs only project scalar columns, so they parse
    // correctly under the default (tab delimiter, no column misalignment). external-heavy-aggregates
    // uses only the bracket-free employees_no_mv twin, so it parses under TSV's default too.
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests(
            BWC_MATRIX_POLICY,
            "/csv-basic.csv-spec",
            "/csv-declared-schema.csv-spec",
            "/datasources/external-declared-schema.csv-spec",
            "/csv-declared-schema-multifile.csv-spec",
            "/datasources/external-heavy-aggregates.csv-spec",
            "/datasources/external-multifile.csv-spec",
            "/datasources/external-multifile-resolution.csv-spec",
            "/tsv-multivalue.csv-spec"
        );
    }
}
