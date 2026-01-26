/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datalake.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvTests;
import org.elasticsearch.xpack.esql.SpecReader;

import java.net.URL;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * CSV-based tests for Iceberg integration.
 * <p>
 * This test class extends {@link CsvTests} to reuse the CSV test infrastructure
 * for testing Iceberg-specific functionality. It uses the same {@link CsvSpecReader}
 * and filtering mechanism to select only iceberg-*.csv-spec files.
 * <p>
 * Test files should be named with the pattern "iceberg-*.csv-spec" and placed in
 * the test resources directory (qa/testFixtures/src/main/resources/).
 */
public class IcebergCsvTests extends CsvTests {

    public IcebergCsvTests(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @ParametersFactory(argumentFormatting = "iceberg-csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        // Filter for iceberg-specific CSV spec files
        List<URL> urls = classpathResources("/iceberg-*.csv-spec");
        assertThat("Not enough iceberg specs found " + urls, urls, hasSize(greaterThan(0)));
        return SpecReader.readScriptSpec(urls, specParser());
    }
}
