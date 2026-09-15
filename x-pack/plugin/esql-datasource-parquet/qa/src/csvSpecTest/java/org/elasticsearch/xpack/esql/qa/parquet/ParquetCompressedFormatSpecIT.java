/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureMatrix;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy.BwcTestId;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceCodecEligibility;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.util.List;

/**
 * Parameterized integration tests for Parquet files with internal compression.
 * Each csv-spec test is run against every configured storage backend and every
 * supported Parquet internal compression codec (SNAPPY, GZIP, ZSTD, LZ4_RAW).
 * <p>
 * The fixtures are generated at build time by {@code ParquetFixtureGenerator} with the
 * corresponding codec and placed into codec-specific directories
 * ({@code standalone-snappy/}, {@code standalone-gzip/}, etc.).
 * This class runs two csv-spec files across four codecs and exceeds
 * {@link EsqlSpecTestCase}'s 10-minute suite budget.
 */
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetCompressedFormatSpecIT extends AbstractParquetExternalSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = BwcMatrixPolicy.compressed(
        StorageBackend.S3,
        "gzip",
        new BwcTestId("external-basic.csv-spec", "readAllEmployees")
    );
    private static final List<String> CODECS = EsqlDataSourceCodecEligibility.parquetCodecs(
        FixtureMatrix.get().parquetCodecs("parquet-compressed").toArray(String[]::new)
    );

    private final String codecName;

    public ParquetCompressedFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String codecName,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend);
        this.codecName = codecName;
    }

    @Override
    protected String fixturesBase() {
        return "standalone-" + codecName;
    }

    @Override
    protected String multifileSplitDir() {
        return "multifile_split-" + codecName;
    }

    @Override
    protected String guardCodecIdentity() {
        return EsqlDataSourceCodecEligibility.normalizeCodecToken(codecName);
    }

    @Override
    protected BwcMatrixPolicy bwcMatrixPolicy() {
        return BWC_MATRIX_POLICY;
    }

    /**
     * This suite routes its own spec set, so its exclusions are declared under its own token.
     * Without the override the lookup falls back to parquet and would read another suite's
     * exclusion set, silently applying entries never written for this suite.
     */
    @Override
    protected String exclusionSuiteToken() {
        return "parquet-compressed";
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithCodecsForSuite(BWC_MATRIX_POLICY, CODECS, "parquet-compressed");
    }
}
