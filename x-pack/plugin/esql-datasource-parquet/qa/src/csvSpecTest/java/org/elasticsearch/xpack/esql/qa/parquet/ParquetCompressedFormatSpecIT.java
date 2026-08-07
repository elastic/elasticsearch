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

import java.util.List;

/**
 * Parameterized integration tests for Parquet files with internal compression.
 * Each csv-spec test is run against every configured storage backend and every
 * supported Parquet internal compression codec (SNAPPY, GZIP, ZSTD, LZ4_RAW).
 * <p>
 * The fixtures are generated at build time by {@code ParquetFixtureGenerator} with the
 * corresponding codec and placed into codec-specific directories
 * ({@code standalone-snappy/}, {@code standalone-gzip/}, etc.).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetCompressedFormatSpecIT extends AbstractParquetExternalSpecTestCase {

    private static final List<String> CODECS = List.of("snappy", "gzip", "zstd", "lz4raw");

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

    // Migrated specs run via FROM <dataset> on S3 and via the rebuilt EXTERNAL query on the other backends.
    // The reader: "java" this IT injects is redundant with the .parquet extension default (the codec lives
    // inside the .parquet file, so the extension is unchanged), so FROM-on-S3 still uses the Java reader.

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithCodecs(CODECS, "/external-basic.csv-spec", "/external-multivalue.csv-spec");
    }
}
