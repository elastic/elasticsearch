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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
public class ParquetCompressedFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    private static final Map<String, String> CODEC_DIR_SUFFIXES = Map.of(
        "snappy",
        "standalone-snappy",
        "gzip",
        "standalone-gzip",
        "zstd",
        "standalone-zstd",
        "lz4raw",
        "standalone-lz4raw"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

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
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
        this.codecName = codecName;
    }

    @Override
    protected String fixturesBase() {
        String dir = CODEC_DIR_SUFFIXES.get(codecName);
        assert dir != null : "Unknown codec: " + codecName;
        return dir;
    }

    @Override
    protected String readerName() {
        return FormatNameResolver.READER_JAVA;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithCodecs(List.of("snappy", "gzip", "zstd", "lz4raw"), "/external-basic.csv-spec");
    }

    /**
     * Cross-products csv-spec tests with codec names and storage backends.
     * Returns parameter arrays with 8 arguments:
     * (fileName, groupName, testName, lineNumber, testCase, instructions, codecName, storageBackend).
     */
    private static List<Object[]> readExternalSpecTestsWithCodecs(List<String> codecs, String... specPatterns) throws Exception {
        List<Object[]> baseWithBackends = readExternalSpecTests(specPatterns);
        List<Object[]> parameterized = new ArrayList<>();
        for (Object[] baseTest : baseWithBackends) {
            for (String codec : codecs) {
                int len = baseTest.length;
                Object[] expanded = new Object[len + 1];
                System.arraycopy(baseTest, 0, expanded, 0, len - 1);
                expanded[len - 1] = codec;
                expanded[len] = baseTest[len - 1];
                parameterized.add(expanded);
            }
        }
        return parameterized;
    }
}
