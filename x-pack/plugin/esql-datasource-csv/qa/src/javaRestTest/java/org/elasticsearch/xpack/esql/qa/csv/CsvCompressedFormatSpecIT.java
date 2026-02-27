/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.util.List;

/**
 * Parameterized integration tests for compressed CSV files (.csv.gz, .csv.zst, .csv.zstd, .csv.bz2, .csv.bz).
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL, GCS) and compression format.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class CsvCompressedFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    private static final List<String> COMPRESSED_FORMATS = List.of("csv.gz", "csv.zst", "csv.zstd", "csv.bz2", "csv.bz");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public CsvCompressedFormatSpecIT(
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
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithFormats(COMPRESSED_FORMATS, "/external-basic.csv-spec");
    }
}
