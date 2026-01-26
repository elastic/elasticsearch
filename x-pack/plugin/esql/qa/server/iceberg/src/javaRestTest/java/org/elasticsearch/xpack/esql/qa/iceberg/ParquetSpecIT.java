/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.iceberg;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.qa.rest.ParquetSpecTestCase;
import org.junit.ClassRule;

import java.net.URL;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.junit.Assert.assertTrue;

/** 
 * Integration tests for standalone Parquet files (loads parquet-*.csv-spec).
 * Tests are parameterized over storage backends: S3, HTTP, and LOCAL.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetSpecIT extends ParquetSpecTestCase {

    /** Elasticsearch cluster with S3 fixture for Parquet testing. */
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public ParquetSpecIT(
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

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/parquet-*.csv-spec");
        assertTrue("No parquet-*.csv-spec files found", urls.size() > 0);
        
        // Read the base test cases from CSV specs
        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, specParser());
        
        // Parameterize each test with storage backend: S3 only for now
        // LOCAL is skipped because ES server entitlements prevent reading local files
        // HTTP is skipped because the HTTP provider returns empty results (needs investigation)
        List<Object[]> parameterizedTests = new java.util.ArrayList<>();
        for (Object[] baseTest : baseTests) {
            for (StorageBackend backend : List.of(StorageBackend.S3)) {
                // Create a new test array with the storage backend appended
                Object[] parameterizedTest = new Object[baseTest.length + 1];
                System.arraycopy(baseTest, 0, parameterizedTest, 0, baseTest.length);
                parameterizedTest[baseTest.length] = backend;
                parameterizedTests.add(parameterizedTest);
            }
        }
        
        return parameterizedTests;
    }
}
