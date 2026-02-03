/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

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
 * Integration tests for standalone Parquet files using S3 storage backend.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetSpecIT extends ParquetSpecTestCase {

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
        int urlCount = urls.size();
        assertTrue("No parquet-*.csv-spec files found", urlCount > 0);

        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, specParser());
        List<Object[]> parameterizedTests = new java.util.ArrayList<>();
        for (Object[] baseTest : baseTests) {
            for (StorageBackend backend : List.of(StorageBackend.S3)) {
                int baseLength = baseTest.length;
                Object[] parameterizedTest = new Object[baseLength + 1];
                System.arraycopy(baseTest, 0, parameterizedTest, 0, baseLength);
                parameterizedTest[baseLength] = backend;
                parameterizedTests.add(parameterizedTest);
            }
        }

        return parameterizedTests;
    }
}
