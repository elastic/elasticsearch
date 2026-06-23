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

import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for standalone Parquet files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class ParquetFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public ParquetFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
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

    // Migrated specs run via FROM <dataset> on S3 and via the rebuilt EXTERNAL query on the other backends.
    // The reader: "java" this IT injects is redundant with the .parquet extension default (FormatNameResolver
    // maps a .parquet resource to the Java reader with no reader key), so FROM-on-S3 still uses the Java reader;
    // the explicit reader injection stays exercised on the rebuilt-EXTERNAL backends.
    @Override
    protected Set<StorageBackend> datasetModeBackends() {
        return Set.of(StorageBackend.S3);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec", "/parquet-*.csv-spec");
    }
}
