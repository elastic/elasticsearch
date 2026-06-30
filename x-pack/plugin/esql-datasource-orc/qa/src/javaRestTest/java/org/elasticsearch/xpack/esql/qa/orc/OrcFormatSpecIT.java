/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;

import java.util.List;
import java.util.Set;

/**
 * Parameterized integration tests for standalone ORC files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class OrcFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    public OrcFormatSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "orc");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    // Migrated specs run via FROM <dataset> on S3 (the anonymous-capable fixture backs a dataset without
    // a cluster encryption key) and via the rebuilt EXTERNAL query on the other backends, so none are skipped.
    @Override
    protected Set<StorageBackend> datasetModeBackends() {
        return Set.of(StorageBackend.S3);
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests("/external-*.csv-spec");
    }
}
