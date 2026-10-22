/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Version;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy;
import org.elasticsearch.xpack.esql.qa.rest.BwcMatrixPolicy.BwcTestId;
import org.elasticsearch.xpack.esql.qa.rest.EsqlDataSourceMixedClusterTestSupport;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.List;

/**
 * Parameterized integration tests for standalone ORC files.
 * Each csv-spec test is run against every configured storage backend (S3, HTTP, LOCAL).
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class OrcFormatSpecIT extends AbstractExternalSourceSpecTestCase {

    private static final BwcMatrixPolicy BWC_MATRIX_POLICY = BwcMatrixPolicy.uncompressed(
        StorageBackend.S3,
        new BwcTestId("external-basic.csv-spec", "readAllEmployees")
    );

    public static ElasticsearchCluster cluster = EsqlDataSourceMixedClusterTestSupport.isBwcTest()
        ? Clusters.bwcTestCluster(() -> s3Fixture.getAddress())
        : Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = chainOuterRuleBeforeFixturesAndCluster(
        EsqlDataSourceMixedClusterTestSupport.outerBwcGuard(Version.V_9_5_0),
        cluster
    );

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
        return dataSourceTestClusterAddresses(cluster);
    }

    @Override
    protected BwcMatrixPolicy bwcMatrixPolicy() {
        return BWC_MATRIX_POLICY;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTests(BWC_MATRIX_POLICY, "/datasources/external-*.csv-spec", "/orc-*.csv-spec");
    }
}
