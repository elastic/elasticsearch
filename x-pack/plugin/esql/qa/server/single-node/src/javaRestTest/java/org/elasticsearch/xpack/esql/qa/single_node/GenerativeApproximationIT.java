/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeApproximationRestTest;
import org.junit.Before;
import org.junit.ClassRule;

import java.nio.file.Path;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GenerativeApproximationIT extends GenerativeApproximationRestTest {

    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    @Before
    public void checkCapability() {
        assumeTrue("query approximation should be enabled", EsqlCapabilities.Cap.APPROXIMATION_V7.isEnabled());
    }

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(CSV_DATA_PATH, spec -> {
        spec.plugin("inference-service-test").settings(nodeSpec -> LOGGING_CLUSTER_SETTINGS);
    });

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public GenerativeApproximationIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        // This suite runs with more than one node and three shards in serverless
        return cluster.getNumNodes() > 1;
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return cluster.getNumNodes() == 1;
    }

    @Override
    protected Path getCsvDataPath() {
        return CSV_DATA_PATH;
    }
}
