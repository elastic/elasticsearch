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
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeForkRestTest;
import org.junit.ClassRule;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GenerativeForkIT extends GenerativeForkRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public GenerativeForkIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions,
        Mode mode
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, mode);
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
}
