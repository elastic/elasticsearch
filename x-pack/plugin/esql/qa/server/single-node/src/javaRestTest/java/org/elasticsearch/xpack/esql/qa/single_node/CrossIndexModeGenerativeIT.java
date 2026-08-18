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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.qa.rest.generative.CrossIndexModeGenerativeRestTest;
import org.junit.ClassRule;

/**
 * Single-node integration test for cross-index-mode differential generative queries.
 *
 * <p>Runs random ES|QL pipelines against two index sets (reference: {@code standard} mode;
 * candidate: {@code columnar} mode) holding identical data and asserts that the results match.
 * Divergences in failure/success, schema, or values indicate correctness bugs in one of the modes.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@TestLogging(value = "org.elasticsearch.xpack.esql.plugin.ComputeService", reason = "see query plans on failure")
public class CrossIndexModeGenerativeIT extends CrossIndexModeGenerativeRestTest {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean supportsSourceFieldMapping() {
        return cluster.getNumNodes() == 1;
    }
}
