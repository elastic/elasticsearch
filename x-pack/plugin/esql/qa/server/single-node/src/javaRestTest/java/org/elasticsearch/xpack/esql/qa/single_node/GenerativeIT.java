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
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;
import org.junit.ClassRule;

/**
 * This test generates random queries, runs them against the CSV test dataset and checks that they don't throw unexpected exceptions.
 *
 * If muted, please:
 * <ul>
 * <li>see the error message reported in the failure and the corresponding query (it's in the logs right before the error)</li>
 * <li>update the corresponding issue with the query (if there is no issue for that failure yet, create one)</li>
 * <li>add a pattern that matches the error message to {@link GenerativeRestTest#ALLOWED_ERRORS}; also link the issue</li>
 * <li>unmute (and possibly check that the test doesn't fail anymore)</li>
 * </ul>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GenerativeIT extends GenerativeRestTest {
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
