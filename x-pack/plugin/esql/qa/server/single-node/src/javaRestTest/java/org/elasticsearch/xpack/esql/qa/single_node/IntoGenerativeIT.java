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
import org.elasticsearch.xpack.esql.qa.rest.generative.IntoGenerativeRestRunner;
import org.junit.ClassRule;

/**
 * Single-node differential generative test for {@code INTO}: random pipelines are written to a dynamically mapped index
 * of a random mode and read back, and the rows have to match what the plain query returned.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class IntoGenerativeIT extends IntoGenerativeRestRunner {

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
