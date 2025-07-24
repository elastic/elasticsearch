/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeTimeseriesRestTest;
import org.junit.ClassRule;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class BasicGenerativeTimeseriesRestIT extends GenerativeTimeseriesRestTest {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(spec -> spec.plugin("inference-service-test"));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
