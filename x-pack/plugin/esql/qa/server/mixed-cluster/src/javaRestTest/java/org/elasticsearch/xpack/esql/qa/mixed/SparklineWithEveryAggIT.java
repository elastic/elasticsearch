/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.qa.rest.SparklineWithEveryAggTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class SparklineWithEveryAggIT extends SparklineWithEveryAggTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    public SparklineWithEveryAggIT(AggTestCase testCase) {
        super(testCase);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void assumeSparklineSupported() throws IOException {
        assumeTrue(
            requiredCapabilities() + " not supported by all nodes in this mixed-version cluster",
            clusterHasCapability("POST", "/_query", List.of(), requiredCapabilities()).orElse(false)
        );
    }
}
