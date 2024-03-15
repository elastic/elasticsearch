/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.eql.EqlRestTestCase;
import org.junit.ClassRule;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlRestIT extends EqlRestTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = EqlTestCluster.CLUSTER;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
