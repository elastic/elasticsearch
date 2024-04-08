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
import org.elasticsearch.test.eql.EqlSpecTestCase;
import org.junit.ClassRule;

import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlSpecIT extends EqlSpecTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = EqlTestCluster.CLUSTER;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EqlSpecIT(String query, String name, List<long[]> eventIds, String[] joinKeys, Integer size, Integer maxSamplesPerKey) {
        super(query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }
}
