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
import org.elasticsearch.test.eql.EqlDateNanosSpecTestCase;
import org.junit.ClassRule;

import java.util.List;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlDateNanosIT extends EqlDateNanosSpecTestCase {

    @ClassRule
    public static final ElasticsearchCluster cluster = EqlTestCluster.CLUSTER;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EqlDateNanosIT(
        String query,
        String name,
        List<long[]> eventIds,
        String[] joinKeys,
        Integer size,
        Integer maxSamplesPerKey,
        Boolean allowPartialSearchResults,
        Boolean allowPartialSequenceResults,
        Boolean expectShardFailures
    ) {
        super(
            query,
            name,
            eventIds,
            joinKeys,
            size,
            maxSamplesPerKey,
            allowPartialSearchResults,
            allowPartialSequenceResults,
            expectShardFailures
        );
    }
}
