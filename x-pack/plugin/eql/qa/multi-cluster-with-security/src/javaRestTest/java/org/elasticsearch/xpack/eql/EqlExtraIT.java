/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.eql.EqlExtraSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;

import static org.elasticsearch.test.eql.DataLoader.TEST_EXTRA_INDEX;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.LOCAL_CLUSTER;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.REMOTE_CLUSTER;
import static org.elasticsearch.xpack.eql.RemoteClusterTestUtils.remoteClusterIndex;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EqlExtraIT extends EqlExtraSpecTestCase {
    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(REMOTE_CLUSTER).around(LOCAL_CLUSTER);

    @Override
    protected String getTestRestCluster() {
        return LOCAL_CLUSTER.getHttpAddresses();
    }

    @Override
    protected String getRemoteCluster() {
        return REMOTE_CLUSTER.getHttpAddresses();
    }

    public EqlExtraIT(String query, String name, List<long[]> eventIds, String[] joinKeys, Integer size, Integer maxSamplesPerKey) {
        super(remoteClusterIndex(TEST_EXTRA_INDEX), query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }
}
