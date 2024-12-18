/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlRestValidationSkipUnavailableIT extends EsqlRestValidationIT {
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster, true);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Override
    protected void assertErrorMessageMaybe(String indexName, String errorMessage, int statusCode) throws IOException {
        assertValidRequestOnIndices(new String[] { indexName });
    }

}
