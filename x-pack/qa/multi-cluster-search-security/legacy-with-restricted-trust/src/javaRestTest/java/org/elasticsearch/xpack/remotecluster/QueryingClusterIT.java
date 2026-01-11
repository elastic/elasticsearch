/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import static org.hamcrest.Matchers.equalTo;

/**
 * Phase 2: Querying Cluster Tests (Cross-Cluster Search)
 * <p>
 * This test class runs against the querying (local) cluster which has a
 * cross-cluster search connection to the fulfilling cluster.
 * <p>
 * These tests depend on the data created by FulfillingClusterIT, which must run first.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class QueryingClusterIT extends ESRestTestCase {

    @ClassRule
    public static TestRule clusterRule = Clusters.clusterRule();

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected String getTestRestCluster() {
        return Clusters.queryingCluster().getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(Clusters.TEST_USER, new SecureString(Clusters.TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Test that cross-cluster search works with restricted trust SSL configuration.
     */
    public void testRemoteAccessPortFunctions() throws Exception {
        Request searchRequest = new Request("GET", "/" + Clusters.REMOTE_CLUSTER_ALIAS + ":test_idx/_search");
        Response response = client().performRequest(searchRequest);
        assertOK(response);
        ObjectPath responseObj = ObjectPath.createFromResponse(response);
        int totalHits = responseObj.evaluate("hits.total.value");
        assertThat(totalHits, equalTo(1));
    }
}
