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
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * Phase 1: Fulfilling Cluster Setup
 * <p>
 * This test class runs against the fulfilling (remote) cluster to set up test data.
 * It must run before QueryingClusterIT (alphabetical ordering ensures this).
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FulfillingClusterIT extends ESRestTestCase {

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
        return Clusters.fulfillingCluster().getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(Clusters.TEST_USER, new SecureString(Clusters.TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Index test data on the fulfilling cluster for cross-cluster search tests.
     */
    public void testSetupTestData() throws Exception {
        Request indexDocRequest = new Request("POST", "/test_idx/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
        Response response = client().performRequest(indexDocRequest);
        assertOK(response);
    }
}
