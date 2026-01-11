/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

/**
 * Phase 2: Querying Cluster Tests (Cross-Cluster Search)
 * <p>
 * This test class runs YAML tests against the querying (local) cluster which has a
 * cross-cluster search connection to the fulfilling cluster.
 * <p>
 * These tests depend on the data created by FulfillingClusterSetupIT, which must run first.
 */
public class QueryingClusterIT extends MultiClusterSearchSecurityTestSuite {

    public QueryingClusterIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters("querying_cluster");
    }

    @Override
    protected String getTestRestCluster() {
        return Clusters.queryingCluster().getHttpAddresses();
    }
}
