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
 * Phase 1: Fulfilling Cluster Setup Tests
 * <p>
 * This test class runs YAML tests against the fulfilling (remote) cluster to set up:
 * <ul>
 *   <li>Users and roles for cross-cluster search</li>
 *   <li>Test indices and data with DLS/FLS</li>
 * </ul>
 * <p>
 * This class runs before QueryingClusterIT (alphabetical ordering).
 * The data created here persists and is used by the subsequent QueryingClusterIT tests.
 */
public class FulfillingClusterSetupIT extends MultiClusterSearchSecurityTestSuite {

    public FulfillingClusterSetupIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters("fulfilling_cluster");
    }

    @Override
    protected String getTestRestCluster() {
        return Clusters.fulfillingCluster().getHttpAddresses();
    }
}
