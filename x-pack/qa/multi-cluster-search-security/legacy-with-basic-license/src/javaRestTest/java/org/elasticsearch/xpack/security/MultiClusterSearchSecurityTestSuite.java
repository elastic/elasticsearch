/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * Base test suite for multi-cluster search security tests with basic license.
 * <p>
 * This test suite runs YAML REST tests against two clusters:
 * <ol>
 *   <li>Fulfilling cluster (remote) - where data is indexed</li>
 *   <li>Querying cluster (local) - which performs cross-cluster searches</li>
 * </ol>
 * <p>
 * Test execution order is critical: FulfillingClusterSetupIT must run before QueryingClusterIT
 * to ensure data is set up on the remote cluster before CCS tests run.
 * <p>
 * All test classes share the same cluster instances via the {@link Clusters} singleton.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class MultiClusterSearchSecurityTestSuite extends ESClientYamlSuiteTestCase {

    // Shared cluster rule - ensures clusters are started once for all test classes
    @ClassRule
    public static TestRule clusterRule = Clusters.clusterRule();

    public MultiClusterSearchSecurityTestSuite(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(Clusters.TEST_USER, new SecureString(Clusters.TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
