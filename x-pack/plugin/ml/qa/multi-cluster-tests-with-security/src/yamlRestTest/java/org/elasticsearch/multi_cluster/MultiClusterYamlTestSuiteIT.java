/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multi_cluster;

import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE) // both suites now run in one JVM against an old-version remote cluster; account for slow VMs
@TestCaseOrdering(MultiClusterYamlTestSuiteIT.RemoteClusterFirstOrder.class)
public class MultiClusterYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = Clusters.TEST_USER;
    private static final String PASS = Clusters.TEST_PASSWORD;

    // The remote cluster must start first so the mixed cluster can resolve its transport endpoints for the
    // cross-cluster-search remote connection seeds.
    private static final ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    private static final ElasticsearchCluster mixedCluster = Clusters.mixedCluster(
        () -> Arrays.stream(remoteCluster.getTransportEndpoints().split(","))
            .map(endpoint -> "\"" + endpoint + "\"")
            .collect(Collectors.joining(",", "[", "]"))
    );

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(mixedCluster);

    private static Boolean usingRemoteCluster;

    public MultiClusterYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    /**
     * The {@code remote_cluster} suite indexes the source data that the {@code multi_cluster} suite queries over
     * cross-cluster search, so the REST client must be re-pointed at whichever cluster the current test targets.
     */
    @Before
    public void reinitializeClientsForCurrentCluster() throws Exception {
        if (usingRemoteCluster == null || usingRemoteCluster != isTargetingRemoteCluster()) {
            usingRemoteCluster = isTargetingRemoteCluster();
            closeClient();
            closeClients();
            initClient();
            initAndResetContext();
        }
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    private boolean isTargetingRemoteCluster() {
        return getTestCandidate().getTestPath().contains("remote_cluster");
    }

    @Override
    protected String getTestRestCluster() {
        return isTargetingRemoteCluster() ? remoteCluster.getHttpAddresses() : mixedCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Orders the {@code remote_cluster} suite before the {@code multi_cluster} suite so the remote source data exists
     * before the cross-cluster-search tests run. The original build enforced this by running the two suites as
     * separate, sequenced Gradle tasks.
     */
    public static class RemoteClusterFirstOrder implements Comparator<TestMethodAndParams> {
        @Override
        public int compare(TestMethodAndParams o1, TestMethodAndParams o2) {
            return Integer.compare(rank(o1), rank(o2));
        }

        private static int rank(TestMethodAndParams params) {
            if (params.getInstanceArguments().isEmpty()) {
                return 1;
            }
            return params.getInstanceArguments().getFirst().toString().contains("remote_cluster") ? 0 : 1;
        }
    }
}
