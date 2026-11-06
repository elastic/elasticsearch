/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.rest.TestFeatureService;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class MultiClusterSearchYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static TestRule clusterRule = MultiClusterSearchClusters.CLUSTER_RULE;

    private static String remoteEsVersion = null;

    @BeforeClass
    public static void readRemoteClusterVersionAndSeedRemote() throws Exception {
        MultiClusterSearchClusters.beforeSuite();
        remoteEsVersion = MultiClusterSearchClusters.remoteClusterVersion();
    }

    @Override
    protected String getTestRestCluster() {
        return MultiClusterSearchClusters.localClusterHosts();
    }

    @Override
    protected ClientYamlTestExecutionContext createRestTestExecutionContext(
        ClientYamlTestCandidate clientYamlTestCandidate,
        ClientYamlTestClient clientYamlTestClient,
        final Set<String> nodesVersions,
        final TestFeatureService testFeatureService,
        final Set<String> osSet
    ) {
        /*
         * Since the esVersion is used to skip tests in ESClientYamlSuiteTestCase, we also take into account the
         * remote cluster version here. This is used to skip tests if some feature isn't available on the remote cluster yet.
         */
        final Set<String> commonVersions;
        if (remoteEsVersion == null || nodesVersions.contains(remoteEsVersion)) {
            commonVersions = nodesVersions;
        } else {
            var versionsCopy = new HashSet<>(nodesVersions);
            versionsCopy.add(remoteEsVersion);
            commonVersions = Collections.unmodifiableSet(versionsCopy);
        }

        // TODO: same for os and features; align with CcsCommonYamlTestSuiteIT if skips need remote OS/features.
        return new ClientYamlTestExecutionContext(
            clientYamlTestCandidate,
            clientYamlTestClient,
            randomizeContentType(),
            commonVersions,
            testFeatureService,
            osSet
        );
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public MultiClusterSearchYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }
}
