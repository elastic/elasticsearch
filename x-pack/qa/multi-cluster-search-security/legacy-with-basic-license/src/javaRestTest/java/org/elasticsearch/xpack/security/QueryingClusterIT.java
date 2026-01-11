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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    @Override
    public void test() throws IOException {
        if (Clusters.isProxyMode()) {
            List<String> skipList = getProxyModeSkipList();
            String testPath = getTestCandidate().getTestPath();
            if (skipList.stream().anyMatch(p -> matchesSkipListPattern(testPath, p))) {
                return;
            }
        }
        super.test();
    }

    /**
     * Tests to skip when running in proxy mode.
     * These tests assume sniff mode connection semantics.
     */
    private static List<String> getProxyModeSkipList() {
        var skipList = new ArrayList<String>();
        skipList.add("querying_cluster/10_basic/Add persistent remote cluster based on the preset cluster");
        skipList.add("querying_cluster/20_info/Add persistent remote cluster based on the preset cluster and check remote info");
        skipList.add("querying_cluster/20_info/Fetch remote cluster info for existing cluster");
        skipList.add("querying_cluster/70_connection_mode_configuration/*");
        return skipList;
    }

    private static boolean matchesSkipListPattern(String testPath, String pattern) {
        if (pattern.endsWith("/*")) {
            String prefix = pattern.substring(0, pattern.length() - 1);
            return testPath.startsWith(prefix);
        }
        return testPath.equals(pattern);
    }
}
