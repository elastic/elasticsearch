/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;

@TestCaseOrdering(YamlFileOrder.class)
public class BasicLicenseIT extends LicenseYamlSuiteTestCase {

    private static final List<String> SKIPPED_TESTS = List.of(
        "querying_cluster/10_basic/Add persistent remote cluster based on the preset cluster",
        "querying_cluster/20_info/Add persistent remote cluster based on the preset cluster and check remote info",
        "querying_cluster/20_info/Fetch remote cluster info for existing cluster",
        "querying_cluster/70_connection_mode_configuration/"
    );

    private static boolean PROXY_MODE;

    private static final ElasticsearchCluster fulfillingCluster = Clusters.fulfillingCluster("basic");
    private static final ElasticsearchCluster queryingCluster = Clusters.queryingCluster(
        "basic",
        () -> PROXY_MODE,
        () -> fulfillingCluster.getTransportEndpoint(0)
    );

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> PROXY_MODE = randomBoolean()))
        .around(RuleChain.outerRule(fulfillingCluster).around(queryingCluster));

    public BasicLicenseIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters("basic_license");
    }

    @Override
    public void test() throws IOException {
        if (PROXY_MODE && SKIPPED_TESTS.stream().anyMatch(t -> getTestCandidate().getTestPath().contains(t))) {
            return; // skip test
        }
        super.test();
    }

    @Override
    protected ElasticsearchCluster fulfillingCluster() {
        return fulfillingCluster;
    }

    @Override
    protected ElasticsearchCluster queryingCluster() {
        return queryingCluster;
    }

}
