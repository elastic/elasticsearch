/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multi_cluster;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Build;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

import java.util.List;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class MultiClusterWithSecurityYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    public static LocalClusterConfigProvider commonConfig = c -> c.distribution(DistributionType.DEFAULT)
        .module("transform")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS, "_es_test_root", true);

    public static ElasticsearchCluster remoteCluster = ElasticsearchCluster.local()
        .name("remote-cluster")
        .apply(commonConfig)
        .setting("node.roles", "[data,ingest,master]")
        .withNode(node -> node.version(Build.current().minWireCompatVersion()))
        .withNode(node -> node.version(Build.current().version()))
        .build();

    public static ElasticsearchCluster multiCluster = ElasticsearchCluster.local()
        .name("multi-cluster")
        .apply(commonConfig)
        .setting("cluster.remote.my_remote_cluster.seeds", () -> "\"" + remoteCluster.getTransportEndpoints() + "\"")
        .setting("cluster.remote.connections_per_cluster", "1")
        .withNode(node -> node.setting("node.roles", "[data,ingest,master,transform]"))
        .withNode(node -> node.setting("node.roles", "[data,ingest,master,transform,remote_cluster_client]"))
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(remoteCluster).around(multiCluster);

    private static TargetCluster clientTargetCluster;
    private final TargetCluster targetCluster;

    @BeforeClass
    public static void init() {
        assumeFalse("Cannot run in FIPS mode since it uses trial license", inFipsJvm());
    }

    public MultiClusterWithSecurityYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.targetCluster = testCandidate.getApi().equals("remote_cluster") ? TargetCluster.REMOTE_CLUSTER : TargetCluster.MULTI_CLUSTER;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() throws Exception {
        // force remote_cluster to run first so documents are indexed into it
        var suites = List.of(createParameters(new String[] { "remote_cluster" }), createParameters(new String[] { "multi_cluster" }));
        return Iterables.flatten(suites);
    }

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void maybeReInitClient() throws Exception {
        if (clientTargetCluster != targetCluster) {
            closeClients(); // close clients from ESRestTestCase
            closeClient(); // close client from ESClientYamlSuiteTestCase
            initClient(); // reinitialize client for ESRestTestCase
            initAndResetContext(); // reinitialize client for ESClientYamlSuiteTestCase
        }
    }

    @Override
    protected String getTestRestCluster() {
        clientTargetCluster = targetCluster;
        assert clientTargetCluster != null;
        return switch (clientTargetCluster) {
            case REMOTE_CLUSTER -> remoteCluster.getHttpAddresses();
            case MULTI_CLUSTER -> multiCluster.getHttpAddresses();
        };
    }

    private enum TargetCluster {
        REMOTE_CLUSTER,
        MULTI_CLUSTER;
    }
}
