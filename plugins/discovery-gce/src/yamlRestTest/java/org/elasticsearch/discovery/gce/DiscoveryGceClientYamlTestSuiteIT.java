/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery.gce;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@SuppressForbidden(reason = "fixtures use java.io.File based APIs")
public class DiscoveryGceClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static GCEFixture gceFixture = new GCEFixture(() -> temporaryFolder.getRoot().toPath().resolve("unicast_hosts.txt"));

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("discovery-gce")
        .nodes(3)
        .node(0, n -> n.withConfigDir(() -> temporaryFolder.getRoot().toPath()))
        .systemProperty("es.allow_reroute_gce_settings", "true")
        .environment("GCE_METADATA_HOST", () -> gceFixture.getHostAndPort())
        .setting("discovery.seed_providers", "gce")
        .setting("cloud.gce.host", () -> gceFixture.getAddress())
        .setting("cloud.gce.root_url", () -> gceFixture.getAddress())
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(temporaryFolder).around(gceFixture).around(cluster);

    public DiscoveryGceClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
