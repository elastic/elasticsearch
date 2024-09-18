/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class IngestGeoIpMixedClusterYamlTestSuiteIT extends IngestGeoIpClientYamlTestSuiteIT {

    private static ElasticsearchCluster mixedCluster = ElasticsearchCluster.local()
        .module("reindex")
        .module("ingest-geoip")
        .distribution(DistributionType.DEFAULT)
        .withNode(node -> node.version(getOldVersion()))
        .withNode(node -> node.version(Version.CURRENT))
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .systemProperty("ingest.geoip.downloader.enabled.default", "true")
        // sets the plain (geoip.elastic.co) downloader endpoint, which is used in these tests
        .setting("ingest.geoip.downloader.endpoint", () -> fixture.getAddress(), s -> useFixture)
        // also sets the enterprise downloader maxmind endpoint, to make sure we do not accidentally hit the real endpoint from tests
        // note: it's not important that the downloading actually work at this point -- the rest tests (so far) don't exercise
        // the downloading code because of license reasons -- but if they did, then it would be important that we're hitting a fixture
        .systemProperty("ingest.geoip.downloader.maxmind.endpoint.default", () -> fixture.getAddress(), s -> useFixture)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(mixedCluster);

    static Version getOldVersion() {
        return Version.fromString(System.getProperty("tests.old_cluster_version"));
    }

    @Override
    protected String getTestRestCluster() {
        return mixedCluster.getHttpAddresses();
    }

    @Override
    int getClusterSize() {
        return mixedCluster.getNumNodes();
    }

    public IngestGeoIpMixedClusterYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }
}
