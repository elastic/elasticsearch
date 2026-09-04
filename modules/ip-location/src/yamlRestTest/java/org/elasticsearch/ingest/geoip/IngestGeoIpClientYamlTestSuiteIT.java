/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import fixture.geoip.GeoIpHttpFixture;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.nio.file.Path;

public class IngestGeoIpClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final boolean useFixture = Booleans.parseBoolean(System.getProperty("geoip_use_service", "false")) == false;

    private static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    public static TemporaryFolder configDir = new TemporaryFolder();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .withConfigDir(() -> getRootPath(configDir))
        .module("reindex")
        .module("ip-location")
        .module("ingest-ip-location")
        .systemProperty("ingest.geoip.downloader.enabled.default", "true")
        // sets the plain (geoip.elastic.co) downloader endpoint, which is used in these tests
        .setting("ingest.geoip.downloader.endpoint", fixture::getAddress, s -> useFixture)
        // also sets the enterprise downloader maxmind endpoint, to make sure we do not accidentally hit the real endpoint from tests
        // note: it's not important that the downloading actually work at this point -- the rest tests (so far) don't exercise
        // the downloading code because of license reasons -- but if they did, then it would be important that we're hitting a fixture
        .systemProperty("ingest.geoip.downloader.maxmind.endpoint.default", fixture::getAddress, s -> useFixture)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(configDir).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public IngestGeoIpClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @BeforeClass
    public static void copyExtraDatabase() throws Exception {
        GeoIpDatabaseTestHelper.copyConfigDatabase(getRootPath(configDir));
    }

    @Before
    public void waitForDatabases() throws Exception {
        GeoIpDatabaseTestHelper.putGeoipPipeline(client(), "pipeline-with-geoip");
        GeoIpDatabaseTestHelper.assertDatabasesLoaded(client());
    }

    @SuppressForbidden(reason = "fixtures use java.io.File based APIs")
    public static Path getRootPath(TemporaryFolder folder) {
        return folder.getRoot().toPath();
    }
}
