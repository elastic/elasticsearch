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
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.multiproject.test.MultipleProjectsClientYamlSuiteTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static org.elasticsearch.ingest.geoip.IngestGeoIpClientYamlTestSuiteIT.assertDatabasesLoaded;
import static org.elasticsearch.ingest.geoip.IngestGeoIpClientYamlTestSuiteIT.getRootPath;
import static org.elasticsearch.ingest.geoip.IngestGeoIpClientYamlTestSuiteIT.putGeoipPipeline;
import static org.hamcrest.Matchers.is;

@FixForMultiProject(description = "Potentially remove this test after https://elasticco.atlassian.net/browse/ES-12094 is implemented")
public class IngestGeoIpClientMultiProjectYamlTestSuiteIT extends MultipleProjectsClientYamlSuiteTestCase {

    private static final boolean useFixture = Booleans.parseBoolean(System.getProperty("geoip_use_service", "false")) == false;

    private static GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    public static TemporaryFolder configDir = new TemporaryFolder();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .withConfigDir(() -> getRootPath(configDir))
        .module("reindex")
        .module("ingest-geoip")
        .systemProperty("ingest.geoip.downloader.enabled.default", "true")
        // sets the plain (geoip.elastic.co) downloader endpoint, which is used in these tests
        .setting("ingest.geoip.downloader.endpoint", () -> fixture.getAddress(), s -> useFixture)
        // also sets the enterprise downloader maxmind endpoint, to make sure we do not accidentally hit the real endpoint from tests
        // note: it's not important that the downloading actually work at this point -- the rest tests (so far) don't exercise
        // the downloading code because of license reasons -- but if they did, then it would be important that we're hitting a fixture
        .systemProperty("ingest.geoip.downloader.maxmind.endpoint.default", () -> fixture.getAddress(), s -> useFixture)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user(USER, PASS)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(configDir).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public IngestGeoIpClientMultiProjectYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @BeforeClass
    public static void copyExtraDatabase() throws Exception {
        Path configPath = getRootPath(configDir);
        assertThat(Files.exists(configPath), is(true));
        Path ingestGeoipDatabaseDir = configPath.resolve("ingest-geoip");
        Files.createDirectory(ingestGeoipDatabaseDir);
        final var clazz = IngestGeoIpClientYamlTestSuiteIT.class; // long line prevention
        Files.copy(
            Objects.requireNonNull(clazz.getResourceAsStream("/ipinfo/asn_sample.mmdb")),
            ingestGeoipDatabaseDir.resolve("asn.mmdb")
        );
    }

    @Before
    public void waitForDatabases() throws Exception {
        putGeoipPipeline("pipeline-with-geoip");
        assertDatabasesLoaded();
    }
}
