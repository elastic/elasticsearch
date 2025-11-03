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

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class IngestGeoIpClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

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

    /**
     * This creates a pipeline with a geoip processor so that the GeoipDownloader will download its databases.
     * @throws IOException
     */
    static void putGeoipPipeline(String pipelineName) throws Exception {
        final BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipelineName);
        putPipelineRequest.setEntity(new ByteArrayEntity(bytes.array(), ContentType.APPLICATION_JSON));
        client().performRequest(putPipelineRequest);
    }

    static void assertDatabasesLoaded() throws Exception {
        // assert that the databases are downloaded and loaded
        assertBusy(() -> {
            Request request = new Request("GET", "/_ingest/geoip/stats");
            Map<String, Object> response = entityAsMap(client().performRequest(request));

            Map<?, ?> downloadStats = (Map<?, ?>) response.get("stats");
            assertThat(downloadStats.get("databases_count"), equalTo(4));

            Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
            assertThat(nodes.size(), equalTo(1));
            Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();

            // confirm the downloaded databases are all correct
            List<?> databases = ((List<?>) node.get("databases"));
            assertThat(databases, notNullValue());
            List<String> databaseNames = databases.stream().map(o -> (String) ((Map<?, ?>) o).get("name")).toList();
            assertThat(
                databaseNames,
                containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "GeoLite2-ASN.mmdb", "MyCustomGeoLite2-City.mmdb")
            );

            // ensure that the extra config database has been set up, too:
            assertThat(node.get("config_databases"), equalTo(List.of("asn.mmdb")));
        });
    }

    @SuppressForbidden(reason = "fixtures use java.io.File based APIs")
    public static Path getRootPath(TemporaryFolder folder) {
        return folder.getRoot().toPath();
    }
}
