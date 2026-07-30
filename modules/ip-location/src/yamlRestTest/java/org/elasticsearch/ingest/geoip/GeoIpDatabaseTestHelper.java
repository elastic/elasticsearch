/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Shared test utilities for ensuring GeoIP databases are downloaded and available
 * before running YAML REST tests. Used by both ip-location and ingest-ip-location
 * test suites.
 */
public final class GeoIpDatabaseTestHelper {

    private GeoIpDatabaseTestHelper() {}

    /**
     * Copies the IPinfo ASN sample database into the config directory so that
     * it is picked up by {@code ConfigDatabases} at node startup.
     */
    public static void copyConfigDatabase(Path configPath) throws Exception {
        Path ingestGeoipDatabaseDir = configPath.resolve("ingest-geoip");
        Files.createDirectories(ingestGeoipDatabaseDir);
        Files.copy(
            Objects.requireNonNull(GeoIpDatabaseTestHelper.class.getResourceAsStream("/ipinfo/asn_sample.mmdb")),
            ingestGeoipDatabaseDir.resolve("asn.mmdb")
        );
    }

    /**
     * Creates a pipeline with a geoip processor to trigger the GeoIP downloader.
     */
    public static void putGeoipPipeline(RestClient client, String pipelineName) throws Exception {
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
        client.performRequest(putPipelineRequest);
    }

    /**
     * Polls {@code /_ingest/geoip/stats} until the expected databases are downloaded
     * and the config database is loaded.
     */
    public static void assertDatabasesLoaded(RestClient client) throws Exception {
        ESTestCase.assertBusy(() -> {
            Request request = new Request("GET", "/_ingest/geoip/stats");
            Map<String, Object> response = ESRestTestCase.entityAsMap(client.performRequest(request));

            Map<?, ?> downloadStats = (Map<?, ?>) response.get("stats");
            assertEquals(4, downloadStats.get("databases_count"));

            Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
            assertEquals(1, nodes.size());
            Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();

            List<?> databases = ((List<?>) node.get("databases"));
            assertNotNull(databases);
            List<String> databaseNames = databases.stream().map(o -> (String) ((Map<?, ?>) o).get("name")).toList();
            assertEquals(
                Set.of("GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "GeoLite2-ASN.mmdb", "MyCustomGeoLite2-City.mmdb"),
                Set.copyOf(databaseNames)
            );

            assertEquals(List.of("asn.mmdb"), node.get("config_databases"));
        }, 20, TimeUnit.SECONDS);
    }
}
