/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateDatabasesIT extends ESRestTestCase {

    public void test() throws Exception {
        String body = """
            {
              "pipeline": {
                "processors": [ { "geoip": { "field": "ip" } } ]
              },
              "docs": [ { "_index": "index", "_id": "id", "_source": { "ip": "89.160.20.128" } } ]
            }""";
        Request simulatePipelineRequest = new Request("POST", "/_ingest/pipeline/_simulate");
        simulatePipelineRequest.setJsonEntity(body);
        {
            Map<String, Object> response = entityAsMap(client().performRequest(simulatePipelineRequest));
            assertThat(ObjectPath.eval("docs.0.doc._source.tags.0", response), equalTo("_geoip_database_unavailable_GeoLite2-City.mmdb"));
        }

        // Ensure no config databases have been setup:
        {
            Map<?, ?> stats = getGeoIpStatsForSingleNode();
            assertThat(stats, nullValue());
        }

        Path configPath = PathUtils.get(System.getProperty("tests.config.dir"));
        assertThat(Files.exists(configPath), is(true));
        Path ingestGeoipDatabaseDir = configPath.resolve("ingest-geoip");
        Files.createDirectory(ingestGeoipDatabaseDir);
        Files.copy(
            UpdateDatabasesIT.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"),
            ingestGeoipDatabaseDir.resolve("GeoLite2-City.mmdb")
        );

        // Ensure that a config database has been setup:
        {
            assertBusy(() -> {
                Map<?, ?> stats = getGeoIpStatsForSingleNode();
                assertThat(stats, notNullValue());
                assertThat(stats.get("config_databases"), equalTo(List.of("GeoLite2-City.mmdb")));
            });
        }

        Map<String, Object> response = entityAsMap(client().performRequest(simulatePipelineRequest));
        assertThat(ObjectPath.eval("docs.0.doc._source.geoip.city_name", response), equalTo("Linköping"));
    }

    private static Map<?, ?> getGeoIpStatsForSingleNode() throws IOException {
        Request request = new Request("GET", "/_ingest/geoip/stats");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        assertThat(nodes.size(), either(equalTo(0)).or(equalTo(1)));
        return nodes.isEmpty() ? null : (Map<?, ?>) nodes.values().iterator().next();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
