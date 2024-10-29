/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LogsdbRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFeatureUsageWithLogsdbIndex() throws IOException {
        {
            if (randomBoolean()) {
                createIndex("test-index", Settings.builder().put("index.mode", "logsdb").build());
            } else if (randomBoolean()) {
                String mapping = """
                    {
                        "properties": {
                            "field1": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                    """;
                var settings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "field1").build();
                createIndex("test-index", settings, mapping);
            } else {
                String mapping = """
                    {
                        "_source": {
                            "mode": "synthetic"
                        }
                    }
                    """;
                createIndex("test-index", Settings.EMPTY, mapping);
            }
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            logger.info("response's features: {}", features);
            assertThat(features, Matchers.not(Matchers.empty()));
            Map<?, ?> feature = features.stream().filter(map -> "mappings".equals(map.get("family"))).findFirst().get();
            assertThat(feature.get("name"), equalTo("synthetic-source"));
            assertThat(feature.get("license_level"), equalTo("enterprise"));

            var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings("test-index").get("test-index")).get("settings");
            assertNull(settings.get("index.mapping.source.mode"));  // Default, no downgrading.
        }
    }

    public void testLogsdbSourceModeForLogsIndex() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"transient\": { \"cluster.logsdb.enabled\": true } }");
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["logs-test-*"],
                "data_stream": {
                }
            }
            """);
        assertOK(client().performRequest(request));

        request = new Request("POST", "/logs-test-foo/_doc");
        request.setJsonEntity("""
            {
                "@timestamp": "2020-01-01T00:00:00.000Z",
                "host.name": "foo",
                "message": "bar"
            }
            """);
        assertOK(client().performRequest(request));

        String index = DataStream.getDefaultBackingIndexName("logs-test-foo", 1);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertNull(settings.get("index.mapping.source.mode"));
    }

}
