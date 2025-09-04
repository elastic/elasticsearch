/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LogsdbWithBasicRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFeatureUsageWithLogsdbIndex() throws IOException {
        {
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            assertThat(features, Matchers.empty());
        }
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
                createIndex("test-index", Settings.builder().put("index.mapping.source.mode", "synthetic").build());
            }
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            assertThat(features, Matchers.empty());
        }
    }

    public void testLogsdbIndexGetsStoredSource() throws IOException {
        final String index = "test-index";
        createIndex(index, Settings.builder().put("index.mode", "logsdb").build());
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbOverrideSyntheticSourceSetting() throws IOException {
        final String index = "test-index";
        createIndex(
            index,
            Settings.builder().put("index.mode", "logsdb").put("index.mapping.source.mode", SourceFieldMapper.Mode.SYNTHETIC).build()
        );
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbOverrideNullSyntheticSourceSetting() throws IOException {
        final String index = "test-index";
        createIndex(index, Settings.builder().put("index.mode", "logsdb").putNull("index.mapping.source.mode").build());
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbOverrideSyntheticSourceSettingInTemplate() throws IOException {
        var request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["test-*"],
                "template": {
                    "settings":{
                        "index": {
                            "mode": "logsdb",
                            "mapping": {
                              "source": {
                                "mode": "synthetic"
                              }
                            }
                        }
                    }
                }
            }
            """);
        assertOK(client().performRequest(request));

        final String index = "test-index";
        createIndex(index);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbOverrideNullInTemplate() throws IOException {
        var request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["test-*"],
                "template": {
                    "settings":{
                        "index": {
                            "mode": "logsdb",
                            "mapping": {
                              "source": {
                                "mode": null
                              }
                            }
                        }
                    }
                }
            }
            """);
        assertOK(client().performRequest(request));

        final String index = "test-index";
        createIndex(index);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbOverrideDefaultModeForLogsIndex() throws IOException {
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

        String index = getDataStreamBackingIndexNames("logs-test-foo").getFirst();
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbRouteOnSortFields() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"transient\": { \"cluster.logsdb.enabled\": true } }");
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["my-log-*"],
                "data_stream": {
                },
                "template": {
                    "settings":{
                        "index": {
                            "mode": "logsdb",
                            "sort.field": [ "host.name", "message", "@timestamp" ],
                            "logsdb.route_on_sort_fields": true
                        }
                    },
                    "mappings": {
                        "properties": {
                            "@timestamp" : {
                                "type": "date"
                            },
                            "host.name": {
                                "type": "keyword"
                            },
                            "message": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }
            """);
        assertOK(client().performRequest(request));

        request = new Request("POST", "/my-log-foo/_doc");
        request.setJsonEntity("""
            {
                "@timestamp": "2020-01-01T00:00:00.000Z",
                "host.name": "foo",
                "message": "bar"
            }
            """);
        assertOK(client().performRequest(request));

        String index = getDataStreamBackingIndexNames("my-log-foo").getFirst();
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
        assertEquals("false", settings.get(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS.getKey()));
        assertNull(settings.get(IndexMetadata.INDEX_ROUTING_PATH.getKey()));
    }
}
