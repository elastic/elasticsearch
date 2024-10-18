/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LogsdbRestIT extends ESRestTestCase {

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

    public void testLogsdbOverrideSyntheticSourceModeInMapping() throws IOException {
        final String index = "test-index";
        String mapping = """
            {
                "_source": {
                    "mode": "synthetic"
                }
            }
            """;
        createIndex(index, Settings.builder().put("index.mode", "logsdb").build(), mapping);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.STORED.toString(), settings.get("index.mapping.source.mode"));
    }

    public void testLogsdbNoOverrideSyntheticSourceSetting() throws IOException {
        final String index = "test-index";
        createIndex(
            index,
            Settings.builder().put("index.mode", "logsdb").put("index.mapping.source.mode", SourceFieldMapper.Mode.SYNTHETIC).build()
        );
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertEquals(SourceFieldMapper.Mode.SYNTHETIC.toString(), settings.get("index.mapping.source.mode"));
    }
}
