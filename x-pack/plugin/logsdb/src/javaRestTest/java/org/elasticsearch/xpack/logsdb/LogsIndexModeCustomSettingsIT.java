/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

@SuppressWarnings("unchecked")
public class LogsIndexModeCustomSettingsIT extends LogsIndexModeRestTestIT {
    @ClassRule()
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.otel_data.registry.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() throws Exception {
        client = client();
        waitForLogs(client);
    }

    private RestClient client;

    public void testOverrideIndexSorting() throws IOException {
        var indexSortOverrideTemplate = """
            {
              "template": {
                "settings": {
                  "index": {
                    "sort.field": ["cluster", "custom.timestamp"],
                    "sort.order":["desc", "asc"]
                  }
                },
                "mappings": {
                  "properties": {
                    "cluster": {
                      "type": "keyword"
                    },
                    "custom": {
                      "properties": {
                        "timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", indexSortOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var indexSortField = (List<String>) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.sort.field");
        assertThat(indexSortField, equalTo(List.of("cluster", "custom.timestamp")));

        var indexSortOrder = (List<String>) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.sort.order");
        assertThat(indexSortOrder, equalTo(List.of("desc", "asc")));

        // @timestamp is a default mapping and should still be present
        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        String type = (String) subObject("properties").andThen(subObject("@timestamp")).apply(mapping).get("type");
        assertThat(type, equalTo("date"));
    }

    public void testConfigureStoredSourceBeforeIndexCreation() throws IOException {
        var storedSourceMapping = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mode": "logsdb",
                    "mapping": {
                      "source": {
                        "mode": "stored"
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", storedSourceMapping));
        Request request = new Request("PUT", "_data_stream/logs-custom-dev");
        assertOK(client.performRequest(request));
        var indexName = getDataStreamBackingIndex(client, "logs-custom-dev", 0);
        var settings = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) getIndexSettings(indexName)).get(indexName)).get("settings");
        assertThat(settings, hasEntry("index.mapping.source.mode", "stored"));
    }

    public void testConfigureDisabledSourceBeforeIndexCreation() {
        var storedSourceMapping = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mode": "logsdb"
                  }
                },
                "mappings": {
                  "_source": {
                    "enabled": false
                  }
                }
              }
            }""";

        Exception e = assertThrows(ResponseException.class, () -> putComponentTemplate(client, "logs@custom", storedSourceMapping));
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: _source can not be disabled in index using [logsdb] index mode")
        );
        assertThat(e.getMessage(), containsString("mapper_parsing_exception"));
    }

    public void testConfigureDisabledSourceModeBeforeIndexCreation() {
        var storedSourceMapping = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mode": "logsdb",
                    "mapping": {
                      "source": {
                        "mode": "disabled"
                      }
                    }
                  }
                }
              }
            }""";

        Exception e = assertThrows(ResponseException.class, () -> putComponentTemplate(client, "logs@custom", storedSourceMapping));
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: _source can not be disabled in index using [logsdb] index mode")
        );
        assertThat(e.getMessage(), containsString("mapper_parsing_exception"));
    }

    public void testConfigureStoredSourceWhenIndexIsCreated() throws IOException {
        var storedSourceMapping = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mapping": {
                      "source": {
                        "mode": "stored"
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", storedSourceMapping));
        Request request = new Request("PUT", "_data_stream/logs-custom-dev");
        assertOK(client.performRequest(request));

        var indexName = getDataStreamBackingIndex(client, "logs-custom-dev", 0);
        var settings = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) getIndexSettings(indexName)).get(indexName)).get("settings");
        assertThat(settings, hasEntry("index.mapping.source.mode", "stored"));
    }

    public void testConfigureDisabledSourceWhenIndexIsCreated() throws IOException {
        var disabledModeMapping = """
            {
              "template": {
                "mappings": {
                  "_source": {
                    "enabled": false
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", disabledModeMapping));
        ResponseException e = expectThrows(ResponseException.class, () -> createDataStream(client, "logs-custom-dev"));
        assertThat(e.getMessage(), containsString("_source can not be disabled in index using [logsdb] index mode"));
    }

    public void testConfigureDisabledSourceModeWhenIndexIsCreated() throws IOException {
        var disabledModeMapping = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mapping": {
                      "source": {
                        "mode": "disabled"
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", disabledModeMapping));
        ResponseException e = expectThrows(ResponseException.class, () -> createDataStream(client, "logs-custom-dev"));
        assertThat(e.getMessage(), containsString("_source can not be disabled in index using [logsdb] index mode"));
    }

    public void testOverrideIndexCodec() throws IOException {
        var indexCodecOverrideTemplate = """
            {
              "template": {
                "settings": {
                  "index": {
                    "codec": "default"
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", indexCodecOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var indexCodec = (String) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.codec");
        assertThat(indexCodec, equalTo("default"));
    }

    public void testOverrideTimestampField() throws IOException {
        var timestampMappingOverrideTemplate = """
            {
              "template": {
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date_nanos"
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", timestampMappingOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        String type = (String) subObject("properties").andThen(subObject("@timestamp")).apply(mapping).get("type");
        assertThat(type, equalTo("date_nanos"));
    }

    public void testOverrideHostNameField() throws IOException {
        var timestampMappingOverrideTemplate = """
            {
              "template": {
                "mappings": {
                  "properties": {
                    "host": {
                      "properties": {
                        "name": {
                          "type": "keyword",
                          "index": false,
                          "ignore_above": 10
                        }
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", timestampMappingOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        var hostNameFieldParameters = subObject("properties").andThen(subObject("host"))
            .andThen(subObject("properties"))
            .andThen(subObject("name"))
            .apply(mapping);
        assertThat((String) hostNameFieldParameters.get("type"), equalTo("keyword"));
        assertThat((boolean) hostNameFieldParameters.get("index"), equalTo(false));
        assertThat((int) hostNameFieldParameters.get("ignore_above"), equalTo(10));
    }

    public void testOverrideIndexSortingWithCustomTimestampField() throws IOException {
        var timestampMappingAndIndexSortOverrideTemplate = """
            {
              "template": {
                "settings": {
                  "index": {
                    "sort.field": ["@timestamp", "cluster"],
                    "sort.order":["asc", "asc"]
                  }
                },
                "mappings": {
                  "properties": {
                    "@timestamp": {
                      "type": "date_nanos"
                    },
                    "cluster": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", timestampMappingAndIndexSortOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        String type = (String) subObject("properties").andThen(subObject("@timestamp")).apply(mapping).get("type");
        assertThat(type, equalTo("date_nanos"));

        var indexSortField = (List<String>) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.sort.field");
        assertThat(indexSortField, equalTo(List.of("@timestamp", "cluster")));

        var indexSortOrder = (List<String>) getSetting(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0), "index.sort.order");
        assertThat(indexSortOrder, equalTo(List.of("asc", "asc")));
    }

    public void testOverrideIgnoreMalformed() throws IOException {
        var ignoreMalformedOverrideTemplate = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mapping": {
                      "ignore_malformed": false
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", ignoreMalformedOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var ignoreMalformedIndexSetting = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            "index.mapping.ignore_malformed"
        );
        assertThat(ignoreMalformedIndexSetting, equalTo("false"));
    }

    public void testOverrideIgnoreDynamicBeyondLimit() throws IOException {
        var ignoreMalformedOverrideTemplate = """
            {
              "template": {
                "settings": {
                  "index": {
                    "mapping": {
                      "total_fields": {
                        "ignore_dynamic_beyond_limit": false
                      }
                    }
                  }
                }
              }
            }""";

        assertOK(putComponentTemplate(client, "logs@custom", ignoreMalformedOverrideTemplate));
        assertOK(createDataStream(client, "logs-custom-dev"));

        var ignoreDynamicBeyondLimitIndexSetting = (String) getSetting(
            client,
            getDataStreamBackingIndex(client, "logs-custom-dev", 0),
            "index.mapping.total_fields.ignore_dynamic_beyond_limit"
        );
        assertThat(ignoreDynamicBeyondLimitIndexSetting, equalTo("false"));
    }

    public void testIgnoreMalformedSetting() throws IOException {
        // with default template
        {
            assertOK(createDataStream(client, "logs-test-1"));
            String logsIndex1 = getDataStreamBackingIndex(client, "logs-test-1", 0);
            assertThat(getSetting(client, logsIndex1, "index.mapping.ignore_malformed"), equalTo("true"));
            for (String newValue : List.of("false", "true")) {
                closeIndex(logsIndex1);
                updateIndexSettings(logsIndex1, Settings.builder().put("index.mapping.ignore_malformed", newValue));
                assertThat(getSetting(client, logsIndex1, "index.mapping.ignore_malformed"), equalTo(newValue));
            }
        }
        // with override template
        {
            var template = """
                {
                  "template": {
                    "settings": {
                      "index": {
                        "mapping": {
                          "ignore_malformed": "false"
                        }
                      }
                    }
                  }
                }""";
            assertOK(putComponentTemplate(client, "logs@custom", template));
            assertOK(createDataStream(client, "logs-custom-dev"));
            String index = getDataStreamBackingIndex(client, "logs-custom-dev", 0);
            assertThat(getSetting(client, index, "index.mapping.ignore_malformed"), equalTo("false"));
            for (String newValue : List.of("true", "false")) {
                closeIndex(index);
                updateIndexSettings(index, Settings.builder().put("index.mapping.ignore_malformed", newValue));
                assertThat(getSetting(client, index, "index.mapping.ignore_malformed"), equalTo(newValue));
            }
        }
        // standard index
        {
            String index = "test-index";
            createIndex(index);
            assertThat(getSetting(client, index, "index.mapping.ignore_malformed"), equalTo("false"));
            for (String newValue : List.of("false", "true")) {
                closeIndex(index);
                updateIndexSettings(index, Settings.builder().put("index.mapping.ignore_malformed", newValue));
                assertThat(getSetting(client, index, "index.mapping.ignore_malformed"), equalTo(newValue));
            }
        }
    }

    public void testIgnoreAboveSetting() throws IOException {
        // with default template
        {
            assertOK(createDataStream(client, "logs-test-1"));
            String logsIndex1 = getDataStreamBackingIndex(client, "logs-test-1", 0);
            assertThat(getSetting(client, logsIndex1, "index.mapping.ignore_above"), equalTo("8191"));
            for (String newValue : List.of("512", "2048", "12000", String.valueOf(Integer.MAX_VALUE))) {
                closeIndex(logsIndex1);
                updateIndexSettings(logsIndex1, Settings.builder().put("index.mapping.ignore_above", newValue));
                assertThat(getSetting(client, logsIndex1, "index.mapping.ignore_above"), equalTo(newValue));
            }
            for (String newValue : List.of(String.valueOf((long) Integer.MAX_VALUE + 1), String.valueOf(Long.MAX_VALUE))) {
                closeIndex(logsIndex1);
                ResponseException ex = assertThrows(
                    ResponseException.class,
                    () -> updateIndexSettings(logsIndex1, Settings.builder().put("index.mapping.ignore_above", newValue))
                );
                assertThat(
                    ex.getMessage(),
                    containsString("Failed to parse value [" + newValue + "] for setting [index.mapping.ignore_above]")
                );
            }
        }
        // with override template
        {
            var template = """
                {
                  "template": {
                    "settings": {
                      "index": {
                        "mapping": {
                          "ignore_above": "128"
                        }
                      }
                    }
                  }
                }""";
            assertOK(putComponentTemplate(client, "logs@custom", template));
            assertOK(createDataStream(client, "logs-custom-dev"));
            String index = getDataStreamBackingIndex(client, "logs-custom-dev", 0);
            assertThat(getSetting(client, index, "index.mapping.ignore_above"), equalTo("128"));
            for (String newValue : List.of("64", "256", "12000", String.valueOf(Integer.MAX_VALUE))) {
                closeIndex(index);
                updateIndexSettings(index, Settings.builder().put("index.mapping.ignore_above", newValue));
                assertThat(getSetting(client, index, "index.mapping.ignore_above"), equalTo(newValue));
            }
        }
        // standard index
        {
            String index = "test-index";
            createIndex(index);
            assertThat(getSetting(client, index, "index.mapping.ignore_above"), equalTo(Integer.toString(Integer.MAX_VALUE)));
            for (String newValue : List.of("256", "512", "12000", String.valueOf(Integer.MAX_VALUE))) {
                closeIndex(index);
                updateIndexSettings(index, Settings.builder().put("index.mapping.ignore_above", newValue));
                assertThat(getSetting(client, index, "index.mapping.ignore_above"), equalTo(newValue));
            }
        }
    }

    private Function<Object, Map<String, Object>> subObject(String key) {
        return (mapAsObject) -> (Map<String, Object>) ((Map<String, Object>) mapAsObject).get(key);
    }
}
