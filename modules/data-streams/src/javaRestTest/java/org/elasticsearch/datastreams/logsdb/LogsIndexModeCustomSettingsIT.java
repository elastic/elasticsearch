/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
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

    public void testConfigureStoredSource() throws IOException {
        var storedSourceMapping = """
            {
              "template": {
                "mappings": {
                  "_source": {
                    "mode": "stored"
                  }
                }
              }
            }""";

        Exception e = assertThrows(ResponseException.class, () -> putComponentTemplate(client, "logs@custom", storedSourceMapping));
        assertThat(
            e.getMessage(),
            containsString("updating component template [logs@custom] results in invalid composable template [logs]")
        );
        assertThat(e.getMessage(), containsString("Indices with with index mode [logsdb] only support synthetic source"));

        assertOK(createDataStream(client, "logs-custom-dev"));

        var mapping = getMapping(client, getDataStreamBackingIndex(client, "logs-custom-dev", 0));
        String sourceMode = (String) subObject("_source").apply(mapping).get("mode");
        assertThat(sourceMode, equalTo("synthetic"));
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

    public void testAddNonCompatibleMapping() throws IOException {
        var nonCompatibleMappingAdditionTemplate = """
            {
              "template": {
                "mappings": {
                  "properties": {
                    "bomb": {
                      "type": "ip",
                      "doc_values": false
                    }
                  }
                }
              }
            }""";

        Exception e = assertThrows(
            ResponseException.class,
            () -> putComponentTemplate(client, "logs@custom", nonCompatibleMappingAdditionTemplate)
        );
        assertThat(
            e.getMessage(),
            containsString("updating component template [logs@custom] results in invalid composable template [logs]")
        );
        assertThat(
            e.getMessage(),
            containsString("field [bomb] of type [ip] doesn't support synthetic source because it doesn't have doc values")
        );
    }

    private static Map<String, Object> getMapping(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_mapping");

        Map<String, Object> mappings = ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get(
            "mappings"
        );

        return mappings;
    }

    private Function<Object, Map<String, Object>> subObject(String key) {
        return (mapAsObject) -> (Map<String, Object>) ((Map<String, Object>) mapAsObject).get(key);
    }
}
