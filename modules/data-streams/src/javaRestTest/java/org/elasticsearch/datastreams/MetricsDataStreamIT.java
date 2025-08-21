/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MetricsDataStreamIT extends AbstractDataStreamIT {

    @SuppressWarnings("unchecked")
    public void testCustomMapping() throws Exception {
        {
            Request request = new Request("POST", "/_component_template/metrics@custom");
            request.setJsonEntity("""
                {
                  "template": {
                    "settings": {
                      "index": {
                        "query": {
                          "default_field": ["custom-message"]
                        }
                      }
                    },
                    "mappings": {
                      "properties": {
                        "numeric_field": {
                          "type": "integer"
                        },
                        "socket": {
                          "properties": {
                            "ip": {
                              "type": "keyword"
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """);
            assertOK(client.performRequest(request));
        }

        String dataStreamName = "metrics-generic-default";
        createDataStream(client, dataStreamName);
        String backingIndex = getWriteBackingIndex(client, dataStreamName);

        // Verify that the custom settings.index.query.default_field overrides the default query field - "message"
        Map<String, Object> settings = getSettings(client, backingIndex);
        assertThat(settings.get("index.query.default_field"), is(List.of("custom-message")));

        // Verify that the new field from the custom component template is applied
        putMapping(client, backingIndex);
        Map<String, Object> mappingProperties = getMappingProperties(client, backingIndex);
        assertThat(getValueFromPath(mappingProperties, List.of("numeric_field", "type")), equalTo("integer"));
        assertThat(getValueFromPath(mappingProperties, List.of("socket", "properties", "ip", "type")), is("keyword"));

        // Insert valid doc and verify successful indexing
        {
            indexDoc(client, dataStreamName, """
                {
                  "@timestamp": "2024-06-10",
                  "test": "doc-with-ip",
                  "socket": {
                    "ip": "127.0.0.1"
                  }
                }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "test": {
                        "value": "doc-with-ip"
                      }
                    }
                  },
                  "fields": ["socket.ip"]
                }
                """);
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");
            assertThat(fields.get("socket"), is(Map.of("ip", "127.0.0.1")));
        }
    }

    @Override
    protected String indexTemplateName() {
        return "metrics";
    }
}
