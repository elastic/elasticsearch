/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;

public class LogsDataStreamIT extends DisabledSecurityDataStreamTestCase {

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
    }

    @SuppressWarnings("unchecked")
    public void testDefaultLogsSettingAndMapping() throws Exception {
        RestClient client = client();
        waitForLogs(client);

        String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);
        String backingIndex = getWriteBackingIndex(client, dataStreamName);

        // Ensure correct settings
        Map<String, Object> settings = getSettings(client, backingIndex);
        assertThat(settings.get("index.mapping.ignore_malformed"), is("true"));

        // Extend the mapping and verify
        putMapping(client, backingIndex);
        Map<String, Object> mappingProperties = getMappingProperties(client, backingIndex);
        assertThat(((Map<String, Object>) mappingProperties.get("@timestamp")).get("ignore_malformed"), equalTo(false));
        assertThat(((Map<String, Object>) mappingProperties.get("numeric_field")).get("type"), equalTo("integer"));

        // Insert valid doc and verify successful indexing
        {
            indexDoc(client, dataStreamName, """
                {
                  "@timestamp": "2023-04-18",
                  "message": "valid",
                  "numeric_field": 42
                }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "message": {
                        "value": "valid"
                      }
                    }
                  },
                  "fields": ["numeric_field"]
                }
                """);
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");
            assertThat(fields.get("numeric_field"), is(List.of(42)));
        }

        // Insert invalid doc and verify successful indexing
        {
            indexDoc(client, dataStreamName, """
                {
                  "@timestamp": "2023-04-18",
                  "message": "invalid",
                  "numeric_field": "forty-two"
                }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "message": {
                        "value": "invalid"
                      }
                    }
                  },
                  "fields": ["numeric_field"]
                }
                """);
            List<String> ignored = ((Map<String, List<String>>) results.get(0)).get("_ignored");
            assertThat(ignored, contains("numeric_field"));
            Map<String, Object> ignoredFieldValues = ((Map<String, Map<String, Object>>) results.get(0)).get("ignored_field_values");
            assertThat(ignoredFieldValues.get("numeric_field"), is(List.of("forty-two")));
        }
    }

    @SuppressWarnings("unchecked")
    public void testLogsDefaultPipeline() throws Exception {
        RestClient client = client();
        waitForLogs(client);

        {
            Request request = new Request("POST", "/_component_template/logs@custom");
            request.setJsonEntity("""
                {
                  "template": {
                    "mappings": {
                      "properties": {
                        "custom_timestamp": {
                          "type": "date"
                        }
                      }
                    }
                  }
                }
                """);
            assertOK(client.performRequest(request));
        }
        {
            Request request = new Request("PUT", "/_ingest/pipeline/logs@custom");
            request.setJsonEntity("""
                    {
                      "processors": [
                        {
                          "set" : {
                            "field": "custom_timestamp",
                            "copy_from": "_ingest.timestamp"
                          }
                        }
                      ]
                    }
                """);
            assertOK(client.performRequest(request));
        }

        String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);
        String backingIndex = getWriteBackingIndex(client, dataStreamName);

        // Verify mapping from custom logs
        Map<String, Object> mappingProperties = getMappingProperties(client, backingIndex);
        assertThat(((Map<String, Object>) mappingProperties.get("@timestamp")).get("type"), equalTo("date"));

        // no timestamp - testing default pipeline's @timestamp set processor
        {
            indexDoc(client, dataStreamName, """
                {
                  "message": "no_timestamp"
                }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "message": {
                        "value": "no_timestamp"
                      }
                    }
                  },
                  "fields": ["@timestamp", "custom_timestamp"]
                }
                """);
            Map<String, Object> source = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");
            String timestamp = (String) source.get("@timestamp");
            assertThat(timestamp, matchesRegex("[0-9-]+T[0-9:.]+Z"));
            assertThat(source.get("custom_timestamp"), is(timestamp));

            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");
            timestamp = ((List<String>) fields.get("@timestamp")).get(0);
            assertThat(timestamp, matchesRegex("[0-9-]+T[0-9:.]+Z"));
            assertThat(((List<Object>) fields.get("custom_timestamp")).get(0), is(timestamp));
        }

        // verify that when a document is ingested with a timestamp, it does not get overridden
        {
            indexDoc(client, dataStreamName, """
                {
                  "message": "with_timestamp",
                  "@timestamp": "2023-05-10"
                }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "message": {
                        "value": "with_timestamp"
                      }
                    }
                  },
                  "fields": ["@timestamp", "custom_timestamp"]
                }
                """);
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");
            assertThat(fields.get("@timestamp"), is(List.of("2023-05-10T00:00:00.000Z")));
        }
    }

    private static void waitForLogs(RestClient client) throws Exception {
        assertBusy(() -> {
            try {
                Request request = new Request("GET", "_index_template/logs");
                assertOK(client.performRequest(request));
            } catch (ResponseException e) {
                fail(e.getMessage());
            }
        });
    }

    private static void createDataStream(RestClient client, String name) throws IOException {
        Request request = new Request("PUT", "_data_stream/" + name);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static String getWriteBackingIndex(RestClient client, String name) throws IOException {
        Request request = new Request("GET", "_data_stream/" + name);
        List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        List<Map<String, String>> indices = (List<Map<String, String>>) dataStream.get("indices");
        return indices.get(0).get("index_name");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSettings(RestClient client, String indexName) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        return ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get("settings");
    }

    private static void putMapping(RestClient client, String indexName) throws IOException {
        Request request = new Request("PUT", "/" + indexName + "/_mapping");
        request.setJsonEntity("""
            {
              "properties": {
                "numeric_field": {
                  "type": "integer"
                }
              }
            }
            """);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMappingProperties(RestClient client, String indexName) throws IOException {
        Request request = new Request("GET", "/" + indexName + "/_mapping");
        Map<String, Object> map = (Map<String, Object>) entityAsMap(client.performRequest(request)).get(indexName);
        Map<String, Object> mappings = (Map<String, Object>) map.get("mappings");
        return (Map<String, Object>) mappings.get("properties");
    }

    private static void indexDoc(RestClient client, String dataStreamName, String doc) throws IOException {
        Request request = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        request.setJsonEntity(doc);
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static List<Object> searchDocs(RestClient client, String dataStreamName, String query) throws IOException {
        Request request = new Request("GET", "/" + dataStreamName + "/_search");
        request.setJsonEntity(query);
        Map<String, Object> hits = (Map<String, Object>) entityAsMap(client.performRequest(request)).get("hits");
        return (List<Object>) hits.get("hits");
    }
}
