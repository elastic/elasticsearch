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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class LogsDataStreamIT extends AbstractDataStreamIT {

    @SuppressWarnings("unchecked")
    public void testDefaultLogsSettingAndMapping() throws Exception {
        String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);
        String backingIndex = getWriteBackingIndex(client, dataStreamName);

        // Ensure correct settings
        Map<String, Object> settings = getSettings(client, backingIndex);
        assertThat(settings.get("index.mapping.ignore_malformed"), is("true"));

        // Extend the mapping and verify
        putMapping(client, backingIndex);
        Map<String, Object> mappingProperties = getMappingProperties(client, backingIndex);
        assertThat(getValueFromPath(mappingProperties, List.of("@timestamp", "ignore_malformed")), equalTo(false));
        assertThat(getValueFromPath(mappingProperties, List.of("numeric_field", "type")), equalTo("integer"));

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
    public void testCustomMapping() throws Exception {
        {
            Request request = new Request("POST", "/_component_template/logs@custom");
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

        String dataStreamName = "logs-generic-default";
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

    @SuppressWarnings("unchecked")
    public void testLogsDefaultPipeline() throws Exception {
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
        assertThat(getValueFromPath(mappingProperties, List.of("@timestamp", "type")), equalTo("date"));

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

    @SuppressWarnings("unchecked")
    public void testLogsMessagePipeline() throws Exception {
        {
            Request request = new Request("PUT", "/_ingest/pipeline/logs@custom");
            request.setJsonEntity("""
                    {
                      "processors": [
                        {
                          "pipeline" : {
                            "name": "logs@json-pipeline",
                            "description": "A pipeline that automatically parses JSON log events into top-level fields if they are such"
                          }
                        }
                      ]
                    }
                """);
            assertOK(client.performRequest(request));
        }

        String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);

        {
            indexDoc(client, dataStreamName, """
                    {
                      "@timestamp":"2023-05-09T16:48:34.135Z",
                      "message":"json",
                      "log.level": "INFO",
                      "ecs.version": "1.6.0",
                      "service.name":"my-app",
                      "event.dataset":"my-app.RollingFile",
                      "process.thread.name":"main",
                      "log.logger":"root.pkg.MyApp"
                    }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "message": {
                        "value": "json"
                      }
                    }
                  },
                  "fields": ["message"]
                }
                """);
            assertThat(results.size(), is(1));
            Map<String, Object> source = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");

            // root field parsed from JSON should win
            assertThat(source.get("@timestamp"), is("2023-05-09T16:48:34.135Z"));
            assertThat(source.get("message"), is("json"));
            assertThat(((List<String>) fields.get("message")).get(0), is("json"));

            // successful access to subfields verifies that dot expansion is part of the pipeline
            assertThat(source.get("log.level"), is("INFO"));
            assertThat(source.get("ecs.version"), is("1.6.0"));
            assertThat(source.get("service.name"), is("my-app"));
            assertThat(source.get("event.dataset"), is("my-app.RollingFile"));
            assertThat(source.get("process.thread.name"), is("main"));
            assertThat(source.get("log.logger"), is("root.pkg.MyApp"));
            // _tmp_json_message should be removed by the pipeline
            assertThat(source.get("_tmp_json_message"), is(nullValue()));
        }

        // test malformed-JSON parsing - parsing error should be ignored and the document should be indexed with original message
        {
            indexDoc(client, dataStreamName, """
                    {
                      "@timestamp":"2023-05-10",
                      "test":"malformed_json",
                      "message": "{\\"@timestamp\\":\\"2023-05-09T16:48:34.135Z\\", \\"message\\":\\"malformed_json\\"}}"
                    }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "test": {
                        "value": "malformed_json"
                      }
                    }
                  }
                }
                """);
            assertThat(results.size(), is(1));
            Map<String, Object> source = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");

            // root field parsed from JSON should win
            assertThat(source.get("@timestamp"), is("2023-05-10"));
            assertThat(source.get("message"), is("{\"@timestamp\":\"2023-05-09T16:48:34.135Z\", \"message\":\"malformed_json\"}}"));
            assertThat(source.get("_tmp_json_message"), is(nullValue()));
        }

        // test non-string message field
        {
            indexDoc(client, dataStreamName, """
                    {
                      "message": 42,
                      "test": "numeric_message"
                    }
                """);
            List<Object> results = searchDocs(client, dataStreamName, """
                {
                  "query": {
                    "term": {
                      "test": {
                        "value": "numeric_message"
                      }
                    }
                  },
                  "fields": ["message"]
                }
                """);
            assertThat(results.size(), is(1));
            Map<String, Object> source = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");

            assertThat(source.get("message"), is(42));
            assertThat(((List<String>) fields.get("message")).get(0), is("42"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testNoSubobjects() throws Exception {
        {
            Request request = new Request("POST", "/_component_template/logs-test-subobjects-mappings");
            request.setJsonEntity("""
                {
                  "template": {
                    "settings": {
                      "mapping": {
                        "ignore_malformed": true
                      }
                    },
                    "mappings": {
                      "subobjects": false,
                      "date_detection": false,
                      "properties": {
                        "data_stream.type": {
                          "type": "constant_keyword",
                          "value": "logs"
                        },
                        "data_stream.dataset": {
                          "type": "constant_keyword"
                        },
                        "data_stream.namespace": {
                          "type": "constant_keyword"
                        }
                      }
                    }
                  }
                }
                """);
            assertOK(client.performRequest(request));
        }
        {
            Request request = new Request("POST", "/_index_template/logs-ecs-test-template");
            request.setJsonEntity("""
                {
                  "priority": 200,
                  "data_stream": {},
                  "index_patterns": ["logs-*-*"],
                  "composed_of": ["logs-test-subobjects-mappings", "ecs@mappings"]
                }
                """);
            assertOK(client.performRequest(request));
        }
        String dataStream = "logs-ecs-test-subobjects";
        createDataStream(client, dataStream);
        String backingIndexName = getWriteBackingIndex(client, dataStream);

        indexDoc(client, dataStream, """
                      {
                        "@timestamp": "2023-06-12",
                        "start_timestamp": "2023-06-08",
                        "test": "flattened",
                        "test.start_timestamp": "not a date",
                        "test.start-timestamp": "not a date",
                        "registry.data.strings": ["C:\\\\rta\\\\red_ttp\\\\bin\\\\myapp.exe"],
                        "process.title": "ssh",
                        "process.executable": "/usr/bin/ssh",
                        "process.name": "ssh",
                        "process.command_line": "/usr/bin/ssh -l user 10.0.0.16",
                        "process.working_directory": "/home/ekoren",
                        "process.io.text": "test",
                        "url.path": "/page",
                        "url.full": "https://mydomain.com/app/page",
                        "url.original": "https://mydomain.com/app/original",
                        "email.message_id": "81ce15$8r2j59@mail01.example.com",
                        "parent.url.path": "/page",
                        "parent.url.full": "https://mydomain.com/app/page",
                        "parent.url.original": "https://mydomain.com/app/original",
                        "parent.body.content": "Some content",
                        "parent.file.path": "/path/to/my/file",
                        "parent.file.target_path": "/path/to/my/file",
                        "parent.registry.data.strings": ["C:\\\\rta\\\\red_ttp\\\\bin\\\\myapp.exe"],
                        "error.stack_trace": "co.elastic.test.TestClass error:\\n at co.elastic.test.BaseTestClass",
                        "error.message": "Error occurred",
                        "file.path": "/path/to/my/file",
                        "file.target_path": "/path/to/my/file",
                        "os.full": "Mac OS Mojave",
                        "user_agent.original": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15",
                        "user.full_name": "John Doe",
                        "vulnerability.score.base": 5.5,
                        "vulnerability.score.temporal": 5.5,
                        "vulnerability.score.version": "2.0",
                        "vulnerability.textual_score": "bad",
                        "host.cpu.usage": 0.68,
                        "host.geo.location": [-73.614830, 45.505918],
                        "data_stream.dataset": "nginx.access",
                        "data_stream.namespace": "production",
                        "data_stream.custom": "whatever",
                        "structured_data": {"key1": "value1", "key2": ["value2", "value3"]},
                        "exports": {"key": "value"},
                        "top_level_imports": {"key": "value"},
                        "nested.imports": {"key": "value"},
                        "numeric_as_string": "42",
                        "socket.ip": "127.0.0.1",
                        "socket.remote_ip": "187.8.8.8"
                      }
            """);
        List<Object> hits = searchDocs(client, dataStream, """
            {
              "query": {
                "term": {
                  "test": {
                    "value": "flattened"
                  }
                }
              },
              "fields": [
                "data_stream.type",
                "host.geo.location",
                "test.start-timestamp",
                "test.start_timestamp",
                "vulnerability.textual_score"
              ]
            }
            """);
        assertThat(hits.size(), is(1));
        Map<String, Object> fields = ((Map<String, Map<String, Object>>) hits.get(0)).get("fields");
        List<String> ignored = ((Map<String, List<String>>) hits.get(0)).get("_ignored");
        Map<String, List<Object>> ignoredFieldValues = ((Map<String, Map<String, List<Object>>>) hits.get(0)).get("ignored_field_values");

        // verify that data_stream.type has the correct constant_keyword value
        assertThat(fields.get("data_stream.type"), is(List.of("logs")));
        // verify geo_point subfields evaluation
        List<Object> geoLocation = (List<Object>) fields.get("host.geo.location");
        assertThat(((Map<String, Object>) geoLocation.get(0)).get("type"), is("Point"));
        List<Double> coordinates = ((Map<String, List<Double>>) geoLocation.get(0)).get("coordinates");
        assertThat(coordinates.size(), is(2));
        assertThat(coordinates.get(0), equalTo(-73.614830));
        assertThat(coordinates.get(1), equalTo(45.505918));
        // "start-timestamp" doesn't match the ECS dynamic mapping pattern "*_timestamp"
        assertThat(fields.get("test.start-timestamp"), is(List.of("not a date")));
        assertThat(ignored.size(), is(2));
        assertThat(ignored, containsInAnyOrder("test.start_timestamp", "vulnerability.textual_score"));
        // the ECS date dynamic template enforces mapping of "*_timestamp" fields to a date type
        assertThat(ignoredFieldValues.get("test.start_timestamp").size(), is(1));
        assertThat(ignoredFieldValues.get("test.start_timestamp"), is(List.of("not a date")));
        assertThat(ignoredFieldValues.get("vulnerability.textual_score").size(), is(1));
        assertThat(ignoredFieldValues.get("vulnerability.textual_score").get(0), is("bad"));

        Map<String, Object> properties = getMappingProperties(client, backingIndexName);
        assertThat(getValueFromPath(properties, List.of("error.message", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("registry.data.strings", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("parent.registry.data.strings", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("process.io.text", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("email.message_id", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("url.path", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("parent.url.path", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("url.full", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("url.full", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("parent.url.full", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("parent.url.full", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("url.original", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("url.original", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("parent.url.original", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("parent.url.original", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("parent.body.content", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("parent.body.content", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("process.command_line", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("process.command_line", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("error.stack_trace", "type")), is("wildcard"));
        assertThat(getValueFromPath(properties, List.of("error.stack_trace", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("file.path", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("file.path", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("parent.file.path", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("parent.file.path", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("file.target_path", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("file.target_path", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("parent.file.target_path", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("parent.file.target_path", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("os.full", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("os.full", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("user_agent.original", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("user_agent.original", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("process.title", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("process.title", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("process.executable", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("process.executable", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("process.name", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("process.name", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("process.working_directory", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("process.working_directory", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("user.full_name", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("user.full_name", "fields", "text", "type")), is("match_only_text"));
        assertThat(getValueFromPath(properties, List.of("start_timestamp", "type")), is("date"));
        assertThat(getValueFromPath(properties, List.of("test.start_timestamp", "type")), is("date"));
        // testing the default mapping of string input fields to keyword if not matching any pattern
        assertThat(getValueFromPath(properties, List.of("test.start-timestamp", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("vulnerability.score.base", "type")), is("float"));
        assertThat(getValueFromPath(properties, List.of("vulnerability.score.temporal", "type")), is("float"));
        assertThat(getValueFromPath(properties, List.of("vulnerability.score.version", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("vulnerability.textual_score", "type")), is("float"));
        assertThat(getValueFromPath(properties, List.of("host.cpu.usage", "type")), is("scaled_float"));
        assertThat(getValueFromPath(properties, List.of("host.cpu.usage", "scaling_factor")), is(1000.0));
        assertThat(getValueFromPath(properties, List.of("host.geo.location", "type")), is("geo_point"));
        assertThat(getValueFromPath(properties, List.of("data_stream.dataset", "type")), is("constant_keyword"));
        assertThat(getValueFromPath(properties, List.of("data_stream.namespace", "type")), is("constant_keyword"));
        assertThat(getValueFromPath(properties, List.of("data_stream.type", "type")), is("constant_keyword"));
        // not one of the three data_stream fields that are explicitly mapped to constant_keyword
        assertThat(getValueFromPath(properties, List.of("data_stream.custom", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("structured_data", "type")), is("flattened"));
        assertThat(getValueFromPath(properties, List.of("exports", "type")), is("flattened"));
        assertThat(getValueFromPath(properties, List.of("top_level_imports", "type")), is("flattened"));
        assertThat(getValueFromPath(properties, List.of("nested.imports", "type")), is("flattened"));
        // verifying the default mapping for strings into keyword, overriding the automatic numeric string detection
        assertThat(getValueFromPath(properties, List.of("numeric_as_string", "type")), is("keyword"));
        assertThat(getValueFromPath(properties, List.of("socket.ip", "type")), is("ip"));
        assertThat(getValueFromPath(properties, List.of("socket.remote_ip", "type")), is("ip"));

    }

    public void testAllFieldsAreSearchableByDefault() throws Exception {
        final String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);

        // index a doc with "message" field and an additional one that will be mapped to a "match_only_text" type
        indexDoc(client, dataStreamName, """
            {
              "@timestamp": "2023-04-18",
              "message": "Hello world",
              "another.message": "Hi world"
            }
            """);

        // verify that both fields are searchable when not querying specific fields
        List<Object> results = searchDocs(client, dataStreamName, """
            {
              "query": {
                "simple_query_string": {
                  "query": "Hello"
                }
              }
            }
            """);
        assertEquals(1, results.size());

        results = searchDocs(client, dataStreamName, """
            {
              "query": {
                "simple_query_string": {
                  "query": "Hi"
                }
              }
            }
            """);
        assertEquals(1, results.size());
    }

    public void testDefaultFieldCustomization() throws Exception {
        Request request = new Request("POST", "/_component_template/logs@custom");
        request.setJsonEntity("""
            {
              "template": {
                "settings": {
                  "index": {
                    "query": {
                      "default_field": ["message"]
                    }
                  }
                }
              }
            }
            """);
        assertOK(client.performRequest(request));

        final String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);

        indexDoc(client, dataStreamName, """
            {
              "@timestamp": "2023-04-18",
              "message": "Hello world",
              "another.message": "Hi world"
            }
            """);

        List<Object> results = searchDocs(client, dataStreamName, """
            {
              "query": {
                "simple_query_string": {
                  "query": "Hello"
                }
              }
            }
            """);
        assertEquals(1, results.size());

        results = searchDocs(client, dataStreamName, """
            {
              "query": {
                "simple_query_string": {
                  "query": "Hi"
                }
              }
            }
            """);
        assertEquals(0, results.size());
    }

    @SuppressWarnings("unchecked")
    public void testIgnoreDynamicBeyondLimit() throws Exception {
        Request request = new Request("POST", "/_component_template/logs@custom");
        request.setJsonEntity("""
            {
              "template": {
                "settings": {
                  "index.mapping.total_fields.limit": 10
                }
              }
            }
            """);
        assertOK(client.performRequest(request));

        final String dataStreamName = "logs-generic-default";
        createDataStream(client, dataStreamName);

        indexDoc(client, dataStreamName, """
            {
              "@timestamp": "2023-04-18",
              "field1": "foo",
              "field2": "foo",
              "field3": "foo",
              "field4": "foo",
              "field5": "foo",
              "field6": "foo",
              "field7": "foo",
              "field8": "foo",
              "field9": "foo",
              "field10": "foo"
            }
            """);

        List<Object> results = searchDocs(client, dataStreamName, """
            {
              "query": {
                "match_all": { }
              },
              "fields": ["*"]
            }
            """);
        assertEquals(1, results.size());
        List<String> ignored = (List<String>) ((Map<String, ?>) results.get(0)).get("_ignored");
        assertThat(ignored, not(empty()));
        assertThat(ignored.stream().filter(i -> i.startsWith("field") == false).toList(), empty());
    }

    @Override
    protected String indexTemplateName() {
        return "logs";
    }
}
