/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.PathUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.datastreams.LogsDataStreamIT.createDataStream;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getMappingProperties;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getValueFromPath;
import static org.elasticsearch.datastreams.LogsDataStreamIT.getWriteBackingIndex;
import static org.elasticsearch.datastreams.LogsDataStreamIT.indexDoc;
import static org.elasticsearch.datastreams.LogsDataStreamIT.searchDocs;
import static org.elasticsearch.datastreams.LogsDataStreamIT.waitForLogs;
import static org.hamcrest.Matchers.is;

public class EcsLogsDataStreamIT extends DisabledSecurityDataStreamTestCase {

    private static final String DATA_STREAM_NAME = "logs-generic-default";
    private RestClient client;
    private String backingIndex;

    @Before
    public void setup() throws Exception {
        client = client();
        waitForLogs(client);

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
        createDataStream(client, DATA_STREAM_NAME);
        backingIndex = getWriteBackingIndex(client, DATA_STREAM_NAME);
    }

    @After
    public void cleanUp() throws IOException {
        adminClient().performRequest(new Request("DELETE", "_data_stream/*"));
    }

    @SuppressWarnings("unchecked")
    public void testElasticAgentLogEcsMappings() throws Exception {
        {
            Path path = PathUtils.get(Thread.currentThread().getContextClassLoader().getResource("ecs-logs/es-agent-ecs-log.json").toURI());
            String agentLog = Files.readString(path);
            indexDoc(client, DATA_STREAM_NAME, agentLog);
            List<Object> results = searchDocs(client, DATA_STREAM_NAME, """
                {
                  "query": {
                    "term": {
                      "test": {
                        "value": "elastic-agent-log"
                      }
                    }
                  },
                  "fields": ["message"]
                }
                """);
            assertThat(results.size(), is(1));
            Map<String, Object> source = ((Map<String, Map<String, Object>>) results.get(0)).get("_source");
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");

            // timestamp from deserialized JSON message field should win
            assertThat(source.get("@timestamp"), is("2023-05-16T13:49:40.374Z"));
            assertThat(
                ((Map<String, Map<String, String>>) source.get("kubernetes")).get("pod").get("name"),
                is("elastic-agent-managed-daemonset-jwktj")
            );
            // expecting the extracted message from within the original JSON-formatted message
            assertThat(((List<String>) fields.get("message")).get(0), is("Non-zero metrics in the last 30s"));

            Map<String, Object> properties = getMappingProperties(client, backingIndex);
            assertThat(getValueFromPath(properties, List.of("@timestamp", "type")), is("date"));
            assertThat(getValueFromPath(properties, List.of("message", "type")), is("match_only_text"));
            assertThat(
                getValueFromPath(properties, List.of("kubernetes", "properties", "pod", "properties", "name", "type")),
                is("keyword")
            );
            assertThat(getValueFromPath(properties, List.of("kubernetes", "properties", "pod", "properties", "ip", "type")), is("ip"));
            assertThat(getValueFromPath(properties, List.of("kubernetes", "properties", "pod", "properties", "test_ip", "type")), is("ip"));
            assertThat(
                getValueFromPath(
                    properties,
                    List.of("kubernetes", "properties", "labels", "properties", "pod-template-generation", "type")
                ),
                is("keyword")
            );
            assertThat(getValueFromPath(properties, List.of("log", "properties", "file", "properties", "path", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("log", "properties", "file", "properties", "path", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("host", "properties", "os", "properties", "name", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("host", "properties", "os", "properties", "name", "fields", "text", "type")),
                is("match_only_text")
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testGeneralMockupEcsMappings() throws Exception {
        {
            indexDoc(client, DATA_STREAM_NAME, """
                    {
                      "start_timestamp": "not a date",
                      "start-timestamp": "not a date",
                      "timestamp.us": 1688550340718000,
                      "test": "mockup-ecs-log",
                      "registry": {
                        "data": {
                          "strings": ["C:\\\\rta\\\\red_ttp\\\\bin\\\\myapp.exe"]
                        }
                      },
                      "process": {
                        "title": "ssh",
                        "executable": "/usr/bin/ssh",
                        "name": "ssh",
                        "command_line": "/usr/bin/ssh -l user 10.0.0.16",
                        "working_directory": "/home/ekoren",
                        "io": {
                          "text": "test"
                        }
                      },
                      "url": {
                        "path": "/page",
                        "full": "https://mydomain.com/app/page",
                        "original": "https://mydomain.com/app/original"
                      },
                      "email": {
                        "message_id": "81ce15$8r2j59@mail01.example.com"
                      },
                      "parent": {
                        "url": {
                          "path": "/page",
                          "full": "https://mydomain.com/app/page",
                          "original": "https://mydomain.com/app/original"
                        },
                        "body": {
                          "content": "Some content"
                        },
                        "file": {
                          "path": "/path/to/my/file",
                          "target_path": "/path/to/my/file"
                        },
                        "code_signature.timestamp": "2023-07-05",
                        "registry.data.strings": ["C:\\\\rta\\\\red_ttp\\\\bin\\\\myapp.exe"]
                      },
                      "error": {
                        "stack_trace": "co.elastic.test.TestClass error:\\n at co.elastic.test.BaseTestClass",
                        "message": "Error occurred"
                      },
                      "file": {
                        "path": "/path/to/my/file",
                        "target_path": "/path/to/my/file"
                      },
                      "os": {
                        "full": "Mac OS Mojave"
                      },
                      "user_agent": {
                        "original": "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15"
                      },
                      "user": {
                        "full_name": "John Doe"
                      },
                      "vulnerability": {
                        "score": {
                          "base": 5.5,
                          "temporal": 5.5,
                          "version": "2.0"
                        },
                        "textual_score": "bad"
                      },
                      "host": {
                        "cpu": {
                          "usage": 0.68
                        }
                      },
                      "geo": {
                        "location": {
                          "lon": -73.614830,
                          "lat": 45.505918
                        }
                      },
                      "data_stream": {
                        "dataset": "nginx.access",
                        "namespace": "production",
                        "custom": "whatever"
                      },
                      "structured_data": {
                        "key1": "value1",
                        "key2": ["value2", "value3"]
                      },
                      "exports": {
                        "key": "value"
                      },
                      "top_level_imports": {
                        "key": "value"
                      },
                      "nested": {
                        "imports": {
                          "key": "value"
                        }
                      },
                      "numeric_as_string": "42",
                      "socket": {
                        "ip": "127.0.0.1",
                        "remote_ip": "187.8.8.8"
                      }
                    }
                """);
            List<Object> results = searchDocs(client, DATA_STREAM_NAME, """
                {
                  "query": {
                    "term": {
                      "test": {
                        "value": "mockup-ecs-log"
                      }
                    }
                  },
                  "fields": ["start-timestamp", "start_timestamp"],
                  "script_fields": {
                    "data_stream_type": {
                      "script": {
                        "source": "doc['data_stream.type'].value"
                      }
                    }
                  }
                }
                """);
            assertThat(results.size(), is(1));
            Map<String, Object> fields = ((Map<String, Map<String, Object>>) results.get(0)).get("fields");
            List<String> ignored = ((Map<String, List<String>>) results.get(0)).get("_ignored");
            Map<String, Object> ignoredFieldValues = ((Map<String, Map<String, Object>>) results.get(0)).get("ignored_field_values");

            // the ECS date dynamic template enforces mapping of "*_timestamp" fields to a date type
            assertThat(ignored.size(), is(2));
            assertThat(ignored.get(0), is("start_timestamp"));
            List<String> startTimestampValues = (List<String>) ignoredFieldValues.get("start_timestamp");
            assertThat(startTimestampValues.size(), is(1));
            assertThat(startTimestampValues.get(0), is("not a date"));
            // "start-timestamp" doesn't match the ECS dynamic mapping pattern "*_timestamp"
            assertThat(fields.get("start-timestamp"), is(List.of("not a date")));
            // verify that data_stream.type has the correct constant_keyword value
            assertThat(fields.get("data_stream_type"), is(List.of("logs")));
            assertThat(ignored.get(1), is("vulnerability.textual_score"));

            Map<String, Object> properties = getMappingProperties(client, backingIndex);
            assertThat(getValueFromPath(properties, List.of("error", "properties", "message", "type")), is("match_only_text"));
            assertThat(
                getValueFromPath(properties, List.of("registry", "properties", "data", "properties", "strings", "type")),
                is("wildcard")
            );
            assertThat(
                getValueFromPath(
                    properties,
                    List.of("parent", "properties", "registry", "properties", "data", "properties", "strings", "type")
                ),
                is("wildcard")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "io", "properties", "text", "type")), is("wildcard"));
            assertThat(getValueFromPath(properties, List.of("email", "properties", "message_id", "type")), is("wildcard"));
            assertThat(getValueFromPath(properties, List.of("url", "properties", "path", "type")), is("wildcard"));
            assertThat(getValueFromPath(properties, List.of("parent", "properties", "url", "properties", "path", "type")), is("wildcard"));
            assertThat(getValueFromPath(properties, List.of("url", "properties", "full", "type")), is("wildcard"));
            assertThat(getValueFromPath(properties, List.of("url", "properties", "full", "fields", "text", "type")), is("match_only_text"));
            assertThat(getValueFromPath(properties, List.of("parent", "properties", "url", "properties", "full", "type")), is("wildcard"));
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "url", "properties", "full", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("url", "properties", "original", "type")), is("wildcard"));
            assertThat(
                getValueFromPath(properties, List.of("url", "properties", "original", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "url", "properties", "original", "type")),
                is("wildcard")
            );
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "url", "properties", "original", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "body", "properties", "content", "type")),
                is("wildcard")
            );
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "body", "properties", "content", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "command_line", "type")), is("wildcard"));
            assertThat(
                getValueFromPath(properties, List.of("process", "properties", "command_line", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("error", "properties", "stack_trace", "type")), is("wildcard"));
            assertThat(
                getValueFromPath(properties, List.of("error", "properties", "stack_trace", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("file", "properties", "path", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("file", "properties", "path", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("parent", "properties", "file", "properties", "path", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "file", "properties", "path", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("file", "properties", "target_path", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("file", "properties", "target_path", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "file", "properties", "target_path", "type")),
                is("keyword")
            );
            assertThat(
                getValueFromPath(
                    properties,
                    List.of("parent", "properties", "file", "properties", "target_path", "fields", "text", "type")
                ),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("os", "properties", "full", "type")), is("keyword"));
            assertThat(getValueFromPath(properties, List.of("os", "properties", "full", "fields", "text", "type")), is("match_only_text"));
            assertThat(getValueFromPath(properties, List.of("user_agent", "properties", "original", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("user_agent", "properties", "original", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "title", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("process", "properties", "title", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "executable", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("process", "properties", "executable", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "name", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("process", "properties", "name", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("process", "properties", "working_directory", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("process", "properties", "working_directory", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("user", "properties", "full_name", "type")), is("keyword"));
            assertThat(
                getValueFromPath(properties, List.of("user", "properties", "full_name", "fields", "text", "type")),
                is("match_only_text")
            );
            assertThat(getValueFromPath(properties, List.of("start_timestamp", "type")), is("date"));
            // testing the default mapping of string input fields to keyword if not matching any pattern
            assertThat(getValueFromPath(properties, List.of("start-timestamp", "type")), is("keyword"));
            assertThat(getValueFromPath(properties, List.of("timestamp", "properties", "us", "type")), is("long"));
            assertThat(
                getValueFromPath(properties, List.of("parent", "properties", "code_signature", "properties", "timestamp", "type")),
                is("date")
            );
            assertThat(
                getValueFromPath(properties, List.of("vulnerability", "properties", "score", "properties", "base", "type")),
                is("float")
            );
            assertThat(
                getValueFromPath(properties, List.of("vulnerability", "properties", "score", "properties", "temporal", "type")),
                is("float")
            );
            assertThat(
                getValueFromPath(properties, List.of("vulnerability", "properties", "score", "properties", "version", "type")),
                is("keyword")
            );
            assertThat(getValueFromPath(properties, List.of("vulnerability", "properties", "textual_score", "type")), is("float"));
            assertThat(
                getValueFromPath(properties, List.of("host", "properties", "cpu", "properties", "usage", "type")),
                is("scaled_float")
            );
            assertThat(
                getValueFromPath(properties, List.of("host", "properties", "cpu", "properties", "usage", "scaling_factor")),
                is(1000.0)
            );
            assertThat(getValueFromPath(properties, List.of("geo", "properties", "location", "type")), is("geo_point"));
            assertThat(getValueFromPath(properties, List.of("data_stream", "properties", "dataset", "type")), is("constant_keyword"));
            assertThat(getValueFromPath(properties, List.of("data_stream", "properties", "namespace", "type")), is("constant_keyword"));
            assertThat(getValueFromPath(properties, List.of("data_stream", "properties", "type", "type")), is("constant_keyword"));
            // not one of the three data_stream fields that are explicitly mapped to constant_keyword
            assertThat(getValueFromPath(properties, List.of("data_stream", "properties", "custom", "type")), is("keyword"));
            assertThat(getValueFromPath(properties, List.of("structured_data", "type")), is("flattened"));
            assertThat(getValueFromPath(properties, List.of("exports", "type")), is("flattened"));
            assertThat(getValueFromPath(properties, List.of("top_level_imports", "type")), is("flattened"));
            assertThat(getValueFromPath(properties, List.of("nested", "properties", "imports", "type")), is("flattened"));
            // verifying the default mapping for strings into keyword, overriding the automatic numeric string detection
            assertThat(getValueFromPath(properties, List.of("numeric_as_string", "type")), is("keyword"));
            assertThat(getValueFromPath(properties, List.of("socket", "properties", "ip", "type")), is("ip"));
            assertThat(getValueFromPath(properties, List.of("socket", "properties", "remote_ip", "type")), is("ip"));
        }
    }
}
