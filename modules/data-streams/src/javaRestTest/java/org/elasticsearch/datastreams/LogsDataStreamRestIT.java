/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LogsDataStreamRestIT extends ESRestTestCase {

    private static final String DATA_STREAM_NAME = "logs-apache-dev";
    private RestClient client;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
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

    private static final String LOGS_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 201,
          "composed_of": [ "logs@mappings", "logs@settings" ],
          "template": {
            "settings": {
              "index": {
                "mode": "logsdb"
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
                "pid": {
                  "type": "long"
                },
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                },
                "ip_address": {
                  "type": "ip"
                }
              }
            }
          }
        }""";

    private static final String STANDARD_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 201,
          "template": {
            "settings": {
              "index": {
                "mode": "standard"
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
                "pid": {
                  "type": "long"
                },
                "method": {
                  "type": "keyword"
                },
                "ip_address": {
                  "type": "ip"
                }
              }
            }
          }
        }""";

    private static final String TIME_SERIES_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 201,
          "template": {
            "settings": {
              "index": {
                "mode": "time_series",
                "look_ahead_time": "5m"
              }
            },
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "host.name": {
                  "type": "keyword",
                  "time_series_dimension": "true"
                },
                "pid": {
                  "type": "long",
                  "time_series_dimension": "true"
                },
                "method": {
                  "type": "keyword"
                },
                "ip_address": {
                  "type": "ip"
                },
                "memory_usage_bytes": {
                  "type": "long",
                  "time_series_metric": "gauge"
                }
              }
            }
          }
        }""";

    private static final String DOC_TEMPLATE = """
        {
            "@timestamp": "%s",
            "host.name": "%s",
            "pid": "%d",
            "method": "%s",
            "message": "%s",
            "ip_address": "%s",
            "memory_usage_bytes": "%d"
        }
        """;

    public void testLogsIndexing() throws IOException {
        putTemplate(client, "custom-template", LOGS_TEMPLATE);
        createDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 0);
        rolloverDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 1);
    }

    public void testLogsStandardIndexModeSwitch() throws IOException {
        putTemplate(client, "custom-template", LOGS_TEMPLATE);
        createDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 0);

        putTemplate(client, "custom-template", STANDARD_TEMPLATE);
        rolloverDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(64),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("standard", 1);

        putTemplate(client, "custom-template", LOGS_TEMPLATE);
        rolloverDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 2);
    }

    public void testLogsTimeSeriesIndexModeSwitch() throws IOException {
        putTemplate(client, "custom-template", LOGS_TEMPLATE);
        createDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now(),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 0);

        putTemplate(client, "custom-template", TIME_SERIES_TEMPLATE);
        rolloverDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now().plusSeconds(10),
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(64),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("time_series", 1);

        putTemplate(client, "custom-template", LOGS_TEMPLATE);
        rolloverDataStream(client, DATA_STREAM_NAME);
        indexDocument(
            client,
            DATA_STREAM_NAME,
            document(
                Instant.now().plusSeconds(320), // 5 mins index.look_ahead_time
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                randomIp(randomBoolean()),
                randomLongBetween(1_000_000L, 2_000_000L)
            )
        );
        assertDataStreamBackingIndexMode("logsdb", 2);
    }

    private void assertDataStreamBackingIndexMode(final String indexMode, int backingIndex) throws IOException {
        assertThat(getSettings(client, getWriteBackingIndex(client, DATA_STREAM_NAME, backingIndex)).get("index.mode"), is(indexMode));
    }

    private String document(
        final Instant timestamp,
        final String hostname,
        long pid,
        final String method,
        final String message,
        final InetAddress ipAddress,
        long memoryUsageBytes
    ) {
        return String.format(
            Locale.ROOT,
            DOC_TEMPLATE,
            DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(timestamp),
            hostname,
            pid,
            method,
            message,
            InetAddresses.toAddrString(ipAddress),
            memoryUsageBytes
        );
    }

    private static void createDataStream(final RestClient client, final String dataStreamName) throws IOException {
        Request request = new Request("PUT", "_data_stream/" + dataStreamName);
        assertOK(client.performRequest(request));
    }

    private static void putTemplate(final RestClient client, final String templateName, final String mappings) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(mappings);
        assertOK(client.performRequest(request));
    }

    private static void indexDocument(final RestClient client, String dataStreamName, String doc) throws IOException {
        final Request request = new Request("POST", "/" + dataStreamName + "/_doc?refresh=true");
        request.setJsonEntity(doc);
        final Response response = client.performRequest(request);
        assertOK(response);
        assertThat(entityAsMap(response).get("result"), equalTo("created"));
    }

    private static void rolloverDataStream(final RestClient client, final String dataStreamName) throws IOException {
        final Request request = new Request("POST", "/" + dataStreamName + "/_rollover");
        final Response response = client.performRequest(request);
        assertOK(response);
        assertThat(entityAsMap(response).get("rolled_over"), is(true));
    }

    @SuppressWarnings("unchecked")
    private static String getWriteBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSettings(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        return ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get("settings");
    }
}
