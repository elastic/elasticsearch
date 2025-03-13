/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
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
        .setting("xpack.license.self_generated.type", "trial")
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

    static final String LOGS_TEMPLATE = """
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

    static final String LOGS_STANDARD_INDEX_MODE = """
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

    static final String STANDARD_TEMPLATE = """
        {
          "index_patterns": [ "standard-*-*" ],
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

    static final String DOC_TEMPLATE = """
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
        assertDataStreamBackingIndexMode("logsdb", 0, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode("logsdb", 1, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode("logsdb", 0, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", LOGS_STANDARD_INDEX_MODE);
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
        assertDataStreamBackingIndexMode("standard", 1, DATA_STREAM_NAME);

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
        assertDataStreamBackingIndexMode("logsdb", 2, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode("logsdb", 0, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", LOGS_STANDARD_INDEX_MODE);
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
        assertDataStreamBackingIndexMode("standard", 1, DATA_STREAM_NAME);

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
        assertDataStreamBackingIndexMode("time_series", 2, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", LOGS_STANDARD_INDEX_MODE);
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
        assertDataStreamBackingIndexMode("standard", 3, DATA_STREAM_NAME);

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
        assertDataStreamBackingIndexMode("logsdb", 4, DATA_STREAM_NAME);
    }

    public void testLogsDBToStandardReindex() throws IOException {
        // LogsDB data stream
        putTemplate(client, "logs-template", LOGS_TEMPLATE);
        createDataStream(client, "logs-apache-kafka");

        // Standard data stream
        putTemplate(client, "standard-template", STANDARD_TEMPLATE);
        createDataStream(client, "standard-apache-kafka");

        // Index some documents in the LogsDB index
        for (int i = 0; i < 10; i++) {
            indexDocument(
                client,
                "logs-apache-kafka",
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
        }
        assertDataStreamBackingIndexMode("logsdb", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);

        // Reindex a LogsDB data stream into a standard data stream
        final Request reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity("""
            {
                "source": {
                    "index": "logs-apache-kafka"
                },
                "dest": {
                  "index": "standard-apache-kafka",
                  "op_type": "create"
                }
            }
            """);
        assertOK(client.performRequest(reindexRequest));
        assertDataStreamBackingIndexMode("standard", 0, "standard-apache-kafka");
        assertDocCount(client, "standard-apache-kafka", 10);
    }

    public void testStandardToLogsDBReindex() throws IOException {
        // LogsDB data stream
        putTemplate(client, "logs-template", LOGS_TEMPLATE);
        createDataStream(client, "logs-apache-kafka");

        // Standard data stream
        putTemplate(client, "standard-template", STANDARD_TEMPLATE);
        createDataStream(client, "standard-apache-kafka");

        // Index some documents in a standard index
        for (int i = 0; i < 10; i++) {
            indexDocument(
                client,
                "standard-apache-kafka",
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
        }
        assertDataStreamBackingIndexMode("standard", 0, "standard-apache-kafka");
        assertDocCount(client, "standard-apache-kafka", 10);

        // Reindex a standard data stream into a LogsDB data stream
        final Request reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity("""
            {
                "source": {
                    "index": "standard-apache-kafka"
                },
                "dest": {
                  "index": "logs-apache-kafka",
                  "op_type": "create"
                }
            }
            """);
        assertOK(client.performRequest(reindexRequest));
        assertDataStreamBackingIndexMode("logsdb", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);
    }

    public void testLogsDBSnapshotCreateRestoreMount() throws IOException {
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(repository, FsRepository.TYPE, Settings.builder().put("location", randomAlphaOfLength(6)));

        final String index = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build());

        for (int i = 0; i < 10; i++) {
            indexDocument(
                client,
                index,
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
        }

        final String snapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        deleteSnapshot(repository, snapshot, true);
        createSnapshot(client, repository, snapshot, true, index);
        wipeDataStreams();
        wipeAllIndices();
        restoreSnapshot(client, repository, snapshot, true, index);

        final String restoreIndex = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        final Request mountRequest = new Request("POST", "/_snapshot/" + repository + '/' + snapshot + "/_mount");
        mountRequest.addParameter("wait_for_completion", "true");
        mountRequest.setJsonEntity("{\"index\": \"" + index + "\",\"renamed_index\": \"" + restoreIndex + "\"}");

        assertOK(client.performRequest(mountRequest));
        assertDocCount(client, restoreIndex, 10);
        assertThat(getSettings(client, restoreIndex).get("index.mode"), Matchers.equalTo(IndexMode.LOGSDB.getName()));
    }

    // NOTE: this test will fail on snapshot creation after fixing
    // https://github.com/elastic/elasticsearch/issues/112735
    public void testLogsDBSourceOnlySnapshotCreation() throws IOException {
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(repository, FsRepository.TYPE, Settings.builder().put("location", randomAlphaOfLength(6)));
        // A source-only repository delegates storage to another repository
        final String sourceOnlyRepository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(
            sourceOnlyRepository,
            "source",
            Settings.builder().put("delegate_type", FsRepository.TYPE).put("location", repository)
        );

        final String index = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build());

        for (int i = 0; i < 10; i++) {
            indexDocument(
                client,
                index,
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
        }

        final String snapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        deleteSnapshot(sourceOnlyRepository, snapshot, true);
        createSnapshot(client, sourceOnlyRepository, snapshot, true, index);
        wipeDataStreams();
        wipeAllIndices();
        // Can't snapshot _source only on an index that has incomplete source ie. has _source disabled or filters the source
        final ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> restoreSnapshot(client, sourceOnlyRepository, snapshot, true, index)
        );
        assertThat(responseException.getMessage(), Matchers.containsString("wasn't fully snapshotted"));
    }

    private static void registerRepository(final String repository, final String type, final Settings.Builder settings) throws IOException {
        registerRepository(repository, type, false, settings.build());
    }

    private void assertDataStreamBackingIndexMode(final String indexMode, int backingIndex, final String dataStreamName)
        throws IOException {
        assertThat(getSettings(client, getWriteBackingIndex(client, dataStreamName, backingIndex)).get("index.mode"), is(indexMode));
    }

    static String document(
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

    static void putTemplate(final RestClient client, final String templateName, final String mappings) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(mappings);
        assertOK(client.performRequest(request));
    }

    static void indexDocument(final RestClient client, String indexOrtDataStream, String doc) throws IOException {
        final Request request = new Request("POST", "/" + indexOrtDataStream + "/_doc?refresh=true");
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

    private static void createSnapshot(
        RestClient restClient,
        String repository,
        String snapshot,
        boolean waitForCompletion,
        final String... indices
    ) throws IOException {
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));
        request.setJsonEntity("""
            "indices": $indices
            """.replace("$indices", String.join(", ", indices)));

        final Response response = restClient.performRequest(request);
        assertThat(
            "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }

    private static void restoreSnapshot(
        final RestClient client,
        final String repository,
        String snapshot,
        boolean waitForCompletion,
        final String... indices
    ) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot + "/_restore");
        request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));
        request.setJsonEntity("""
            "indices": $indices
            """.replace("$indices", String.join(", ", indices)));

        final Response response = client.performRequest(request);
        assertThat(
            "Failed to restore snapshot [" + snapshot + "] from repository [" + repository + "]: " + response,
            response.getStatusLine().getStatusCode(),
            equalTo(RestStatus.OK.getStatus())
        );
    }
}
