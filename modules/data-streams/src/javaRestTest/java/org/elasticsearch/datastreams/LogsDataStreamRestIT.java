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
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LogsDataStreamRestIT extends ESRestTestCase {

    private static final String DATA_STREAM_NAME = "logs-apache-dev";
    private RestClient client;

    private static boolean columnarEnabled;

    private static final ExternalResource randomizeColumnarRule = new ExternalResource() {
        @Override
        protected void before() {
            columnarEnabled = randomBoolean();
        }
    };

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(randomizeColumnarRule).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Returns the logsdb template string, using logsdb_columnar mode when columnarEnabled is true.
     * Tests that explicitly set logsdb mode in templates must use this helper to participate in columnar randomization,
     * since the server-side cluster.logsdb_columnar.enabled upgrade only applies when no mode is set in the template.
     */
    private String logsTemplate() {
        return columnarEnabled ? LOGS_LOGSDB_COLUMNAR_TEMPLATE : LOGS_TEMPLATE;
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

    static final String LOGS_COLUMNAR_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 201,
          "template": {
            "settings": {
              "index": {
                "mode": "columnar"
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

    static final String LOGS_LOGSDB_COLUMNAR_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 201,
          "composed_of": [ "logs@mappings", "logs@settings" ],
          "template": {
            "settings": {
              "index": {
                "mode": "logsdb_columnar"
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
        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 1, DATA_STREAM_NAME);
    }

    public void testLogsStandardIndexModeSwitch() throws IOException {
        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, DATA_STREAM_NAME);

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

        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 2, DATA_STREAM_NAME);
    }

    public void testLogsTimeSeriesIndexModeSwitch() throws IOException {
        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, DATA_STREAM_NAME);

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

        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 4, DATA_STREAM_NAME);
    }

    public void testColumnarIndexing() throws IOException {
        putTemplate(client, "custom-template", LOGS_COLUMNAR_TEMPLATE);
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
        assertDataStreamBackingIndexMode("columnar", 0, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode("columnar", 1, DATA_STREAM_NAME);
    }

    public void testColumnarLogsDBIndexing() throws IOException {
        putTemplate(client, "custom-template", LOGS_LOGSDB_COLUMNAR_TEMPLATE);
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
        assertDataStreamBackingIndexMode("logsdb_columnar", 0, DATA_STREAM_NAME);
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
        assertDataStreamBackingIndexMode("logsdb_columnar", 1, DATA_STREAM_NAME);
    }

    public void testColumnarIndexModeSwitch() throws IOException {
        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", LOGS_LOGSDB_COLUMNAR_TEMPLATE);
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
        assertDataStreamBackingIndexMode("logsdb_columnar", 1, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", LOGS_COLUMNAR_TEMPLATE);
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
        assertDataStreamBackingIndexMode("columnar", 2, DATA_STREAM_NAME);

        putTemplate(client, "custom-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 3, DATA_STREAM_NAME);
    }

    public void testStandardToColumnarLogsDBMigration() throws IOException {
        assertIndexModeMigration(LOGS_STANDARD_INDEX_MODE, "standard", LOGS_LOGSDB_COLUMNAR_TEMPLATE, "logsdb_columnar");
    }

    public void testLogsDBToColumnarLogsDBMigration() throws IOException {
        assertIndexModeMigration(LOGS_TEMPLATE, "logsdb", LOGS_LOGSDB_COLUMNAR_TEMPLATE, "logsdb_columnar");
    }

    public void testColumnarLogsDBToStandardMigration() throws IOException {
        assertIndexModeMigration(LOGS_LOGSDB_COLUMNAR_TEMPLATE, "logsdb_columnar", LOGS_STANDARD_INDEX_MODE, "standard");
    }

    public void testColumnarLogsDBToLogsDBMigration() throws IOException {
        assertIndexModeMigration(LOGS_LOGSDB_COLUMNAR_TEMPLATE, "logsdb_columnar", LOGS_TEMPLATE, "logsdb");
    }

    /**
     * Creates a data stream with {@code fromTemplate}, switches the template to {@code toTemplate} and rolls over,
     * verifying the backing index modes, the indexed documents and common queries after every step. The templates are passed explicitly
     * (rather than through {@link #logsTemplate()}) so that the columnar randomization does not alter the modes under test.
     */
    private void assertIndexModeMigration(String fromTemplate, String fromMode, String toTemplate, String toMode) throws IOException {
        final List<ExpectedDoc> expectedDocs = new ArrayList<>();

        putTemplate(client, "custom-template", fromTemplate);
        createDataStream(client, DATA_STREAM_NAME);
        indexDocuments(expectedDocs, 0);
        assertDataStreamBackingIndexMode(fromMode, 0, DATA_STREAM_NAME);
        assertDocuments(expectedDocs);
        assertQueries(expectedDocs);

        // Updating the template must not affect the existing write index
        putTemplate(client, "custom-template", toTemplate);
        indexDocuments(expectedDocs, 0);
        assertDataStreamBackingIndexMode(fromMode, 0, DATA_STREAM_NAME);
        assertDocuments(expectedDocs);
        assertQueries(expectedDocs);

        rolloverDataStream(client, DATA_STREAM_NAME);
        assertDataStreamBackingIndexMode(fromMode, 0, DATA_STREAM_NAME);
        assertDataStreamBackingIndexMode(toMode, 1, DATA_STREAM_NAME);
        assertDocuments(expectedDocs);
        assertQueries(expectedDocs);

        indexDocuments(expectedDocs, 1);
        assertDataStreamBackingIndexMode(fromMode, 0, DATA_STREAM_NAME);
        assertDataStreamBackingIndexMode(toMode, 1, DATA_STREAM_NAME);
        assertDocuments(expectedDocs);
        assertQueries(expectedDocs);
    }

    private record ExpectedDoc(
        String backingIndex,
        Instant timestamp,
        String hostName,
        long pid,
        String method,
        String message,
        String ip
    ) {}

    /**
     * Bulk indexes a few thousand documents into the data stream and records them, together with the backing index
     * (identified by its position in the data stream) they are expected to land in. Three batches of at most 3000
     * documents stay below {@code index.max_result_window}, so {@link #assertDocuments} can fetch all of them at once.
     */
    private void indexDocuments(List<ExpectedDoc> expectedDocs, int writeBackingIndex) throws IOException {
        final String backingIndex = getWriteBackingIndex(client, DATA_STREAM_NAME, writeBackingIndex);
        final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final int numDocs = randomIntBetween(1000, 3000);
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            final ExpectedDoc doc = new ExpectedDoc(
                backingIndex,
                // spread over the last minute so that documents from different backing indices interleave in time
                now.minusMillis(randomLongBetween(0, 60_000)),
                // unique per document so that search hits can be matched back to what was indexed
                randomAlphaOfLength(10) + "-" + expectedDocs.size(),
                randomNonNegativeLong(),
                randomFrom("PUT", "POST", "GET"),
                randomAlphaOfLength(32),
                InetAddresses.toAddrString(randomIp(randomBoolean()))
            );
            bulk.append("{ \"create\": {} }\n");
            // bulk requires every document on a single line
            bulk.append(
                document(
                    doc.timestamp(),
                    doc.hostName(),
                    doc.pid(),
                    doc.method(),
                    doc.message(),
                    InetAddresses.forString(doc.ip()),
                    randomLongBetween(1_000_000L, 2_000_000L)
                ).replace("\n", "")
            ).append('\n');
            expectedDocs.add(doc);
        }
        final Request request = new Request("POST", "/" + DATA_STREAM_NAME + "/_bulk?refresh=true");
        request.setJsonEntity(bulk.toString());
        final Response response = client.performRequest(request);
        assertOK(response);
        assertThat("bulk request had failures", entityAsMap(response).get("errors"), is(false));
    }

    /**
     * Searches the whole data stream and verifies that exactly the expected documents are returned from the expected
     * backing indices. Uses the fields API so the check doesn't depend on how each index mode stores {@code _source}.
     */
    @SuppressWarnings("unchecked")
    private void assertDocuments(List<ExpectedDoc> expectedDocs) throws IOException {
        final Request request = new Request("GET", "/" + DATA_STREAM_NAME + "/_search");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "size": %d,
              "_source": false,
              "fields": [ { "field": "@timestamp", "format": "epoch_millis" }, "host.name", "pid", "method", "message", "ip_address" ]
            }
            """, expectedDocs.size()));
        final Map<String, Object> hitsObject = (Map<String, Object>) entityAsMap(client.performRequest(request)).get("hits");
        final List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsObject.get("hits");
        assertThat(hits.size(), equalTo(expectedDocs.size()));

        final Map<String, Map<String, Object>> hitsByHostName = new HashMap<>();
        for (Map<String, Object> hit : hits) {
            final Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            final String hostName = (String) ((List<Object>) fields.get("host.name")).get(0);
            hitsByHostName.put(hostName, hit);
        }
        assertThat(hitsByHostName.size(), equalTo(expectedDocs.size()));

        for (ExpectedDoc expected : expectedDocs) {
            final Map<String, Object> hit = hitsByHostName.get(expected.hostName());
            assertNotNull("missing document for host [" + expected.hostName() + "]", hit);
            assertThat(hit.get("_index"), equalTo(expected.backingIndex()));
            final Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
            assertThat(firstValue(fields, "@timestamp"), equalTo(Long.toString(expected.timestamp().toEpochMilli())));
            assertThat(((Number) firstValue(fields, "pid")).longValue(), equalTo(expected.pid()));
            assertThat(firstValue(fields, "method"), equalTo(expected.method()));
            assertThat(firstValue(fields, "message"), equalTo(expected.message()));
            assertThat(firstValue(fields, "ip_address"), equalTo(expected.ip()));
        }
    }

    /**
     * Runs common queries over a random time range across all backing indices and verifies the responses: the search and
     * the ES|QL request Kibana Discover sends, and an ES|QL aggregation.
     */
    private void assertQueries(List<ExpectedDoc> expectedDocs) throws IOException {
        final long from = randomFrom(expectedDocs).timestamp().toEpochMilli();
        final long to = randomFrom(expectedDocs).timestamp().toEpochMilli();
        final Instant gte = Instant.ofEpochMilli(Math.min(from, to));
        final Instant lte = Instant.ofEpochMilli(Math.max(from, to));
        final List<ExpectedDoc> inRange = expectedDocs.stream()
            .filter(doc -> doc.timestamp().isBefore(gte) == false && doc.timestamp().isAfter(lte) == false)
            .sorted(Comparator.comparing(ExpectedDoc::timestamp).reversed())
            .toList();
        final Map<String, ExpectedDoc> docsByHostName = expectedDocs.stream()
            .collect(Collectors.toMap(ExpectedDoc::hostName, Function.identity()));

        assertDiscoverSearch(gte, lte, inRange, docsByHostName);
        assertDiscoverEsql(gte, lte, inRange, docsByHostName);
        assertEsqlStats(expectedDocs);
    }

    /**
     * Simulates the search Kibana Discover sends: a time range filter, the most recent hits sorted by {@code @timestamp}
     * with all fields, an exact total hit count and a date histogram.
     */
    @SuppressWarnings("unchecked")
    private void assertDiscoverSearch(Instant gte, Instant lte, List<ExpectedDoc> inRange, Map<String, ExpectedDoc> docsByHostName)
        throws IOException {
        final int size = 500;
        final Request request = new Request("GET", "/" + DATA_STREAM_NAME + "/_search");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {
              "size": %d,
              "track_total_hits": true,
              "sort": [ { "@timestamp": { "order": "desc", "unmapped_type": "boolean" } } ],
              "_source": false,
              "fields": [ { "field": "*", "include_unmapped": true } ],
              "query": {
                "bool": {
                  "filter": [
                    { "range": { "@timestamp": { "gte": "%s", "lte": "%s", "format": "strict_date_optional_time" } } }
                  ]
                }
              },
              "aggs": {
                "histogram": {
                  "date_histogram": { "field": "@timestamp", "fixed_interval": "1s", "time_zone": "UTC", "min_doc_count": 1 }
                }
              }
            }
            """, size, gte, lte));
        final Map<String, Object> response = entityAsMap(client.performRequest(request));

        final Map<String, Object> hitsObject = (Map<String, Object>) response.get("hits");
        assertThat(((Map<String, Object>) hitsObject.get("total")).get("value"), equalTo(inRange.size()));
        final List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsObject.get("hits");
        assertThat(hits.size(), equalTo(Math.min(size, inRange.size())));
        for (int i = 0; i < hits.size(); i++) {
            final Map<String, Object> hit = hits.get(i);
            // documents can share a timestamp, so check the sort order by timestamp and the content by host name
            final long timestamp = ((Number) ((List<Object>) hit.get("sort")).get(0)).longValue();
            assertThat(timestamp, equalTo(inRange.get(i).timestamp().toEpochMilli()));
            final ExpectedDoc expected = docsByHostName.get((String) firstValue((Map<String, Object>) hit.get("fields"), "host.name"));
            assertNotNull(expected);
            assertThat(expected.timestamp().toEpochMilli(), equalTo(timestamp));
            assertThat(hit.get("_index"), equalTo(expected.backingIndex()));
        }

        final Map<Long, Long> expectedBuckets = inRange.stream()
            .collect(Collectors.groupingBy(doc -> doc.timestamp().truncatedTo(ChronoUnit.SECONDS).toEpochMilli(), Collectors.counting()));
        final Map<Long, Long> actualBuckets = new HashMap<>();
        final Map<String, Object> histogram = (Map<String, Object>) ((Map<String, Object>) response.get("aggregations")).get("histogram");
        for (Map<String, Object> bucket : (List<Map<String, Object>>) histogram.get("buckets")) {
            actualBuckets.put(((Number) bucket.get("key")).longValue(), ((Number) bucket.get("doc_count")).longValue());
        }
        assertThat(actualBuckets, equalTo(expectedBuckets));
    }

    /**
     * Simulates the ES|QL request Kibana Discover sends: the most recent documents, with the time range passed as a filter.
     */
    private void assertDiscoverEsql(Instant gte, Instant lte, List<ExpectedDoc> inRange, Map<String, ExpectedDoc> docsByHostName)
        throws IOException {
        final int limit = 100;
        final List<Map<String, Object>> rows = esql(String.format(Locale.ROOT, """
            {
              "query": "FROM %s | SORT @timestamp DESC | KEEP @timestamp, host.name, pid, method, message, ip_address | LIMIT %d",
              "filter": {
                "bool": {
                  "filter": [
                    { "range": { "@timestamp": { "gte": "%s", "lte": "%s", "format": "strict_date_optional_time" } } }
                  ]
                }
              }
            }
            """, DATA_STREAM_NAME, limit, gte, lte));
        assertThat(rows.size(), equalTo(Math.min(limit, inRange.size())));
        for (int i = 0; i < rows.size(); i++) {
            final Map<String, Object> row = rows.get(i);
            // documents can share a timestamp, so check the sort order by timestamp and the content by host name
            final Instant timestamp = Instant.parse((String) row.get("@timestamp"));
            assertThat(timestamp, equalTo(inRange.get(i).timestamp()));
            final ExpectedDoc expected = docsByHostName.get((String) row.get("host.name"));
            assertNotNull(expected);
            assertThat(expected.timestamp(), equalTo(timestamp));
            assertThat(((Number) row.get("pid")).longValue(), equalTo(expected.pid()));
            assertThat(row.get("method"), equalTo(expected.method()));
            assertThat(row.get("message"), equalTo(expected.message()));
            assertThat(row.get("ip_address"), equalTo(expected.ip()));
        }
    }

    /**
     * Runs an ES|QL aggregation grouping by backing index and a keyword field.
     */
    private void assertEsqlStats(List<ExpectedDoc> expectedDocs) throws IOException {
        final List<Map<String, Object>> rows = esql(String.format(Locale.ROOT, """
            { "query": "FROM %s METADATA _index | STATS c = COUNT(*) BY _index, method | LIMIT 10" }
            """, DATA_STREAM_NAME));
        final Map<List<String>, Long> expectedCounts = expectedDocs.stream()
            .collect(Collectors.groupingBy(doc -> List.of(doc.backingIndex(), doc.method()), Collectors.counting()));
        final Map<List<String>, Long> actualCounts = new HashMap<>();
        for (Map<String, Object> row : rows) {
            actualCounts.put(List.of((String) row.get("_index"), (String) row.get("method")), ((Number) row.get("c")).longValue());
        }
        assertThat(actualCounts, equalTo(expectedCounts));
    }

    /**
     * Runs an ES|QL request and returns the result rows as maps from column name to value.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> esql(String body) throws IOException {
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity(body);
        final Map<String, Object> response = entityAsMap(client.performRequest(request));
        final List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");
        final List<Map<String, Object>> rows = new ArrayList<>();
        for (List<Object> values : (List<List<Object>>) response.get("values")) {
            final Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                row.put((String) columns.get(i).get("name"), values.get(i));
            }
            rows.add(row);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static Object firstValue(Map<String, Object> fields, String field) {
        return ((List<Object>) fields.get(field)).get(0);
    }

    public void testLogsDBToStandardReindex() throws IOException {
        // LogsDB data stream
        putTemplate(client, "logs-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, "logs-apache-kafka");
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
        putTemplate(client, "logs-template", logsTemplate());
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);
    }

    public void testStandardToColumnarLogsDBReindex() throws IOException {
        // Standard data stream
        putTemplate(client, "standard-template", STANDARD_TEMPLATE);
        createDataStream(client, "standard-apache-kafka");

        // ColumnarLogsDB data stream
        putTemplate(client, "logs-template", LOGS_LOGSDB_COLUMNAR_TEMPLATE);
        createDataStream(client, "logs-apache-kafka");

        // Index some documents in the standard data stream
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

        // Reindex the standard data stream into a logsdb_columnar data stream
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
        assertDataStreamBackingIndexMode("logsdb_columnar", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);
    }

    public void testLogsDBToColumnarLogsDBReindex() throws IOException {
        // LogsDB data stream (source)
        putTemplate(client, "logs-template", logsTemplate());
        createDataStream(client, "logs-apache-kafka");

        // Index some documents in the logsdb data stream
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
        assertDataStreamBackingIndexMode(columnarEnabled ? "logsdb_columnar" : "logsdb", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);

        // Switch template to logsdb_columnar and create destination data stream
        putTemplate(client, "logs-template", LOGS_LOGSDB_COLUMNAR_TEMPLATE);
        createDataStream(client, "logs-apache-nginx");

        // Reindex the logsdb data stream into a logsdb_columnar data stream
        final Request reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity("""
            {
                "source": {
                    "index": "logs-apache-kafka"
                },
                "dest": {
                  "index": "logs-apache-nginx",
                  "op_type": "create"
                }
            }
            """);
        assertOK(client.performRequest(reindexRequest));
        assertDataStreamBackingIndexMode("logsdb_columnar", 0, "logs-apache-nginx");
        assertDocCount(client, "logs-apache-nginx", 10);
    }

    public void testColumnarToColumnarLogsDBReindex() throws IOException {
        // Columnar data stream (source)
        putTemplate(client, "logs-template", LOGS_COLUMNAR_TEMPLATE);
        createDataStream(client, "logs-apache-kafka");

        // Index some documents in the columnar data stream
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
        assertDataStreamBackingIndexMode("columnar", 0, "logs-apache-kafka");
        assertDocCount(client, "logs-apache-kafka", 10);

        // Switch template to logsdb_columnar and create destination data stream
        putTemplate(client, "logs-template", LOGS_LOGSDB_COLUMNAR_TEMPLATE);
        createDataStream(client, "logs-apache-nginx");

        // Reindex the columnar data stream into a logsdb_columnar data stream
        final Request reindexRequest = new Request("POST", "/_reindex?refresh=true");
        reindexRequest.setJsonEntity("""
            {
                "source": {
                    "index": "logs-apache-kafka"
                },
                "dest": {
                  "index": "logs-apache-nginx",
                  "op_type": "create"
                }
            }
            """);
        assertOK(client.performRequest(reindexRequest));
        assertDataStreamBackingIndexMode("logsdb_columnar", 0, "logs-apache-nginx");
        assertDocCount(client, "logs-apache-nginx", 10);
    }

    public void testLogsDBSnapshotCreateRestoreMount() throws IOException {
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(repository, FsRepository.TYPE, Settings.builder().put("location", randomAlphaOfLength(6)));

        final String index = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        IndexMode indexMode = columnarEnabled ? IndexMode.LOGSDB_COLUMNAR : IndexMode.LOGSDB;
        createIndex(client, index, Settings.builder().put("index.mode", indexMode.getName()).build());

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
        assertThat(getSettings(client, restoreIndex).get("index.mode"), Matchers.equalTo(indexMode.getName()));
    }

    public void testColumnarSnapshotCreateRestoreMount() throws IOException {
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(repository, FsRepository.TYPE, Settings.builder().put("location", randomAlphaOfLength(6)));

        final String index = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.COLUMNAR.getName()).build());

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
        assertThat(getSettings(client, restoreIndex).get("index.mode"), Matchers.equalTo(IndexMode.COLUMNAR.getName()));
    }

    public void testColumnarLogsDBSnapshotCreateRestoreMount() throws IOException {
        final String repository = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        registerRepository(repository, FsRepository.TYPE, Settings.builder().put("location", randomAlphaOfLength(6)));

        final String index = randomAlphaOfLength(12).toLowerCase(Locale.ROOT);
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.LOGSDB_COLUMNAR.getName()).build());

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
        assertThat(getSettings(client, restoreIndex).get("index.mode"), Matchers.equalTo(IndexMode.LOGSDB_COLUMNAR.getName()));
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

    // NOTE: this test will fail on snapshot creation after fixing
    // https://github.com/elastic/elasticsearch/issues/112735
    public void testColumnarSourceOnlySnapshotCreation() throws IOException {
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
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.COLUMNAR.getName()).build());

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

    // NOTE: this test will fail on snapshot creation after fixing
    // https://github.com/elastic/elasticsearch/issues/112735
    public void testColumnarLogsDBSourceOnlySnapshotCreation() throws IOException {
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
        createIndex(client, index, Settings.builder().put("index.mode", IndexMode.LOGSDB_COLUMNAR.getName()).build());

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
