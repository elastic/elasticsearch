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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
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

import static org.elasticsearch.datastreams.LogsDataStreamRestIT.LOGS_LOGSDB_COLUMNAR_TEMPLATE;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.LOGS_STANDARD_INDEX_MODE;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.LOGS_TEMPLATE;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.assertDataStreamBackingIndexMode;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.createDataStream;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.document;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.getWriteBackingIndex;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.putTemplate;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.rolloverDataStream;
import static org.elasticsearch.datastreams.LogsDataStreamRestIT.waitForLogs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests migrating an existing data stream between the standard, logsdb and logsdb_columnar index modes by updating its
 * index template and rolling over, checking that documents in both the old and the new backing indices stay searchable.
 */
public class LogsDataStreamIndexModeMigrationRestIT extends ESRestTestCase {

    private static final String DATA_STREAM_NAME = "logs-apache-dev";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    private RestClient client;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setup() throws Exception {
        client = client();
        waitForLogs(client);
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
     * verifying the backing index modes, the indexed documents and common queries after every step.
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
}
