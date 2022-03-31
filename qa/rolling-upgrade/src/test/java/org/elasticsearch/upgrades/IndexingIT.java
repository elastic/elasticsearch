/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

/**
 * Basic test that indexed documents survive the rolling restart. See
 * {@link RecoveryIT} for much more in depth testing of the mechanism
 * by which they survive.
 * <p>
 * This test is an almost exact copy of <code>IndexingIT</code> in the
 * xpack rolling restart tests. We should work on a way to remove this
 * duplication but for now we have no real way to share code.
 */
public class IndexingIT extends AbstractRollingTestCase {

    public void testIndexing() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
                break;
            case MIXED:
                Request waitForYellow = new Request("GET", "/_cluster/health");
                waitForYellow.addParameter("wait_for_nodes", "3");
                waitForYellow.addParameter("wait_for_status", "yellow");
                client().performRequest(waitForYellow);
                break;
            case UPGRADED:
                Request waitForGreen = new Request("GET", "/_cluster/health/test_index,index_with_replicas,empty_index");
                waitForGreen.addParameter("wait_for_nodes", "3");
                waitForGreen.addParameter("wait_for_status", "green");
                // wait for long enough that we give delayed unassigned shards to stop being delayed
                waitForGreen.addParameter("timeout", "70s");
                waitForGreen.addParameter("level", "shards");
                client().performRequest(waitForGreen);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {
            Request createTestIndex = new Request("PUT", "/test_index");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createTestIndex);
            client().performRequest(createTestIndex);

            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";
            Request createIndexWithReplicas = new Request("PUT", "/index_with_replicas");
            createIndexWithReplicas.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createIndexWithReplicas);
            client().performRequest(createIndexWithReplicas);

            Request createEmptyIndex = new Request("PUT", "/empty_index");
            // Ask for recovery to be quick
            createEmptyIndex.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createEmptyIndex);
            client().performRequest(createEmptyIndex);

            bulk("test_index", "_OLD", 5);
            bulk("index_with_replicas", "_OLD", 5);
        }

        int expectedCount;
        switch (CLUSTER_TYPE) {
            case OLD:
                expectedCount = 5;
                break;
            case MIXED:
                if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                    expectedCount = 5;
                } else {
                    expectedCount = 10;
                }
                break;
            case UPGRADED:
                expectedCount = 15;
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (CLUSTER_TYPE != ClusterType.OLD) {
            bulk("test_index", "_" + CLUSTER_TYPE, 5);
            Request toBeDeleted = new Request("PUT", "/test_index/_doc/to_be_deleted");
            toBeDeleted.addParameter("refresh", "true");
            toBeDeleted.setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            assertCount("test_index", expectedCount + 6);

            Request delete = new Request("DELETE", "/test_index/_doc/to_be_deleted");
            delete.addParameter("refresh", "true");
            client().performRequest(delete);

            assertCount("test_index", expectedCount + 5);
        }
    }

    public void testAutoIdWithOpTypeCreate() throws IOException {
        final String indexName = "auto_id_and_op_type_create_index";
        String b = """
            {"create": {"_index": "%s"}}
            {"f1": "v"}
            """.formatted(indexName);
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b);

        switch (CLUSTER_TYPE) {
            case OLD -> {
                Request createTestIndex = new Request("PUT", "/" + indexName);
                createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
                client().performRequest(createTestIndex);
            }
            case MIXED -> {
                Request waitForGreen = new Request("GET", "/_cluster/health");
                waitForGreen.addParameter("wait_for_nodes", "3");
                client().performRequest(waitForGreen);
                Version minNodeVersion = minNodeVersion();
                if (minNodeVersion.before(Version.V_7_5_0)) {
                    ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(bulk));
                    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                    assertThat(
                        e.getMessage(),
                        // if request goes to 7.5+ node
                        either(containsString("optype create not supported for indexing requests without explicit id until"))
                            // if request goes to < 7.5 node
                            .or(containsString("an id must be provided if version type or value are set"))
                    );
                } else {
                    client().performRequest(bulk);
                }
            }
            case UPGRADED -> client().performRequest(bulk);
            default -> throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testDateNanosFormatUpgrade() throws IOException {
        final String indexName = "test_date_nanos";
        switch (CLUSTER_TYPE) {
            case OLD -> {
                Request createIndex = new Request("PUT", "/" + indexName);
                XContentBuilder mappings = XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .startObject("mappings")
                    .startObject("properties")
                    .startObject("date")
                    .field("type", "date")
                    .endObject()
                    .startObject("date_nanos")
                    .field("type", "date_nanos")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
                createIndex.setJsonEntity(Strings.toString(mappings));
                client().performRequest(createIndex);
                Request index = new Request("POST", "/" + indexName + "/_doc/");
                XContentBuilder doc = XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .field("date", "2015-01-01T12:10:30.123456789Z")
                    .field("date_nanos", "2015-01-01T12:10:30.123456789Z")
                    .endObject();
                index.addParameter("refresh", "true");
                index.setJsonEntity(Strings.toString(doc));
                client().performRequest(index);
            }
            case UPGRADED -> {
                Request search = new Request("POST", "/" + indexName + "/_search");
                XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .array("fields", new String[] { "date", "date_nanos" })
                    .endObject();
                search.setJsonEntity(Strings.toString(query));
                Map<String, Object> response = entityAsMap(client().performRequest(search));
                Map<?, ?> bestHit = (Map<?, ?>) ((List<?>) (XContentMapValues.extractValue("hits.hits", response))).get(0);
                List<?> date = (List<?>) XContentMapValues.extractValue("fields.date", bestHit);
                assertThat(date.size(), equalTo(1));
                assertThat(date.get(0), equalTo("2015-01-01T12:10:30.123Z"));
                List<?> dateNanos = (List<?>) XContentMapValues.extractValue("fields.date_nanos", bestHit);
                assertThat(dateNanos.size(), equalTo(1));
                assertThat(dateNanos.get(0), equalTo("2015-01-01T12:10:30.123456789Z"));
            }
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        bulk(index, b.toString());
    }

    private static final List<String> TSDB_DIMS = List.of("6a841a21", "947e4ced", "a4c385a1", "b47a2f4e", "df3145b3");
    private static final long[] TSDB_TIMES;
    static {
        String[] times = new String[] {
            "2021-01-01T00:00:00Z",
            "2021-01-02T00:00:00Z",
            "2021-01-02T00:10:00Z",
            "2021-01-02T00:20:00Z",
            "2021-01-02T00:30:00Z" };
        TSDB_TIMES = new long[times.length];
        for (int i = 0; i < times.length; i++) {
            TSDB_TIMES[i] = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(times[i]);
        }
    }

    public void testTsdb() throws IOException {
        assumeTrue("indexing time series indices changed in 8.2.0", UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_2_0));

        StringBuilder bulk = new StringBuilder();
        switch (CLUSTER_TYPE) {
            case OLD -> {
                createTsdbIndex();
                tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[0], TSDB_TIMES[1], 0.1);
                tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[0], TSDB_TIMES[1], -0.1);
                bulk("tsdb", bulk.toString());
                assertTsdbAgg(closeTo(215.95, 0.005), closeTo(-215.95, 0.005));
                return;
            }
            case MIXED -> {
                if (FIRST_MIXED_ROUND) {
                    tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[1], TSDB_TIMES[2], 0.1);
                    tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[1], TSDB_TIMES[2], -0.1);
                    tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[0], TSDB_TIMES[2], 1.1);
                    bulk("tsdb", bulk.toString());
                    assertTsdbAgg(closeTo(217.45, 0.005), closeTo(-217.45, 0.005), closeTo(2391.95, 0.005));
                    return;
                }
                tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[2], TSDB_TIMES[3], 0.1);
                tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[2], TSDB_TIMES[3], -0.1);
                tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[2], TSDB_TIMES[3], 1.1);
                tsdbBulk(bulk, TSDB_DIMS.get(3), TSDB_TIMES[0], TSDB_TIMES[3], 10);
                bulk("tsdb", bulk.toString());
                assertTsdbAgg(closeTo(218.95, 0.005), closeTo(-218.95, 0.005), closeTo(2408.45, 0.005), closeTo(21895, 0.5));
                return;
            }
            case UPGRADED -> {
                tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[3], TSDB_TIMES[4], 0.1);
                tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[3], TSDB_TIMES[4], -0.1);
                tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[3], TSDB_TIMES[4], 1.1);
                tsdbBulk(bulk, TSDB_DIMS.get(3), TSDB_TIMES[3], TSDB_TIMES[4], 10);
                tsdbBulk(bulk, TSDB_DIMS.get(4), TSDB_TIMES[0], TSDB_TIMES[4], -5);
                bulk("tsdb", bulk.toString());
                assertTsdbAgg(
                    closeTo(220.45, 0.005),
                    closeTo(-220.45, 0.005),
                    closeTo(2424.95, 0.005),
                    closeTo(22045, 0.5),
                    closeTo(-11022.5, 0.5)
                );
                return;
            }
        }
    }

    private void bulk(String index, String entity) throws IOException {
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(entity.toString());
        assertThat(EntityUtils.toString(client().performRequest(bulk).getEntity()), containsString("\"errors\":false"));
    }

    private void createTsdbIndex() throws IOException {
        Request createIndex = new Request("PUT", "/tsdb");
        XContentBuilder indexSpec = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
        indexSpec.startObject("mappings").startObject("properties");
        {
            indexSpec.startObject("@timestamp").field("type", "date").endObject();
            indexSpec.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
        }
        indexSpec.endObject().endObject();
        indexSpec.startObject("settings").startObject("index");
        indexSpec.field("mode", "time_series");
        indexSpec.array("routing_path", new String[] { "dim" });
        indexSpec.field("time_series.start_time", 1L);
        indexSpec.field("time_series.end_time", DateUtils.MAX_MILLIS_BEFORE_9999 - 1);
        indexSpec.endObject().endObject();
        createIndex.setJsonEntity(Strings.toString(indexSpec.endObject()));
        client().performRequest(createIndex);
    }

    private void tsdbBulk(StringBuilder bulk, String dim, long timeStart, long timeEnd, double rate) throws IOException {
        long delta = TimeUnit.SECONDS.toMillis(20);
        double value = (timeStart - TSDB_TIMES[0]) / TimeUnit.SECONDS.toMillis(20) * rate;
        for (long t = timeStart; t < timeEnd; t += delta) {
            bulk.append("""
                {"index": {"_index": "tsdb"}}
                {"@timestamp": %s, "dim": "%s", "value": %s}
                """.formatted(t, dim, value));
            value += rate;
        }
    }

    private void assertTsdbAgg(Matcher<?>... expected) throws IOException {
        Request request = new Request("POST", "/tsdb/_search");
        request.addParameter("size", "0");
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.startObject("aggs").startObject("tsids");
        {
            body.startObject("terms").field("field", "_tsid").endObject();
            body.startObject("aggs").startObject("avg");
            {
                body.startObject("avg").field("field", "value").endObject();
            }
            body.endObject().endObject();
        }
        body.endObject().endObject();
        request.setJsonEntity(Strings.toString(body.endObject()));
        ListMatcher tsidsExpected = matchesList();
        for (int d = 0; d < expected.length; d++) {
            Object key = Map.of("dim", TSDB_DIMS.get(d));
            tsidsExpected = tsidsExpected.item(matchesMap().extraOk().entry("key", key).entry("avg", Map.of("value", expected[d])));
        }
        assertMap(
            entityAsMap(client().performRequest(request)),
            matchesMap().extraOk()
                .entry("aggregations", matchesMap().entry("tsids", matchesMap().extraOk().entry("buckets", tsidsExpected)))
        );
    }

    private void assertCount(String index, int count) throws IOException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals(
            "{\"hits\":{\"total\":" + count + "}}",
            EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8)
        );
    }

    private Version minNodeVersion() throws IOException {
        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
        Version minNodeVersion = null;
        for (Map.Entry<?, ?> node : nodes.entrySet()) {
            Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
            Version nodeVersion = Version.fromString(nodeInfo.get("version").toString());
            if (minNodeVersion == null) {
                minNodeVersion = nodeVersion;
            } else if (nodeVersion.before(minNodeVersion)) {
                minNodeVersion = nodeVersion;
            }
        }
        return minNodeVersion;
    }
}
