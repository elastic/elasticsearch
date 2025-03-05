/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
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
import java.util.function.Predicate;

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
 * {@code RecoveryIT} for much more in depth testing of the mechanism
 * by which they survive.
 * <p>
 * This test is an almost exact copy of <code>IndexingIT</code> in the
 * xpack rolling restart tests. We should work on a way to remove this
 * duplication but for now we have no real way to share code.
 */
public class IndexingIT extends AbstractRollingUpgradeTestCase {

    public IndexingIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testIndexing() throws IOException {
        if (isMixedCluster()) {
            Request waitForYellow = new Request("GET", "/_cluster/health");
            waitForYellow.addParameter("wait_for_nodes", "3");
            waitForYellow.addParameter("wait_for_status", "yellow");
            client().performRequest(waitForYellow);
        } else if (isUpgradedCluster()) {
            Request waitForGreen = new Request("GET", "/_cluster/health/test_index,index_with_replicas,empty_index");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            // wait for long enough that we give delayed unassigned shards to stop being delayed
            waitForGreen.addParameter("timeout", "70s");
            waitForGreen.addParameter("level", "shards");
            client().performRequest(waitForGreen);
        }

        if (isOldCluster()) {
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
        if (isOldCluster() || isFirstMixedCluster()) {
            expectedCount = 5;
        } else if (isMixedCluster()) {
            expectedCount = 10;
        } else {
            expectedCount = 15;
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (isOldCluster() == false) {
            bulk("test_index", "_" + (isMixedCluster() ? "MIXED" : "UPGRADED"), 5);
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
        String b = Strings.format("""
            {"create": {"_index": "%s"}}
            {"f1": "v"}
            """, indexName);
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b);

        if (isOldCluster()) {
            Request createTestIndex = new Request("PUT", "/" + indexName);
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);
        } else if (isMixedCluster()) {
            Request waitForGreen = new Request("GET", "/_cluster/health");
            waitForGreen.addParameter("wait_for_nodes", "3");
            client().performRequest(waitForGreen);
            if (clusterSupportsBulkApi() == false) {
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
        } else if (isUpgradedCluster()) {
            client().performRequest(bulk);
        }
    }

    public void testDateNanosFormatUpgrade() throws IOException {
        final String indexName = "test_date_nanos";
        if (isOldCluster()) {
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
        } else if (isUpgradedCluster()) {
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

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        bulk(index, b.toString());
    }

    private static final List<String> TSDB_DIMS = List.of("6a841a21", "947e4ced", "a4c385a1", "b47a2f4e", "df3145b3");

    private static final List<String> EXPECTED_TSDB_TSIDS_NODES_0 = List.of(
        "JFFUy14C9UcX3MnFnsFrpf0UyQYeoe87sMXUQ025sHhvhKDa4g",
        "JFFUy14C9UcX3MnFnsFrpf3X1Mw5gSg0zb_Y50PDlARq0q0ANA"
    );

    private static final List<String> EXPECTED_TSDB_TSIDS_NODES_1 = List.of(
        "JFFUy14C9UcX3MnFnsFrpf0UyQYeoe87sMXUQ025sHhvhKDa4g",
        "JFFUy14C9UcX3MnFnsFrpf1qe4IAVEfi3wL8wnx3pd_rA41HpA",
        "JFFUy14C9UcX3MnFnsFrpf3X1Mw5gSg0zb_Y50PDlARq0q0ANA"
    );

    private static final List<String> EXPECTED_TSDB_TSIDS_NODES_2 = List.of(
        "JFFUy14C9UcX3MnFnsFrpf0UyQYeoe87sMXUQ025sHhvhKDa4g",
        "JFFUy14C9UcX3MnFnsFrpf1fK0gnGg01X2P0BuJX0wb-7i2pqA",
        "JFFUy14C9UcX3MnFnsFrpf1qe4IAVEfi3wL8wnx3pd_rA41HpA",
        "JFFUy14C9UcX3MnFnsFrpf3X1Mw5gSg0zb_Y50PDlARq0q0ANA"
    );
    private static final List<String> EXPECTED_TSDB_TSIDS_NODES_3 = List.of(
        "JFFUy14C9UcX3MnFnsFrpf0UyQYeoe87sMXUQ025sHhvhKDa4g",
        "JFFUy14C9UcX3MnFnsFrpf0ayKYYMlhssuNhG-tPuituN3POiA",
        "JFFUy14C9UcX3MnFnsFrpf1fK0gnGg01X2P0BuJX0wb-7i2pqA",
        "JFFUy14C9UcX3MnFnsFrpf1qe4IAVEfi3wL8wnx3pd_rA41HpA",
        "JFFUy14C9UcX3MnFnsFrpf3X1Mw5gSg0zb_Y50PDlARq0q0ANA"
    );
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
        final Version oldClusterVersion = Version.fromString(getOldClusterVersion());

        StringBuilder bulk = new StringBuilder();
        if (isOldCluster()) {
            createTsdbIndex();
            tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[0], TSDB_TIMES[1], 0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[0], TSDB_TIMES[1], -0.1);
            bulk("tsdb", bulk.toString());
            assertTsdbAgg(oldClusterVersion, EXPECTED_TSDB_TSIDS_NODES_0, closeTo(215.95, 0.005), closeTo(-215.95, 0.005));
        } else if (isFirstMixedCluster()) {
            tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[1], TSDB_TIMES[2], 0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[1], TSDB_TIMES[2], -0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[0], TSDB_TIMES[2], 1.1);
            bulk("tsdb", bulk.toString());
            if (oldClusterVersion.onOrAfter(Version.V_8_13_0)) {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_1,
                    closeTo(217.45, 0.005),
                    closeTo(2391.95, 0.005),
                    closeTo(-217.45, 0.005)
                );
            } else {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_1,
                    closeTo(217.45, 0.005),
                    closeTo(-217.45, 0.005),
                    closeTo(2391.95, 0.005)
                );
            }
        } else if (isMixedCluster()) {
            tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[2], TSDB_TIMES[3], 0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[2], TSDB_TIMES[3], -0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[2], TSDB_TIMES[3], 1.1);
            tsdbBulk(bulk, TSDB_DIMS.get(3), TSDB_TIMES[0], TSDB_TIMES[3], 10);
            bulk("tsdb", bulk.toString());
            if (oldClusterVersion.onOrAfter(Version.V_8_13_0)) {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_2,
                    closeTo(218.95, 0.5),
                    closeTo(21895.0, 0.005),
                    closeTo(2408.45, 0.005),
                    closeTo(-218.95, 0.005)
                );
            } else {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_2,
                    closeTo(218.95, 0.005),
                    closeTo(-218.95, 0.005),
                    closeTo(2408.45, 0.005),
                    closeTo(21895, 0.5)
                );
            }
        } else {
            tsdbBulk(bulk, TSDB_DIMS.get(0), TSDB_TIMES[3], TSDB_TIMES[4], 0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(1), TSDB_TIMES[3], TSDB_TIMES[4], -0.1);
            tsdbBulk(bulk, TSDB_DIMS.get(2), TSDB_TIMES[3], TSDB_TIMES[4], 1.1);
            tsdbBulk(bulk, TSDB_DIMS.get(3), TSDB_TIMES[3], TSDB_TIMES[4], 10);
            tsdbBulk(bulk, TSDB_DIMS.get(4), TSDB_TIMES[0], TSDB_TIMES[4], -5);
            bulk("tsdb", bulk.toString());
            if (oldClusterVersion.onOrAfter(Version.V_8_13_0)) {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_3,
                    closeTo(220.45, 0.005),
                    closeTo(-11022.5, 0.5),
                    closeTo(22045, 0.5),
                    closeTo(2424.95, 0.005),
                    closeTo(-220.45, 0.005)
                );
            } else {
                assertTsdbAgg(
                    oldClusterVersion,
                    EXPECTED_TSDB_TSIDS_NODES_3,
                    closeTo(220.45, 0.005),
                    closeTo(-220.45, 0.005),
                    closeTo(2424.95, 0.005),
                    closeTo(22045, 0.5),
                    closeTo(-11022.5, 0.5)
                );
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
            bulk.append(Strings.format("""
                {"index": {"_index": "tsdb"}}
                {"@timestamp": %s, "dim": "%s", "value": %s}
                """, t, dim, value));
            value += rate;
        }
    }

    private void assertTsdbAgg(final Version oldClusterVersion, final List<String> expectedTsids, final Matcher<?>... expected)
        throws IOException {
        @UpdateForV9(owner = UpdateForV9.Owner.SEARCH_ANALYTICS)
        boolean onOrAfterTsidHashingVersion = oldClusterVersion.onOrAfter(Version.V_8_13_0);
        Request request = new Request("POST", "/tsdb/_search");
        request.addParameter("size", "0");
        XContentBuilder body = JsonXContent.contentBuilder().startObject();
        body.startObject("aggs").startObject("tsids");
        {
            body.startObject("terms").field("field", TimeSeriesIdFieldMapper.NAME).endObject();
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
            // NOTE: from Version 8.12.0 on we use tsid hashing for the _tsid field
            Object key = onOrAfterTsidHashingVersion ? expectedTsids.get(d) : Map.of("dim", IndexingIT.TSDB_DIMS.get(d));
            tsidsExpected = tsidsExpected.item(matchesMap().extraOk().entry("key", key).entry("avg", Map.of("value", expected[d])));
        }
        assertMap(
            entityAsMap(client().performRequest(request)),
            matchesMap().extraOk()
                .entry("aggregations", matchesMap().entry("tsids", matchesMap().extraOk().entry("buckets", tsidsExpected)))
        );
    }

    public void testSyntheticSource() throws IOException {
        if (isOldCluster()) {
            Request createIndex = new Request("PUT", "/synthetic");
            XContentBuilder indexSpec = XContentBuilder.builder(XContentType.JSON.xContent()).startObject();
            boolean useIndexSetting = SourceFieldMapper.onOrAfterDeprecateModeVersion(getOldClusterIndexVersion());
            if (useIndexSetting) {
                indexSpec.startObject("settings").field("index.mapping.source.mode", "synthetic").endObject();
            }
            indexSpec.startObject("mappings");
            {
                if (useIndexSetting == false) {
                    indexSpec.startObject("_source").field("mode", "synthetic").endObject();
                }
                indexSpec.startObject("properties").startObject("kwd").field("type", "keyword").endObject().endObject();
            }
            indexSpec.endObject();
            createIndex.setJsonEntity(Strings.toString(indexSpec.endObject()));
            client().performRequest(createIndex);
            bulk("synthetic", """
                {"index": {"_index": "synthetic", "_id": "old"}}
                {"kwd": "old", "int": -12}
                """);
        } else if (isFirstMixedCluster()) {
            bulk("synthetic", """
                {"index": {"_index": "synthetic", "_id": "mixed_1"}}
                {"kwd": "mixed_1", "int": 22}
                """);
        } else if (isMixedCluster()) {
            bulk("synthetic", """
                {"index": {"_index": "synthetic", "_id": "mixed_2"}}
                {"kwd": "mixed_2", "int": 33}
                """);

        } else {
            bulk("synthetic", """
                {"index": {"_index": "synthetic", "_id": "new"}}
                {"kwd": "new", "int": 21341325}
                """);
        }

        assertMap(
            entityAsMap(client().performRequest(new Request("GET", "/synthetic/_doc/old"))),
            matchesMap().extraOk().entry("_source", matchesMap().entry("kwd", "old").entry("int", -12))
        );
        if (isOldCluster()) {
            return;
        }
        assertMap(
            entityAsMap(client().performRequest(new Request("GET", "/synthetic/_doc/mixed_1"))),
            matchesMap().extraOk().entry("_source", matchesMap().entry("kwd", "mixed_1").entry("int", 22))
        );
        if (isFirstMixedCluster()) {
            return;
        }
        assertMap(
            entityAsMap(client().performRequest(new Request("GET", "/synthetic/_doc/mixed_2"))),
            matchesMap().extraOk().entry("_source", matchesMap().entry("kwd", "mixed_2").entry("int", 33))
        );
        if (isMixedCluster()) {
            return;
        }
        assertMap(
            entityAsMap(client().performRequest(new Request("GET", "/synthetic/_doc/new"))),
            matchesMap().extraOk().entry("_source", matchesMap().entry("kwd", "new").entry("int", 21341325))
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

    // TODO[lor]: replace this check with a (historical) feature check ("supports bulk requests")
    private boolean clusterSupportsBulkApi() throws IOException {
        Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
        Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");

        Predicate<Map<?, ?>> nodeSupportsBulkApi = n -> Version.fromString(n.get("version").toString()).onOrAfter(Version.V_7_5_0);

        return nodes.values().stream().map(o -> (Map<?, ?>) o).allMatch(nodeSupportsBulkApi);
    }
}
