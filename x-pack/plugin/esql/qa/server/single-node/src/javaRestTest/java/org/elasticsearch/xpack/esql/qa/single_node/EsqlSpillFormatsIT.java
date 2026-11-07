/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * A result wider than the result budget goes to a run and comes back through the REST layer's continuations, over
 * the real HTTP transport: every format renders the same bytes as the same result kept in memory, and the node ends
 * with nothing in its store. The unit tests drive the response's parts over a fake channel; this is the one place the
 * continuation, the empty chunk a part must give and the close hook run under the transport they were written for.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlSpillFormatsIT extends ESRestTestCase {
    private static final String INDEX = "spill_formats";
    private static final int ROWS = 1_500;
    private static final String QUERY = "FROM " + INDEX + " | SORT key | LIMIT " + ROWS + " | KEEP key, value, empty";

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void enableSpilling() throws IOException {
        assumeTrue("the spill settings and pragmas exist in snapshot builds", Build.current().isSnapshot());
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            {"persistent": {"esql.spill.enabled": true}}""");
        assertEquals(200, client().performRequest(settings).getStatusLine().getStatusCode());
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"index.number_of_shards": 2, "index.number_of_replicas": 0},
              "mappings": {"properties": {"key": {"type": "long"}, "value": {"type": "keyword"}, "empty": {"type": "keyword"}}}
            }""");
        assertEquals(200, client().performRequest(create).getStatusLine().getStatusCode());
        StringBuilder bulk = new StringBuilder();
        for (int key = 0; key < ROWS; key++) {
            bulk.append("{\"index\": {}}\n");
            bulk.append(String.format(Locale.ROOT, "{\"key\": %d, \"value\": \"%s\"}\n", key, randomAlphaOfLength(300)));
        }
        Request index = new Request("POST", "/_bulk?index=" + INDEX + "&refresh=true");
        index.setJsonEntity(bulk.toString());
        assertEquals(200, client().performRequest(index).getStatusLine().getStatusCode());
    }

    @After
    public void disableSpilling() throws Exception {
        if (Build.current().isSnapshot() == false) {
            return;
        }
        assertStoreEmpty();
        Request settings = new Request("PUT", "/_cluster/settings");
        settings.setJsonEntity("""
            {"persistent": {"esql.spill.enabled": null}}""");
        assertEquals(200, client().performRequest(settings).getStatusLine().getStatusCode());
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    public void testJsonRows() throws IOException {
        assertSameXContent("json", false, false);
    }

    public void testJsonColumnar() throws IOException {
        assertSameXContent("json", true, false);
    }

    public void testJsonDroppingNullColumns() throws IOException {
        assertSameXContent("json", randomBoolean(), true);
    }

    public void testYaml() throws IOException {
        assertSameXContent("yaml", randomBoolean(), randomBoolean());
    }

    public void testCbor() throws IOException {
        assertSameXContent("cbor", randomBoolean(), randomBoolean());
    }

    public void testSmile() throws IOException {
        assertSameXContent("smile", randomBoolean(), randomBoolean());
    }

    public void testCsv() throws IOException {
        assertSameBytes("csv", "");
    }

    public void testCsvDroppingNullColumns() throws IOException {
        assertSameBytes("csv", "&drop_null_columns=true");
    }

    public void testTsv() throws IOException {
        assertSameBytes("tsv", "");
    }

    public void testText() throws IOException {
        assertSameBytes("txt", "");
    }

    public void testTextDroppingNullColumns() throws IOException {
        assertSameBytes("txt", "&drop_null_columns=true");
    }

    public void testArrow() throws IOException {
        assertSameBytes("arrow", "");
    }

    public void testArrowDroppingNullColumns() throws IOException {
        assertSameBytes("arrow", "&drop_null_columns=true");
    }

    /**
     * The XContent formats carry the time the query took and spent, which no two runs share, so they are compared as
     * parsed with those fields removed.
     */
    private void assertSameXContent(String format, boolean columnar, boolean dropNullColumns) throws IOException {
        String params = "&drop_null_columns=" + dropNullColumns;
        XContentType type = XContentType.fromFormat(format);
        Map<String, Object> expected = parse(type, run(format, params, columnar, false));
        Map<String, Object> actual = parse(type, run(format, params, columnar, true));
        for (String timing : List.of(
            "took",
            "cpu_nanos",
            "driver_nanos",
            "read_nanos",
            "read_cpu_nanos",
            "start_time_in_millis",
            "completion_time_in_millis",
            "expiration_time_in_millis"
        )) {
            expected.remove(timing);
            actual.remove(timing);
        }
        assertThat(actual, equalTo(expected));
    }

    private void assertSameBytes(String format, String params) throws IOException {
        byte[] expected = run(format, params, false, false);
        byte[] actual = run(format, params, false, true);
        assertThat(new String(actual, StandardCharsets.ISO_8859_1), equalTo(new String(expected, StandardCharsets.ISO_8859_1)));
    }

    private static Map<String, Object> parse(XContentType type, byte[] body) {
        return XContentHelper.convertToMap(type.xContent(), body, 0, body.length, false);
    }

    /**
     * @param spill whether the result budget pragma sends the result to a run; without it the result stays in memory
     */
    private byte[] run(String format, String params, boolean columnar, boolean spill) throws IOException {
        Request request = new Request("POST", "/_query?format=" + format + params);
        String pragma = spill ? ", \"pragma\": {\"spill_result_budget\": \"1kb\"}, \"accept_pragma_risks\": true" : "";
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query": "%s", "columnar": %s%s}""", QUERY, columnar, pragma));
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        return EntityUtils.toByteArray(response.getEntity());
    }

    /**
     * The node's store holds nothing once the responses are out; the run goes with the response, after the last part.
     */
    private void assertStoreEmpty() throws Exception {
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_esql/stats"));
            byte[] body = EntityUtils.toByteArray(response.getEntity());
            Map<String, Object> stats = XContentHelper.convertToMap(XContentType.JSON.xContent(), body, 0, body.length, false);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> nodes = (List<Map<String, Object>>) stats.get("stats");
            for (Map<String, Object> node : nodes) {
                @SuppressWarnings("unchecked")
                Map<String, Object> counters = (Map<String, Object>) node.get("stats");
                @SuppressWarnings("unchecked")
                Map<String, Object> spill = (Map<String, Object>) counters.get("spill");
                assertThat("the store holds nothing: " + spill, ((Number) spill.get("bytes_in_use")).longValue(), equalTo(0L));
                assertThat("no scope is open: " + spill, ((Number) spill.get("scopes")).longValue(), equalTo(0L));
            }
        });
    }
}
