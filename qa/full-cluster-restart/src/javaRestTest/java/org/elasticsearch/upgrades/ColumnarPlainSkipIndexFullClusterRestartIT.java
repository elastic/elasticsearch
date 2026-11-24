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

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ColumnarPlainSkipIndexFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final String DATA_STREAM = "fcr-columnar-plain-skip-index";
    private static final int DOCS_PER_BULK = 500;
    private static final int OLD_CLUSTER_BULKS = 3;
    private static final int NEW_INDEX_BULKS = 3;
    private static final Instant BASE_TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .build();

    public ColumnarPlainSkipIndexFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testPlainSkipIndexDataStream() throws Exception {
        if (isRunningAgainstOldCluster()) {
            assumeTrue(
                "the old cluster does not support columnar index modes",
                clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false)
            );
            putTemplate();
            assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM)));
            for (int bulk = 0; bulk < OLD_CLUSTER_BULKS; bulk++) {
                bulkIndex(bulk * DOCS_PER_BULK, DOCS_PER_BULK);
            }
            ensureGreen(DATA_STREAM);
            assertThat(backingIndices(), hasSize(1));
            verify(DATA_STREAM, 0, OLD_CLUSTER_BULKS * DOCS_PER_BULK);
        } else {
            assumeTrue("the old cluster did not support columnar index modes", dataStreamExists());
            int docs = OLD_CLUSTER_BULKS * DOCS_PER_BULK;

            String oldBackingIndex = backingIndices().getFirst();
            ensureGreen(DATA_STREAM);
            verify(oldBackingIndex, 0, docs);
            assertThat(bulkIndex(docs, DOCS_PER_BULK), equalTo(oldBackingIndex));
            docs += DOCS_PER_BULK;
            verify(oldBackingIndex, 0, docs);
            forceMerge(oldBackingIndex);
            verify(oldBackingIndex, 0, docs);

            assertOK(client().performRequest(new Request("POST", "/" + DATA_STREAM + "/_rollover")));
            List<String> backingIndices = backingIndices();
            assertThat(backingIndices, hasSize(2));
            String newBackingIndex = backingIndices.getLast();
            int firstNewDoc = docs;
            for (int bulk = 0; bulk < NEW_INDEX_BULKS; bulk++) {
                assertThat(bulkIndex(docs, DOCS_PER_BULK), equalTo(newBackingIndex));
                docs += DOCS_PER_BULK;
            }
            ensureGreen(DATA_STREAM);
            verify(newBackingIndex, firstNewDoc, docs);
            verify(DATA_STREAM, 0, docs);

            forceMerge(DATA_STREAM);
            verify(oldBackingIndex, 0, firstNewDoc);
            verify(newBackingIndex, firstNewDoc, docs);
            verify(DATA_STREAM, 0, docs);
            assertWrittenByColumnarCodec(newBackingIndex);
        }
    }

    private static void putTemplate() throws IOException {
        Request request = new Request("PUT", "/_index_template/" + DATA_STREAM);
        request.setJsonEntity(Strings.format("""
            {
              "index_patterns": [ "%s" ],
              "data_stream": {},
              "priority": 500,
              "template": {
                "settings": {
                  "index.mode": "columnar",
                  "index.compound_format": false,
                  "index.number_of_shards": 1,
                  "index.number_of_replicas": 0,
                  "index.sort.field": [ "trace_id" ],
                  "index.sort.order": [ "desc" ]
                },
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "trace_id": { "type": "keyword", "index": false }
                  }
                }
              }
            }""", DATA_STREAM));
        assertOK(client().performRequest(request));
    }

    private static String bulkIndex(int firstDoc, int count) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int doc = firstDoc; doc < firstDoc + count; doc++) {
            bulk.append("{\"create\":{}}\n").append(doc(doc)).append('\n');
        }
        Request request = new Request("POST", "/" + DATA_STREAM + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> body = entityAsMap(response);
        assertThat("bulk response: " + body, ObjectPath.evaluate(body, "errors"), is(false));
        String index = ObjectPath.evaluate(body, "items.0.create._index");
        for (int item = 1; item < count; item++) {
            assertThat(ObjectPath.evaluate(body, "items." + item + ".create._index"), equalTo(index));
        }
        return index;
    }

    private static String doc(int doc) {
        return Strings.format("{\"@timestamp\":\"%s\",\"trace_id\":\"%s\"}", BASE_TIMESTAMP.plusSeconds(doc), traceId(doc * 3L));
    }

    private static String traceId(long number) {
        return Strings.format("trace-%08d", number);
    }

    private static void verify(String target, int firstDoc, int lastDoc) throws IOException {
        int docs = lastDoc - firstDoc;
        Map<String, Object> count = entityAsMap(client().performRequest(new Request("GET", "/" + target + "/_count")));
        assertThat(ObjectPath.evaluate(count, "_shards.failed"), equalTo(0));
        assertThat(ObjectPath.evaluate(count, "count"), equalTo(docs));

        int from = firstDoc + docs / 4;
        int to = from + docs / 10;
        assertTotal(target, Strings.format("""
            { "range": { "trace_id": { "gte": "%s", "lt": "%s" } } }""", traceId(from * 3L), traceId(to * 3L)), to - from);
        assertTotal(target, Strings.format("""
            { "range": { "trace_id": { "gt": "%s", "lte": "%s" } } }""", traceId(from * 3L), traceId(to * 3L)), to - from);
        assertTotal(target, Strings.format("""
            { "range": { "trace_id": { "gte": "%s" } } }""", traceId(lastDoc * 3L)), 0);
        assertTotal(target, Strings.format("""
            { "range": { "trace_id": { "lt": "%s" } } }""", traceId(firstDoc * 3L)), 0);
        assertTotal(target, Strings.format("""
            { "range": { "trace_id": { "gte": "%s", "lte": "%s" } } }""", traceId(firstDoc * 3L), traceId((lastDoc - 1) * 3L)), docs);

        int middle = firstDoc + docs / 2;
        assertTotal(target, Strings.format("{ \"term\": { \"trace_id\": \"%s\" } }", traceId(middle * 3L)), 1);
        assertTotal(target, Strings.format("{ \"term\": { \"trace_id\": \"%s\" } }", traceId(middle * 3L + 1)), 0);

        String prefix = traceId(middle * 3L).substring(0, traceId(0).length() - 2);
        int withPrefix = 0;
        for (int doc = firstDoc; doc < lastDoc; doc++) {
            if (traceId(doc * 3L).startsWith(prefix)) {
                withPrefix++;
            }
        }
        assertTotal(target, Strings.format("{ \"prefix\": { \"trace_id\": \"%s\" } }", prefix), withPrefix);

        Map<String, Object> top = search(target, """
            { "size": 3, "sort": [ { "trace_id": "desc" } ] }""");
        for (int hit = 0; hit < 3; hit++) {
            String expected = traceId((lastDoc - 1 - hit) * 3L);
            assertThat(ObjectPath.evaluate(top, "hits.hits." + hit + ".sort.0"), equalTo(expected));
            assertThat(ObjectPath.evaluate(top, "hits.hits." + hit + "._source.trace_id"), equalTo(expected));
        }
    }

    private static void assertTotal(String target, String query, int expected) throws IOException {
        Map<String, Object> response = search(target, "{ \"size\": 0, \"track_total_hits\": true, \"query\": " + query + " }");
        assertThat(query, ObjectPath.evaluate(response, "hits.total.value"), equalTo(expected));
    }

    private static Map<String, Object> search(String target, String body) throws IOException {
        Request request = new Request("POST", "/" + target + "/_search");
        request.setJsonEntity(body);
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(response, "_shards.failed"), equalTo(0));
        return response;
    }

    private static void forceMerge(String target) throws IOException {
        Request request = new Request("POST", "/" + target + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(request));
    }

    private static void assertWrittenByColumnarCodec(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_stats/segments");
        request.addParameter("include_segment_file_sizes", "true");
        Map<String, Object> stats = entityAsMap(client().performRequest(request));
        Map<String, Object> fileSizes = ObjectPath.evaluate(stats, "_all.primaries.segments.file_sizes");
        assertThat("segment files of [" + index + "]", fileSizes.keySet(), hasItems("cnd", "cnm", "cns"));
    }

    private static boolean dataStreamExists() throws IOException {
        try {
            return client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM)).getStatusLine().getStatusCode() == 200;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private static List<String> backingIndices() throws IOException {
        Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM)));
        List<Object> indices = ObjectPath.evaluate(response, "data_streams.0.indices");
        List<String> names = new ArrayList<>();
        for (Object index : indices) {
            names.add(ObjectPath.evaluate(index, "index_name"));
        }
        return names;
    }
}
