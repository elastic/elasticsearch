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
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ColumnarIndexModeRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String DATA_STREAM = "rolling-columnar";
    private static final String CONTROL_INDEX = "rolling-columnar-control";
    private static final String INDEX_SETTINGS = """
        {
          "index.mode": "columnar",
          "index.compound_format": false,
          "index.number_of_shards": 1,
          "index.number_of_replicas": 0
        }""";
    private static final String INDEX_MAPPINGS = """
        {
          "properties": {
            "@timestamp": { "type": "date" },
            "host.name": { "type": "keyword" },
            "method": { "type": "keyword" },
            "tags": { "type": "keyword" },
            "message": { "type": "text" },
            "body": { "type": "match_only_text" },
            "length": { "type": "long" }
          }
        }""";
    private static final int DOCS_PER_BULK = 500;
    private static final int OLD_CLUSTER_BULKS = 3;
    private static final int NEW_INDEX_BULKS = 2;
    private static final int HOSTS = 10;
    private static final int METHODS = 5;
    private static final Instant BASE_TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static boolean oldBackingIndexHasColumnarFiles;

    private final int upgradedNodes;

    public ColumnarIndexModeRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
    }

    public void testColumnarDataStream() throws Exception {
        if (isOldCluster()) {
            assumeTrue(
                "the old cluster does not support columnar index modes",
                clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false)
            );
            createDataStream();
            return;
        }
        assumeTrue("the old cluster did not support columnar index modes", dataStreamExists());

        int docs = (OLD_CLUSTER_BULKS + upgradedNodes - 1) * DOCS_PER_BULK;
        ensureGreen(DATA_STREAM);
        verify(DATA_STREAM, 0, docs);
        if (isFirstMixedCluster()) {
            rollover();
        }
        assertThat(bulkIndex(DATA_STREAM, docs, DOCS_PER_BULK), equalTo(getDataStreamBackingIndexNames(DATA_STREAM).getLast()));
        docs += DOCS_PER_BULK;
        verify(DATA_STREAM, 0, docs);
        if (isUpgradedCluster()) {
            mergeRolloverAndVerify(docs);
        }
    }

    private void createDataStream() throws IOException {
        putTemplate();
        assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM)));
        for (int bulk = 0; bulk < OLD_CLUSTER_BULKS; bulk++) {
            bulkIndex(DATA_STREAM, bulk * DOCS_PER_BULK, DOCS_PER_BULK);
        }
        ensureGreen(DATA_STREAM);
        verify(DATA_STREAM, 0, OLD_CLUSTER_BULKS * DOCS_PER_BULK);
        // Read off a merged control index rather than the backing index, whose unmerged segments are compound files that hide the
        // codec's files, and which the upgraded cluster merges later.
        oldBackingIndexHasColumnarFiles = controlIndexHasColumnarFiles();
    }

    private void mergeRolloverAndVerify(int docs) throws IOException {
        final List<String> existingIndices = getDataStreamBackingIndexNames(DATA_STREAM);
        assertThat(existingIndices, hasSize(2));
        final String oldBackingIndex = existingIndices.getFirst();
        final String mixedBackingIndex = existingIndices.getLast();
        final int firstMixedDoc = OLD_CLUSTER_BULKS * DOCS_PER_BULK;
        assertEsqlText(DATA_STREAM, 0);
        forceMerge(oldBackingIndex);
        forceMerge(mixedBackingIndex);
        verify(oldBackingIndex, 0, firstMixedDoc);
        verify(mixedBackingIndex, firstMixedDoc, docs);
        assertColumnarFiles(oldBackingIndex, oldBackingIndexHasColumnarFiles);
        assertColumnarFiles(mixedBackingIndex, oldBackingIndexHasColumnarFiles);

        rollover();
        final List<String> backingIndices = getDataStreamBackingIndexNames(DATA_STREAM);
        assertThat(backingIndices, hasSize(3));
        final String newBackingIndex = backingIndices.getLast();
        assertThat(getIndexSettingsAsMap(newBackingIndex).get("index.mode"), equalTo("columnar"));
        for (int bulk = 0; bulk < NEW_INDEX_BULKS; bulk++) {
            assertThat(bulkIndex(DATA_STREAM, docs + bulk * DOCS_PER_BULK, DOCS_PER_BULK), equalTo(newBackingIndex));
        }
        final int totalDocs = docs + NEW_INDEX_BULKS * DOCS_PER_BULK;
        ensureGreen(DATA_STREAM);
        verify(newBackingIndex, docs, totalDocs);

        forceMerge(newBackingIndex);
        assertTrue("ColumNAR files in a control index created by the upgraded cluster", controlIndexHasColumnarFiles());
        assertColumnarFiles(newBackingIndex, true);
        verify(oldBackingIndex, 0, firstMixedDoc);
        verify(mixedBackingIndex, firstMixedDoc, docs);
        verify(newBackingIndex, docs, totalDocs);
        verify(DATA_STREAM, 0, totalDocs);
        assertEsqlText(oldBackingIndex, 0);
        assertEsqlText(mixedBackingIndex, firstMixedDoc);
        assertEsqlText(newBackingIndex, docs);
        assertEsqlText(DATA_STREAM, 0);
        addReplicasAndVerify(backingIndices, totalDocs);
    }

    private void addReplicasAndVerify(List<String> indices, int docs) throws IOException {
        for (String index : indices) {
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 1));
        }
        ensureGreen(DATA_STREAM);
        verify(DATA_STREAM, 0, docs);
    }

    private static void rollover() throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + DATA_STREAM + "/_rollover")));
    }

    private static void assertColumnarFiles(String index, boolean expected) throws IOException {
        assertThat("ColumNAR files in [" + index + "]", hasColumnarFiles(index), equalTo(expected));
    }

    private static void putTemplate() throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + DATA_STREAM);
        request.setJsonEntity(Strings.format("""
            {
              "index_patterns": [ "%s" ],
              "data_stream": {},
              "priority": 500,
              "template": { "settings": %s, "mappings": %s }
            }""", DATA_STREAM, INDEX_SETTINGS, INDEX_MAPPINGS));
        assertOK(client().performRequest(request));
    }

    private static boolean controlIndexHasColumnarFiles() throws IOException {
        final Request request = new Request("PUT", "/" + CONTROL_INDEX);
        request.setJsonEntity(Strings.format("""
            { "settings": %s, "mappings": %s }""", INDEX_SETTINGS, INDEX_MAPPINGS));
        assertOK(client().performRequest(request));
        try {
            // A flushed segment is always a compound file, which hides the codec's files, so the index is merged like the others.
            bulkIndex(CONTROL_INDEX, 0, DOCS_PER_BULK);
            bulkIndex(CONTROL_INDEX, DOCS_PER_BULK, DOCS_PER_BULK);
            forceMerge(CONTROL_INDEX);
            return hasColumnarFiles(CONTROL_INDEX);
        } finally {
            deleteIndex(CONTROL_INDEX);
        }
    }

    private static String bulkIndex(String target, int firstDoc, int count) throws IOException {
        final StringBuilder bulk = new StringBuilder();
        for (int doc = firstDoc; doc < firstDoc + count; doc++) {
            bulk.append("{\"create\":{}}\n").append(doc(doc)).append('\n');
        }
        final Request request = new Request("POST", "/" + target + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> body = entityAsMap(response);
        assertThat("bulk response: " + body, ObjectPath.evaluate(body, "errors"), is(false));
        final String index = ObjectPath.evaluate(body, "items.0.create._index");
        for (int item = 1; item < count; item++) {
            assertThat(ObjectPath.evaluate(body, "items." + item + ".create._index"), equalTo(index));
        }
        return index;
    }

    private static String doc(int doc) {
        return Strings.format(
            """
                {"@timestamp":"%s","host.name":"host%d","method":"m%d","tags":["tag-a%d","tag-b%d"],\
                "message":"line%d alpha beta","body":"body%d gamma delta","length":%d}""",
            BASE_TIMESTAMP.plusSeconds(doc),
            doc % HOSTS,
            doc % METHODS,
            doc % 3,
            doc % 7,
            doc,
            doc,
            doc * 1000L
        );
    }

    private static void forceMerge(String index) throws IOException {
        assertThat("segments of [" + index + "] before the merge", segmentCount(index), greaterThan(1));
        final Request request = new Request("POST", "/" + index + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat("force merge response: " + response, ObjectPath.evaluate(response, "_shards.failed"), equalTo(0));
        assertThat("force merge response: " + response, ObjectPath.evaluate(response, "_shards.successful"), equalTo(1));
        assertThat("segments of [" + index + "] after the merge", segmentCount(index), equalTo(1));
    }

    private static int segmentCount(String index) throws IOException {
        return ObjectPath.evaluate(segmentStats(index), "_all.primaries.segments.count");
    }

    private static boolean hasColumnarFiles(String index) throws IOException {
        final Map<String, Object> fileSizes = ObjectPath.evaluate(segmentStats(index), "_all.primaries.segments.file_sizes");
        return fileSizes.containsKey("cnd");
    }

    private static Map<String, Object> segmentStats(String index) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_stats/segments");
        request.addParameter("include_segment_file_sizes", "true");
        return entityAsMap(client().performRequest(request));
    }

    private static void verify(String target, int firstDoc, int lastDoc) throws IOException {
        assertDocCount(client(), target, lastDoc - firstDoc);
        assertTermsAggregation(target, firstDoc, lastDoc);
        assertSortedSearch(target, firstDoc, lastDoc);
        assertTextSearch(target, firstDoc, lastDoc);
        assertEsqlAggregation(target, firstDoc, lastDoc);
    }

    private static void assertEsqlAggregation(String target, int firstDoc, int lastDoc) throws IOException {
        final Map<String, Object> count = esql("FROM " + target + " | STATS c = count(*) | LIMIT 1");
        assertThat(ObjectPath.evaluate(count, "values.0.0"), equalTo(lastDoc - firstDoc));

        final Map<String, Object> byHost = esql("FROM " + target + " | STATS c = count(*) BY host.name | SORT host.name | LIMIT 20");
        final List<Object> rows = ObjectPath.evaluate(byHost, "values");
        assertThat(rows, hasSize(HOSTS));
        for (int host = 0; host < HOSTS; host++) {
            assertThat(ObjectPath.evaluate(rows.get(host), "0"), equalTo(countWithRemainder(firstDoc, lastDoc, HOSTS, host)));
            assertThat(ObjectPath.evaluate(rows.get(host), "1"), equalTo("host" + host));
        }
    }

    private static void assertEsqlText(String target, int firstDoc) throws IOException {
        final int doc = firstWithRemainder(firstDoc, HOSTS, 3);
        final Map<String, Object> text = esql(
            "FROM " + target + " | WHERE host.name == \"host3\" | SORT @timestamp | KEEP message, body, tags | LIMIT 1"
        );
        assertThat(ObjectPath.evaluate(text, "values.0.0"), equalTo("line" + doc + " alpha beta"));
        assertThat(ObjectPath.evaluate(text, "values.0.1"), equalTo("body" + doc + " gamma delta"));
        assertThat(asStrings(ObjectPath.evaluate(text, "values.0.2")), containsInAnyOrder("tag-a" + doc % 3, "tag-b" + doc % 7));
    }

    private static void assertTermsAggregation(String target, int firstDoc, int lastDoc) throws IOException {
        final Map<String, Object> response = search(target, """
            {
              "size": 0,
              "aggs": {
                "hosts": {
                  "terms": { "field": "host.name", "size": 20, "order": { "_key": "asc" } },
                  "aggs": { "max_length": { "max": { "field": "length" } } }
                }
              }
            }""");
        final List<Object> buckets = ObjectPath.evaluate(response, "aggregations.hosts.buckets");
        assertThat(buckets, hasSize(HOSTS));
        for (int host = 0; host < HOSTS; host++) {
            assertThat(ObjectPath.evaluate(buckets.get(host), "key"), equalTo("host" + host));
            assertThat(ObjectPath.evaluate(buckets.get(host), "doc_count"), equalTo(countWithRemainder(firstDoc, lastDoc, HOSTS, host)));
            assertThat(
                ObjectPath.evaluate(buckets.get(host), "max_length.value"),
                equalTo((double) lastWithRemainder(lastDoc, HOSTS, host) * 1000)
            );
        }
    }

    private static void assertSortedSearch(String target, int firstDoc, int lastDoc) throws IOException {
        final Map<String, Object> response = search(target, """
            {
              "size": 5,
              "sort": [ { "method": "asc" }, { "@timestamp": "asc" } ]
            }""");
        assertThat(ObjectPath.evaluate(response, "hits.total.value"), equalTo(lastDoc - firstDoc));
        final List<Object> hits = ObjectPath.evaluate(response, "hits.hits");
        assertThat(hits, hasSize(5));
        int doc = firstWithRemainder(firstDoc, METHODS, 0);
        for (Object hit : hits) {
            assertThat(ObjectPath.evaluate(hit, "sort.0"), equalTo("m0"));
            assertSource(ObjectPath.evaluate(hit, "_source"), doc);
            doc += METHODS;
        }
    }

    private static void assertTextSearch(String target, int firstDoc, int lastDoc) throws IOException {
        final int doc = (firstDoc + lastDoc) / 2;
        assertSingleHit(target, Strings.format("{ \"match\": { \"message\": \"line%d\" } }", doc), doc);
        assertSingleHit(target, Strings.format("{ \"match_phrase\": { \"body\": \"body%d gamma\" } }", doc), doc);
    }

    private static void assertSingleHit(String target, String query, int doc) throws IOException {
        final Map<String, Object> response = search(target, "{ \"query\": " + query + " }");
        assertThat(ObjectPath.evaluate(response, "hits.total.value"), equalTo(1));
        assertSource(ObjectPath.evaluate(response, "hits.hits.0._source"), doc);
    }

    private static void assertSource(Map<String, Object> source, int doc) throws IOException {
        assertThat(hostName(source), equalTo("host" + doc % HOSTS));
        assertThat(source.get("method"), equalTo("m" + doc % METHODS));
        assertThat(source.get("message"), equalTo("line" + doc + " alpha beta"));
        assertThat(source.get("body"), equalTo("body" + doc + " gamma delta"));
        assertThat(source.get("length"), equalTo(doc * 1000));
        assertThat(asStrings(source.get("tags")), containsInAnyOrder("tag-a" + doc % 3, "tag-b" + doc % 7));
    }

    private static String hostName(Map<String, Object> source) throws IOException {
        final Object flat = source.get("host.name");
        return flat != null ? (String) flat : ObjectPath.evaluate(source, "host.name");
    }

    private static Map<String, Object> search(String target, String body) throws IOException {
        final Request request = new Request("POST", "/" + target + "/_search");
        request.setJsonEntity(body);
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(response, "_shards.failed"), equalTo(0));
        return response;
    }

    private static Map<String, Object> esql(String query) throws IOException {
        final Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query.replace("\"", "\\\"") + "\"}");
        return entityAsMap(client().performRequest(request));
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

    private static int countWithRemainder(int from, int to, int modulus, int remainder) {
        int count = 0;
        for (int doc = from; doc < to; doc++) {
            if (doc % modulus == remainder) {
                count++;
            }
        }
        return count;
    }

    private static int firstWithRemainder(int from, int modulus, int remainder) {
        int doc = from;
        while (doc % modulus != remainder) {
            doc++;
        }
        return doc;
    }

    private static int lastWithRemainder(int to, int modulus, int remainder) {
        int doc = to - 1;
        while (doc % modulus != remainder) {
            doc--;
        }
        return doc;
    }

    private static List<String> asStrings(Object values) {
        final List<String> strings = new ArrayList<>();
        for (Object value : (List<?>) values) {
            strings.add((String) value);
        }
        return strings;
    }
}
