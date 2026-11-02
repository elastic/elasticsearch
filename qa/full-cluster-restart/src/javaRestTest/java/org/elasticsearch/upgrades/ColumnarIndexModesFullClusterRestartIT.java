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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Full cluster restart coverage for the strict columnar index modes, {@code columnar} and {@code logsdb_columnar}. The two are
 * tested separately because they differ in default index sort, default mapping and validations, so a pass for one says nothing
 * about the other.
 *
 * <p>The old cluster writes a data stream. After the restart the old backing index is read, written to and force merged, then the
 * data stream is rolled over. The new backing index is created by the upgraded cluster, so it is the one the ColumNAR codec
 * writes, while the old one keeps the format it was created with. Search, sort, aggregations and ES|QL are run over each backing
 * index and over the data stream as a whole, before and after force merges.
 */
public class ColumnarIndexModesFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    private static final int DOCS_PER_BULK = 500;
    private static final int OLD_CLUSTER_BULKS = 3;
    private static final int HOSTS = 10;
    private static final int METHODS = 5;
    private static final Instant BASE_TIMESTAMP = Instant.parse("2024-01-01T00:00:00Z");

    private static String licenseType;

    /**
     * The license of the cluster, which columnar indices react to: under basic they keep a stored source. It is picked when the cluster
     * starts, as static initializers run before there is a random seed, and kept so the upgraded cluster has the license of the old one.
     * FIPS mode does not allow a basic license, and the test cluster setup forces a trial one there, so that is what is expected.
     */
    private static synchronized String licenseType() {
        if (licenseType == null) {
            licenseType = Boolean.getBoolean("tests.fips.enabled") ? "trial" : randomFrom("basic", "trial");
        }
        return licenseType;
    }

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", ColumnarIndexModesFullClusterRestartIT::licenseType)
        .setting("xpack.ml.enabled", "false")
        .build();

    public ColumnarIndexModesFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
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

    public void testColumnar() throws Exception {
        testDataStream("fcr-columnar", "columnar");
    }

    public void testLogsdbColumnar() throws Exception {
        testDataStream("fcr-logsdb-columnar", "logsdb_columnar");
    }

    private void testDataStream(String dataStream, String indexMode) throws Exception {
        Map<String, Object> license = entityAsMap(client().performRequest(new Request("GET", "/_license")));
        assertThat(ObjectPath.evaluate(license, "license.type"), equalTo(licenseType()));
        logger.info("running under a [{}] license", licenseType());
        if (isRunningAgainstOldCluster()) {
            assumeTrue(
                "the old cluster does not support columnar index modes",
                clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false)
            );
            putTemplate(dataStream, indexMode);
            assertOK(client().performRequest(new Request("PUT", "/_data_stream/" + dataStream)));
            for (int bulk = 0; bulk < OLD_CLUSTER_BULKS; bulk++) {
                bulkIndex(dataStream, bulk * DOCS_PER_BULK, DOCS_PER_BULK);
            }
            ensureGreen(dataStream);
            List<String> backingIndices = backingIndices(dataStream);
            assertThat(backingIndices, hasSize(1));
            assertThat(indexSetting(backingIndices.getFirst(), "index.mode"), equalTo(indexMode));
            verify(dataStream, OLD_CLUSTER_BULKS * DOCS_PER_BULK);
        } else {
            assumeTrue("the old cluster did not support columnar index modes", dataStreamExists(dataStream));
            int docs = OLD_CLUSTER_BULKS * DOCS_PER_BULK;

            // the old backing index is read, written to and merged by the upgraded cluster
            String oldBackingIndex = backingIndices(dataStream).getFirst();
            ensureGreen(dataStream);
            verify(oldBackingIndex, docs);
            assertThat(bulkIndex(dataStream, docs, DOCS_PER_BULK), equalTo(oldBackingIndex));
            docs += DOCS_PER_BULK;
            verify(oldBackingIndex, docs);
            forceMerge(oldBackingIndex);
            verify(oldBackingIndex, docs);

            // the new backing index is created by the upgraded cluster
            assertOK(client().performRequest(new Request("POST", "/" + dataStream + "/_rollover")));
            List<String> backingIndices = backingIndices(dataStream);
            assertThat(backingIndices, hasSize(2));
            String newBackingIndex = backingIndices.getLast();
            assertThat(indexSetting(newBackingIndex, "index.mode"), equalTo(indexMode));
            // the files of a compound segment aren't listed in the segment stats, so the codec's own files couldn't be seen
            updateIndexSettings(newBackingIndex, Settings.builder().put("index.compound_format", false));

            assertThat(bulkIndex(dataStream, docs, DOCS_PER_BULK), equalTo(newBackingIndex));
            int newIndexDocs = DOCS_PER_BULK;
            docs += DOCS_PER_BULK;
            ensureGreen(dataStream);
            verify(newBackingIndex, newIndexDocs, docs - newIndexDocs);
            verify(dataStream, docs);

            // merge both backing indices, which still hold segments of different formats between them
            forceMerge(dataStream);
            verify(oldBackingIndex, docs - newIndexDocs);
            verify(newBackingIndex, newIndexDocs, docs - newIndexDocs);
            verify(dataStream, docs);
            assertWrittenByColumnarCodec(newBackingIndex);
        }
    }

    private static void putTemplate(String dataStream, String indexMode) throws IOException {
        Request request = new Request("PUT", "/_index_template/" + dataStream);
        request.setJsonEntity(Strings.format("""
            {
              "index_patterns": [ "%s" ],
              "data_stream": {},
              "priority": 500,
              "template": {
                "settings": {
                  "index.mode": "%s",
                  "index.number_of_shards": 1,
                  "index.number_of_replicas": 0
                },
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "host.name": { "type": "keyword" },
                    "method": { "type": "keyword" },
                    "tags": { "type": "keyword" },
                    "message": { "type": "text" },
                    "body": { "type": "match_only_text" },
                    "length": { "type": "long" }
                  }
                }
              }
            }""", dataStream, indexMode));
        assertOK(client().performRequest(request));
    }

    /** Indexes documents {@code firstDoc} up to {@code firstDoc + count} and returns the index they were all written to. */
    private static String bulkIndex(String dataStream, int firstDoc, int count) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = firstDoc; i < firstDoc + count; i++) {
            bulk.append("{\"create\":{}}\n").append(doc(i)).append('\n');
        }
        Request request = new Request("POST", "/" + dataStream + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> body = entityAsMap(response);
        assertThat("bulk response: " + body, ObjectPath.evaluate(body, "errors"), is(false));
        String index = ObjectPath.evaluate(body, "items.0.create._index");
        for (int i = 1; i < count; i++) {
            assertThat(ObjectPath.evaluate(body, "items." + i + ".create._index"), equalTo(index));
        }
        return index;
    }

    /** Document {@code i}, derived from {@code i} alone so that every expected count and value can be computed. */
    private static String doc(int i) {
        return Strings.format(
            """
                {"@timestamp":"%s","host.name":"host%d","method":"m%d","tags":["tag-a%d","tag-b%d"],\
                "message":"line%d alpha beta","body":"body%d gamma delta","length":%d}""",
            BASE_TIMESTAMP.plusSeconds(i),
            i % HOSTS,
            i % METHODS,
            i % 3,
            i % 7,
            i,
            i,
            i * 1000L
        );
    }

    private static void forceMerge(String target) throws IOException {
        Request request = new Request("POST", "/" + target + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(request));
    }

    /**
     * Verifies a target holding documents {@code 0} to {@code expectedDocCount - 1}.
     */
    private static void verify(String target, int expectedDocCount) throws IOException {
        verify(target, expectedDocCount, 0);
    }

    /**
     * Verifies a target holding documents {@code firstDoc} to {@code firstDoc + expectedDocCount - 1}.
     */
    private static void verify(String target, int expectedDocCount, int firstDoc) throws IOException {
        int lastDoc = firstDoc + expectedDocCount;
        assertCount(target, expectedDocCount);
        assertTermsAggregation(target, firstDoc, lastDoc);
        assertSortedSearch(target, firstDoc, lastDoc);
        assertTextSearch(target, firstDoc, lastDoc);
        assertEsql(target, firstDoc, lastDoc);
    }

    private static void assertCount(String target, int expectedDocCount) throws IOException {
        Map<String, Object> count = entityAsMap(client().performRequest(new Request("GET", "/" + target + "/_count")));
        assertThat(ObjectPath.evaluate(count, "_shards.failed"), equalTo(0));
        assertThat(ObjectPath.evaluate(count, "count"), equalTo(expectedDocCount));
        Map<String, Object> esql = esql("FROM " + target + " | STATS c = count(*) | LIMIT 1");
        assertThat(ObjectPath.evaluate(esql, "values.0.0"), equalTo(expectedDocCount));
    }

    private static void assertTermsAggregation(String target, int firstDoc, int lastDoc) throws IOException {
        Map<String, Object> response = search(target, """
            {
              "size": 0,
              "aggs": {
                "hosts": {
                  "terms": { "field": "host.name", "size": 20, "order": { "_key": "asc" } },
                  "aggs": { "max_length": { "max": { "field": "length" } } }
                }
              }
            }""");
        List<Object> buckets = ObjectPath.evaluate(response, "aggregations.hosts.buckets");
        assertThat(buckets, hasSize(HOSTS));
        for (int host = 0; host < HOSTS; host++) {
            assertThat(ObjectPath.evaluate(buckets.get(host), "key"), equalTo("host" + host));
            assertThat(ObjectPath.evaluate(buckets.get(host), "doc_count"), equalTo(countWhere(firstDoc, lastDoc, HOSTS, host)));
            int lastWithHost = lastDocWhere(lastDoc, HOSTS, host);
            assertThat(ObjectPath.evaluate(buckets.get(host), "max_length.value"), equalTo((double) lastWithHost * 1000));
        }
    }

    /** Sorts on doc values and reads the first documents back, so the order proves the values are read correctly. */
    private static void assertSortedSearch(String target, int firstDoc, int lastDoc) throws IOException {
        Map<String, Object> response = search(target, """
            {
              "size": 5,
              "sort": [ { "method": "asc" }, { "@timestamp": "asc" } ]
            }""");
        assertThat(ObjectPath.evaluate(response, "hits.total.value"), equalTo(lastDoc - firstDoc));
        List<Object> hits = ObjectPath.evaluate(response, "hits.hits");
        assertThat(hits, hasSize(5));
        // the first documents with method m0, in timestamp order
        int doc = firstDoc + Math.floorMod(-firstDoc, METHODS);
        for (Object hit : hits) {
            assertThat(ObjectPath.evaluate(hit, "sort.0"), equalTo("m0"));
            assertThat(ObjectPath.evaluate(hit, "_source.message"), equalTo("line" + doc + " alpha beta"));
            assertThat(ObjectPath.evaluate(hit, "_source.body"), equalTo("body" + doc + " gamma delta"));
            assertThat(hostName(ObjectPath.evaluate(hit, "_source")), equalTo("host" + doc % HOSTS));
            doc += METHODS;
        }
    }

    /** Looks up one document by its text and match_only_text values and checks the {@code _source} rebuilt from doc values. */
    private static void assertTextSearch(String target, int firstDoc, int lastDoc) throws IOException {
        int doc = (firstDoc + lastDoc) / 2;
        Map<String, Object> byMessage = search(target, Strings.format("""
            { "query": { "match": { "message": "line%d" } } }""", doc));
        assertThat(ObjectPath.evaluate(byMessage, "hits.total.value"), equalTo(1));
        assertSource(ObjectPath.evaluate(byMessage, "hits.hits.0._source"), doc);

        Map<String, Object> byBody = search(target, Strings.format("""
            { "query": { "match_phrase": { "body": "body%d gamma" } } }""", doc));
        assertThat(ObjectPath.evaluate(byBody, "hits.total.value"), equalTo(1));
        assertSource(ObjectPath.evaluate(byBody, "hits.hits.0._source"), doc);
    }

    private static void assertSource(Map<String, Object> source, int doc) throws IOException {
        assertThat(hostName(source), equalTo("host" + doc % HOSTS));
        assertThat(source.get("method"), equalTo("m" + doc % METHODS));
        assertThat(source.get("message"), equalTo("line" + doc + " alpha beta"));
        assertThat(source.get("body"), equalTo("body" + doc + " gamma delta"));
        assertThat(source.get("length"), equalTo(doc * 1000));
        // keyword values may come back in a different order than they were indexed in
        assertThat(asStrings(source.get("tags")), containsInAnyOrder("tag-a" + doc % 3, "tag-b" + doc % 7));
    }

    /** The {@code host.name} of a {@code _source}, whether it was rebuilt as a flat dotted key or as a nested object. */
    private static String hostName(Map<String, Object> source) throws IOException {
        Object flat = source.get("host.name");
        return flat != null ? (String) flat : ObjectPath.evaluate(source, "host.name");
    }

    private static void assertEsql(String target, int firstDoc, int lastDoc) throws IOException {
        Map<String, Object> byHost = esql("FROM " + target + " | STATS c = count(*) BY host.name | SORT host.name | LIMIT 20");
        List<Object> rows = ObjectPath.evaluate(byHost, "values");
        assertThat(rows, hasSize(HOSTS));
        for (int host = 0; host < HOSTS; host++) {
            assertThat(ObjectPath.evaluate(rows.get(host), "0"), equalTo(countWhere(firstDoc, lastDoc, HOSTS, host)));
            assertThat(ObjectPath.evaluate(rows.get(host), "1"), equalTo("host" + host));
        }

        int doc = firstDoc + Math.floorMod(3 - firstDoc, HOSTS);
        Map<String, Object> text = esql(
            "FROM " + target + " | WHERE host.name == \"host3\" | SORT @timestamp | KEEP message, body, tags | LIMIT 1"
        );
        assertThat(ObjectPath.evaluate(text, "values.0.0"), equalTo("line" + doc + " alpha beta"));
        assertThat(ObjectPath.evaluate(text, "values.0.1"), equalTo("body" + doc + " gamma delta"));
        assertThat(asStrings(ObjectPath.evaluate(text, "values.0.2")), containsInAnyOrder("tag-a" + doc % 3, "tag-b" + doc % 7));
    }

    /**
     * Asserts the ColumNAR codec wrote doc values for {@code index}, read off the extensions of its segment files rather than off
     * the index mode, which a strict columnar index has whichever codec writes it. The files of a compound segment are not listed,
     * so the index has to be written with {@code index.compound_format} off.
     */
    private static void assertWrittenByColumnarCodec(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_stats/segments");
        request.addParameter("include_segment_file_sizes", "true");
        Map<String, Object> stats = entityAsMap(client().performRequest(request));
        // the request names one index, so the totals are its own, and its name, which holds dots, can't break the path
        Map<String, Object> fileSizes = ObjectPath.evaluate(stats, "_all.primaries.segments.file_sizes");
        assertThat("segment files of [" + index + "]", fileSizes.keySet(), hasItems("cnd", "cnm"));
    }

    private static Map<String, Object> search(String target, String body) throws IOException {
        Request request = new Request("POST", "/" + target + "/_search");
        request.setJsonEntity(body);
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(ObjectPath.evaluate(response, "_shards.failed"), equalTo(0));
        return response;
    }

    private static Map<String, Object> esql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\": \"" + query.replace("\"", "\\\"") + "\"}");
        return entityAsMap(client().performRequest(request));
    }

    private static boolean dataStreamExists(String dataStream) throws IOException {
        try {
            return client().performRequest(new Request("GET", "/_data_stream/" + dataStream)).getStatusLine().getStatusCode() == 200;
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private static List<String> backingIndices(String dataStream) throws IOException {
        Map<String, Object> response = entityAsMap(client().performRequest(new Request("GET", "/_data_stream/" + dataStream)));
        List<Object> indices = ObjectPath.evaluate(response, "data_streams.0.indices");
        List<String> names = new ArrayList<>();
        for (Object index : indices) {
            names.add(ObjectPath.evaluate(index, "index_name"));
        }
        return names;
    }

    private static String indexSetting(String index, String setting) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        // backing index names and flat setting names both hold dots
        return ObjectPath.evaluate(response, index.replace(".", "\\.") + ".settings." + setting.replace(".", "\\."));
    }

    /** Number of documents in {@code [from, to)} whose index modulo {@code modulus} is {@code remainder}. */
    private static int countWhere(int from, int to, int modulus, int remainder) {
        // the matches up to x number floorDiv(x - remainder, modulus) + 1, so the matches in [from, to) are the difference at to - 1
        // and from - 1
        return Math.floorDiv(to - 1 - remainder, modulus) - Math.floorDiv(from - 1 - remainder, modulus);
    }

    /** The last document in {@code [from, to)} whose index modulo {@code modulus} is {@code remainder}, which the range must hold. */
    private static int lastDocWhere(int to, int modulus, int remainder) {
        return to - 1 - Math.floorMod(to - 1 - remainder, modulus);
    }

    private static List<String> asStrings(Object values) {
        List<String> strings = new ArrayList<>();
        for (Object value : (List<?>) values) {
            strings.add((String) value);
        }
        return strings;
    }
}
