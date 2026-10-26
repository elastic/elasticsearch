/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperFeatures;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * BWC test for arrays whose encoded {@code .offsets} exceed the sorted doc values limit ({@link IndexWriter#MAX_TERM_LENGTH}).
 * Old versions without the fallback reject such a document with Lucene's error; versions with it store the array in
 * {@code _ignored_source}, so the document is accepted and its synthetic source reproduces the array exactly.
 * <p>
 * The test verifies that regular array documents written on the old cluster stay readable throughout the upgrade, that an
 * oversized array is rejected or accepted according to the version of the node holding the primary, and that after the upgrade
 * oversized arrays are accepted and round-trip through GET, search and a force merge.
 */
public class OversizedArrayOffsetsRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    private static final String INDEX_NAME = "bwc-oversized-array-offsets";
    private static final String MAPPING = """
        {
          "properties": {
            "@timestamp": { "type": "date" },
            "kw": { "type": "keyword" },
            "n": { "type": "long" }
          }
        }""";
    /** Lucene's error for an oversized sorted doc value, produced by versions without the fallback. */
    private static final String OLD_REJECTION = "is too large, must be <= " + IndexWriter.MAX_TERM_LENGTH;

    public void testOversizedArraysAcrossUpgrade() throws Exception {
        boolean oldClusterHasFallback = oldClusterHasFeature(MapperFeatures.OVERSIZED_ARRAY_OFFSETS_IGNORED_SOURCE_FALLBACK);
        // One replica keeps the index available while nodes restart one at a time.
        createIndex(
            INDEX_NAME,
            Settings.builder().put("index.mode", "logsdb").put("index.number_of_shards", 1).put("index.number_of_replicas", 1).build(),
            MAPPING
        );
        ensureGreen(INDEX_NAME);

        // Documents whose synthetic source must match exactly, by id. Regular arrays use native offsets; they are indexed on the
        // old cluster, before the upgrade.
        Map<String, Map<String, Object>> expected = new LinkedHashMap<>();
        Instant time = Instant.now().minusSeconds(60 * 60);
        for (int i = 0; i < between(3, 6); i++) {
            Map<String, Object> document = regularDocument(time, i);
            time = time.plusSeconds(1);
            indexDocument("old-" + i, document);
            expected.put("old-" + i, document);
        }
        verify(expected);

        // On the old cluster the oversized array is either rejected with Lucene's error, or accepted and kept in _ignored_source.
        Map<String, Object> oversized = oversizedDocument(time, false);
        time = time.plusSeconds(1);
        if (oldClusterHasFallback) {
            indexDocument("old-oversized", oversized);
            expected.put("old-oversized", oversized);
            verify(expected);
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> indexDocument("old-oversized", oversized));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString(OLD_REJECTION));
        }

        var timeRef = new Instant[] { time };
        clusterRollingUpgrade(nodeIndex -> {
            // The index may legitimately be yellow here: an oversized array accepted by an upgraded primary fails on a replica
            // that still runs the old version, and that replica cannot be re-allocated to an older node than the primary.
            ensureYellowAndNoInitializingShards(INDEX_NAME, "120s");
            verify(expected);

            // Whether an oversized array is accepted in the mixed cluster depends on the version of the node holding the primary.
            String id = "mixed-oversized-" + nodeIndex;
            Map<String, Object> document = oversizedDocument(timeRef[0], randomBoolean());
            timeRef[0] = timeRef[0].plusSeconds(1);
            try {
                indexDocument(id, document);
                logger.info("oversized array accepted after upgrading node [{}]", nodeIndex);
                expected.put(id, document);
                verify(expected);
            } catch (ResponseException e) {
                logger.info("oversized array rejected after upgrading node [{}]: {}", nodeIndex, e.getMessage());
                assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                assertThat(e.getMessage(), containsString(OLD_REJECTION));
            }
        });

        ensureGreen(INDEX_NAME);
        verify(expected);

        // Fully upgraded: oversized arrays are accepted, alone and in a bulk request next to regular documents.
        Map<String, Object> oversizedN = oversizedDocument(timeRef[0], false);
        timeRef[0] = timeRef[0].plusSeconds(1);
        indexDocument("new-oversized-n", oversizedN);
        expected.put("new-oversized-n", oversizedN);
        Map<String, Object> oversizedBoth = oversizedDocument(timeRef[0], true);
        timeRef[0] = timeRef[0].plusSeconds(1);
        indexDocument("new-oversized-kw-n", oversizedBoth);
        expected.put("new-oversized-kw-n", oversizedBoth);

        Map<String, Map<String, Object>> bulk = new LinkedHashMap<>();
        for (int i = 0; i < between(2, 4); i++) {
            bulk.put("new-" + i, regularDocument(timeRef[0], i));
            timeRef[0] = timeRef[0].plusSeconds(1);
        }
        bulk.put("new-bulk-oversized", oversizedDocument(timeRef[0], randomBoolean()));
        bulkIndex(bulk);
        expected.putAll(bulk);
        verify(expected);

        var forceMergeRequest = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(forceMergeRequest));
        refresh(INDEX_NAME);
        verify(expected);
    }

    private static Map<String, Object> regularDocument(Instant time, int i) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("@timestamp", formatInstant(time));
        // Out-of-order values with duplicates and nulls, so the offsets carry real ordering information.
        document.put("kw", Arrays.asList("b", "a", null, "b", "doc-" + i));
        document.put("n", Arrays.asList(3, 1, null, 3, 2, i));
        return document;
    }

    /**
     * A document whose {@code n} array, and optionally also its {@code kw} array, has more elements than a sorted doc value can
     * hold offsets for: one more slot than {@link IndexWriter#MAX_TERM_LENGTH} guarantees that, since every slot takes at least
     * one byte on top of the length prefix.
     */
    private static Map<String, Object> oversizedDocument(Instant time, boolean oversizedKeyword) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("@timestamp", formatInstant(time));
        document.put("kw", oversizedKeyword ? oversizedValues(List.of("a", "b", "c", "d")) : Arrays.asList("b", "a", null, "b"));
        document.put("n", oversizedValues(List.of(0, 1, 2, 3)));
        return document;
    }

    private static <T> List<T> oversizedValues(List<T> pool) {
        int numSlots = IndexWriter.MAX_TERM_LENGTH + between(1, 500);
        List<T> values = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            values.add(randomInt(10) == 1 ? null : randomFrom(pool));
        }
        return values;
    }

    private static void indexDocument(String id, Map<String, Object> document) throws IOException {
        var request = new Request("PUT", "/" + INDEX_NAME + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(toJson(document));
        assertOK(client().performRequest(request));
    }

    private static void bulkIndex(Map<String, Map<String, Object>> documents) throws IOException {
        var request = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder body = new StringBuilder();
        for (var entry : documents.entrySet()) {
            body.append("{\"index\":{\"_id\":\"").append(entry.getKey()).append("\"}}\n");
            body.append(toJson(entry.getValue())).append('\n');
        }
        request.setJsonEntity(body.toString());
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
    }

    private static String toJson(Map<String, Object> document) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(document);
            return Strings.toString(builder);
        }
    }

    /**
     * Asserts that every document reads back with exactly the expected source, both via a non-realtime GET and via search, so
     * the synthesized source rather than the translog is compared.
     */
    private static void verify(Map<String, Map<String, Object>> expected) throws IOException {
        for (var entry : expected.entrySet()) {
            var getRequest = new Request("GET", "/" + INDEX_NAME + "/_doc/" + entry.getKey());
            getRequest.addParameter("realtime", "false");
            Map<String, Object> getResponse = entityAsMap(client().performRequest(getRequest));
            assertThat(entry.getKey(), getResponse.get("found"), equalTo(true));
            assertThat(entry.getKey(), getResponse.get("_source"), equalTo(entry.getValue()));
        }

        var searchRequest = new Request("GET", "/" + INDEX_NAME + "/_search");
        searchRequest.setJsonEntity(Strings.format("""
            {"query": {"ids": {"values": %s}}, "size": %d}""", toJsonArray(expected.keySet()), expected.size()));
        Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
        assertThat(ObjectPath.evaluate(searchResponse, "_shards.failed"), equalTo(0));
        assertThat(ObjectPath.evaluate(searchResponse, "hits.total.value"), equalTo(expected.size()));
        List<Map<String, Object>> hits = ObjectPath.evaluate(searchResponse, "hits.hits");
        for (Map<String, Object> hit : hits) {
            String id = (String) hit.get("_id");
            assertThat(id, hit.get("_source"), equalTo(expected.get(id)));
        }
    }

    private static String toJsonArray(Iterable<String> values) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startArray();
            for (String value : values) {
                builder.value(value);
            }
            builder.endArray();
            return Strings.toString(builder);
        }
    }
}
