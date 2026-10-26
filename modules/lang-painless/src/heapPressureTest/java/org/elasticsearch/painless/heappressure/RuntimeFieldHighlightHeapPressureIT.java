/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.equalTo;

/**
 * Highlights every hit of a search whose query references a runtime field that reads {@code params._source},
 * on a synthetic source (logsdb) index. The highlighter evaluates the query once per hit and highlighted field
 * against a throwaway reader, and each evaluation adds a synthetic source leaf to the request's
 * {@code ConcurrentSegmentSourceProvider}. None of that memory is charged to a circuit breaker, so the node runs
 * out of heap instead of rejecting the request. Only keyword runtime fields are kept out of this path by the
 * unified highlighter, so every other runtime field type is covered here.
 */
public class RuntimeFieldHighlightHeapPressureIT extends ESRestTestCase {

    private static final String INDEX = "runtime-field-highlight";
    private static final int KEYWORD_FIELDS = 100;
    private static final int TEXT_FIELDS = 10;
    private static final int TEXT_FIELD_WORDS = 150;
    private static final int DOCS = 200;
    private static final int BULK_SIZE = 50;
    private static final int TYPE_TEST_HITS = 50;
    private static final String READ_MESSAGE = "String m = params._source['message']; ";
    // matches every document through the keyword and text fields, so all of them are highlighted
    private static final String HIGHLIGHTED_MATCH = "{\"multi_match\": {\"query\": \"401\", \"lenient\": true}}";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("lang-painless")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // highlighting re-runs the runtime field script for every hit and field, which can exceed the default 60s on CI
        return Settings.builder().put(super.restClientSettings()).put(CLIENT_SOCKET_TIMEOUT, "180s").build();
    }

    public void testDoubleRuntimeFieldFromFreeTextSearch() throws IOException {
        // multi_match without fields expands to index.query.default_field (*), which includes the runtime field
        assertSearchDoesNotKillTheNode(
            format("""
                "rf": {"type": "double", "script": {"source": "%s if (m != null) { emit(m.length()); }"}}""", READ_MESSAGE),
            HIGHLIGHTED_MATCH,
            DOCS
        );
    }

    public void testLongRuntimeField() throws IOException {
        assertSearchDoesNotKillTheNode(
            format("""
                "rf": {"type": "long", "script": {"source": "%s if (m != null) { emit(m.length()); }"}}""", READ_MESSAGE),
            withHighlightedMatch("{\"term\": {\"rf\": 1}}"),
            TYPE_TEST_HITS
        );
    }

    public void testDateRuntimeField() throws IOException {
        assertSearchDoesNotKillTheNode(
            format("""
                "rf": {"type": "date", "script": {"source": "%s if (m != null) { emit(m.length()); }"}}""", READ_MESSAGE),
            withHighlightedMatch("{\"term\": {\"rf\": 1}}"),
            TYPE_TEST_HITS
        );
    }

    public void testBooleanRuntimeField() throws IOException {
        assertSearchDoesNotKillTheNode(
            format("""
                "rf": {"type": "boolean", "script": {"source": "%s emit(m != null && m.contains('401'));"}}""", READ_MESSAGE),
            withHighlightedMatch("{\"term\": {\"rf\": true}}"),
            TYPE_TEST_HITS
        );
    }

    public void testCompositeRuntimeSubfield() throws IOException {
        assertSearchDoesNotKillTheNode(format("""
            "rf": {
              "type": "composite",
              "script": {"source": "%s if (m != null) { emit(['length': m.length()]); }"},
              "fields": {"length": {"type": "long"}}
            }""", READ_MESSAGE), withHighlightedMatch("{\"term\": {\"rf.length\": 1}}"), TYPE_TEST_HITS);
    }

    private void assertSearchDoesNotKillTheNode(String runtimeField, String query, int size) throws IOException {
        createIndex();
        indexDocs();

        try {
            assertOK(client().performRequest(searchRequest(runtimeField, query, size)));
        } catch (ResponseException e) {
            // nothing on this path is charged to a breaker, so a rejection means the heap filled up and the real memory breaker tripped
            throw new AssertionError("expected the search to succeed", e);
        } catch (IOException e) {
            throw new AssertionError("the node did not answer the search, check the cluster log for an OutOfMemoryError", e);
        }
        assertOK(client().performRequest(new Request("GET", "/_cluster/health")));
    }

    private void createIndex() throws IOException {
        StringBuilder properties = new StringBuilder("""
            "@timestamp": {"type": "date"}, "host.name": {"type": "keyword"}, "message": {"type": "text"}""");
        for (int i = 0; i < KEYWORD_FIELDS; i++) {
            properties.append(", \"k").append(i).append("\": {\"type\": \"keyword\"}");
        }
        for (int i = 0; i < TEXT_FIELDS; i++) {
            properties.append(", \"t").append(i).append("\": {\"type\": \"text\"}");
        }
        Request request = new Request("PUT", "/" + INDEX);
        request.setJsonEntity(format("""
            {
              "settings": {"index.mode": "logsdb", "number_of_shards": 1, "number_of_replicas": 0},
              "mappings": {"properties": {%s}}
            }""", properties));
        assertOK(client().performRequest(request));
    }

    private void indexDocs() throws IOException {
        for (int start = 0; start < DOCS; start += BULK_SIZE) {
            StringBuilder bulk = new StringBuilder();
            for (int doc = start; doc < start + BULK_SIZE; doc++) {
                bulk.append("{\"index\": {}}\n");
                bulk.append("{\"@timestamp\": ").append(1_700_000_000_000L + doc);
                bulk.append(", \"host.name\": \"host-").append(doc % 3).append('"');
                bulk.append(", \"message\": \"request ").append(doc).append(" returned status 401\"");
                for (int i = 0; i < KEYWORD_FIELDS; i++) {
                    bulk.append(", \"k").append(i).append("\": \"401\"");
                }
                for (int i = 0; i < TEXT_FIELDS; i++) {
                    bulk.append(", \"t").append(i).append("\": \"").append(randomText()).append('"');
                }
                bulk.append("}\n");
            }
            Request request = new Request("POST", "/" + INDEX + "/_bulk");
            request.setJsonEntity(bulk.toString());
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(entityAsMap(response).get("errors"), equalTo(false));
        }
        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(forceMerge));
        assertOK(client().performRequest(new Request("POST", "/" + INDEX + "/_refresh")));
    }

    private static String randomText() {
        StringBuilder text = new StringBuilder("401");
        for (int i = 0; i < TEXT_FIELD_WORDS; i++) {
            text.append(' ').append(randomAlphaOfLengthBetween(3, 10));
        }
        return text.toString();
    }

    private static String withHighlightedMatch(String runtimeFieldClause) {
        return "{\"bool\": {\"should\": [" + HIGHLIGHTED_MATCH + ", " + runtimeFieldClause + "]}}";
    }

    private static Request searchRequest(String runtimeField, String query, int size) {
        Request request = new Request("POST", "/" + INDEX + "/_search");
        request.setJsonEntity(format("""
            {
              "size": %d,
              "_source": false,
              "runtime_mappings": {%s},
              "query": %s,
              "highlight": {"fields": {"*": {}}}
            }""", size, runtimeField, query));
        return request;
    }
}
