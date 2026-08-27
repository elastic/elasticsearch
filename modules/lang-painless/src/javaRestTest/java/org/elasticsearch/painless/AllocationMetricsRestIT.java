/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Runs scripts on a real node with allocation metrics switched on.
 * <p>
 * Unit tests drive the counters directly; what they cannot reach is the part that only exists in a running node. Enabling
 * metrics makes a compile register a counter per bucket, under a name built from the script's context, and makes the
 * generated class read a recorder injected as a static constant. A context name that the metric name pattern rejects, or a
 * recorder the generated class cannot see, throws while compiling — so every script for that context fails, and no unit
 * test would notice because they all use one synthetic context.
 * <p>
 * So each test here runs a script through a different real context and checks the answer. Getting the right answer back is
 * the assertion: it means the script compiled, the counters registered under a legal name, and the recorder was reachable.
 */
public class AllocationMetricsRestIT extends ESRestTestCase {

    /**
     * Mirrors {@code CompilerSettings.ALLOCATION_METRICS_ENABLED_PROPERTY}. A REST test talks to the cluster over HTTP and
     * has none of the plugin's classes on its classpath, so the name is repeated rather than referenced. If the two drift,
     * these tests still pass, but with metrics off — which is to say they stop testing anything.
     */
    private static final String ALLOCATION_METRICS_ENABLED_PROPERTY = "es.painless.allocation_metrics.enabled";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("lang-painless")
        // For the script ingest processor, which is how testIngestScript reaches the ingest context.
        .module("ingest-common")
        // Without apm the registry is a no-op that accepts any metric name. With it, registering a counter validates the
        // name, so a context whose name cannot form a legal one fails the compile and these tests catch it.
        .module("apm")
        .setting("telemetry.metrics.enabled", "true")
        .systemProperty(ALLOCATION_METRICS_ENABLED_PROPERTY, "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private static void indexDocument(String index, String id, String body) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> search(String index, String body) throws IOException {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(body);

        return entityAsMap(assertOK(client().performRequest(request)));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hitsOf(Map<String, Object> response) {
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    /** The score context, whose scripts run once per matching document. */
    public void testScoreScript() throws IOException {
        indexDocument("metrics_score", "1", """
            {"n": 7}""");

        Map<String, Object> response = search("metrics_score", """
            {
              "query": {
                "script_score": {
                  "query": {"match_all": {}},
                  "script": {"source": "doc['n'].value * 2"}
                }
              }
            }""");

        List<Map<String, Object>> hits = hitsOf(response);
        assertEquals(1, hits.size());
        assertEquals(14.0, ((Number) hits.get(0).get("_score")).doubleValue(), 0.0);
    }

    /** A runtime field, which allocates on every document it is evaluated for. */
    public void testRuntimeFieldScript() throws IOException {
        indexDocument("metrics_runtime", "1", """
            {"first": "sam", "last": "carter"}""");

        Map<String, Object> response = search("metrics_runtime", """
            {
              "runtime_mappings": {
                "full": {
                  "type": "keyword",
                  "script": {"source": "emit(params._source.first + ' ' + params._source.last)"}
                }
              },
              "fields": ["full"],
              "query": {"match_all": {}}
            }""");

        List<Map<String, Object>> hits = hitsOf(response);
        assertEquals(1, hits.size());
        assertEquals(List.of("sam carter"), ((Map<?, ?>) hits.get(0).get("fields")).get("full"));
    }

    /** The ingest context, reached through a pipeline rather than a search. */
    public void testIngestScript() throws IOException {
        Request pipeline = new Request("PUT", "/_ingest/pipeline/metrics_pipeline");
        pipeline.setJsonEntity("""
            {
              "processors": [
                {"script": {"source": "ctx.doubled = ctx.n * 2"}}
              ]
            }""");
        assertOK(client().performRequest(pipeline));

        Request index = new Request("PUT", "/metrics_ingest/_doc/1");
        index.addParameter("pipeline", "metrics_pipeline");
        index.addParameter("refresh", "true");
        index.setJsonEntity("""
            {"n": 21}""");
        assertOK(client().performRequest(index));

        Map<String, Object> response = search("metrics_ingest", """
            {"query": {"match_all": {}}}""");

        List<Map<String, Object>> hits = hitsOf(response);
        assertEquals(1, hits.size());
        assertEquals(42, ((Map<?, ?>) hits.get(0).get("_source")).get("doubled"));
    }

    /** An update script, a context whose scripts run once per updated document. */
    public void testUpdateScript() throws IOException {
        indexDocument("metrics_update", "1", """
            {"n": 1}""");

        Request update = new Request("POST", "/metrics_update/_update/1");
        update.addParameter("refresh", "true");
        update.setJsonEntity("""
            {"script": {"source": "ctx._source.n += params.by", "params": {"by": 4}}}""");
        assertOK(client().performRequest(update));

        Map<String, Object> response = search("metrics_update", """
            {"query": {"match_all": {}}}""");

        assertEquals(5, ((Map<?, ?>) hitsOf(response).get(0).get("_source")).get("n"));
    }

    /** Allocation tracking must still fail a script that exceeds the limit while metrics are recording. */
    public void testLimitStillAppliesWhileRecording() throws IOException {
        Request request = new Request("POST", "/_scripts/painless/_execute");
        request.setJsonEntity("""
            {"script": {"source": "def s = ''; for (int i = 0; i < 100; ++i) { s += 'abcdefghij'; } return s.length();"}}""");

        // No limit is configured on this cluster, so a heavy script still completes; the point is that recording does not
        // change the answer.
        Map<String, Object> response = entityAsMap(assertOK(client().performRequest(request)));
        assertEquals("1000", response.get("result"));
    }

    /** A script that does not compile must still fail cleanly rather than failing inside metric registration. */
    public void testCompilationErrorIsStillReported() {
        Request request = new Request("POST", "/_scripts/painless/_execute");
        request.setJsonEntity("""
            {"script": {"source": "this is not painless"}}""");

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    }
}
