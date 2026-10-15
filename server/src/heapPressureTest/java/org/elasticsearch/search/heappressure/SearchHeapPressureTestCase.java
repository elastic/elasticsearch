/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for search tests against nodes with a deliberately small heap, where a request whose memory is not charged to a circuit
 * breaker brings a node down instead of being rejected. Each subclass owns its {@code @ClassRule} cluster, because heap sizes are fixed at
 * node startup, and runs its cases one after another.
 *
 * <p>Between cases the request breaker of every node must be back to zero, so a leaked charge cannot make the next case trip. Once a node
 * has died, the remaining cases are skipped.
 */
public abstract class SearchHeapPressureTestCase extends HeapPressureTestCase {

    /**
     * Size of the {@code big} field of {@link #indexLargeDocuments}. {@link #LARGE_DOCS} of them are about the size of a 256 MB heap, so
     * holding them all cannot fit, but a few at a time is harmless.
     */
    protected static final int DOC_BYTES = 256 * 1024;
    protected static final int LARGE_DOCS = 1000;

    /** A {@code big} field that is only ever read back from {@code _source}. */
    protected static final String SOURCE_ONLY_KEYWORD = """
        {"type": "keyword", "index": false, "doc_values": false}""";

    protected abstract ElasticsearchCluster cluster();

    /**
     * Settings every node of a subclass cluster shares, before the nodes and their heaps are added.
     */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> heapConstrainedCluster() {
        return ElasticsearchCluster.local()
            // The base class unblocks the pausable field after every case, which needs the module.
            .module("test-pausable-field")
            // An OutOfMemoryError must end the node so the test sees it.
            .jvmArg("-XX:+ExitOnOutOfMemoryError")
            .jvmArg("-XX:-HeapDumpOnOutOfMemoryError")
            // Only charged bytes count towards the parent breaker. Real memory accounting would also trip on uncharged memory, which hides
            // the accounting gaps these tests look for.
            .setting("indices.breaker.total.use_real_memory", "false");
    }

    /** A single node with a {@code heapSize} heap that holds the data and coordinates the requests. */
    protected static LocalClusterSpecBuilder<ElasticsearchCluster> heapConstrainedNode(String heapSize) {
        return heapConstrainedCluster().nodes(1).jvmArg("-Xms" + heapSize).jvmArg("-Xmx" + heapSize);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster().getHttpAddresses();
    }

    /** Fixtures are expensive to index, so every case of the class shares them. */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean shouldFailureSkipRemainingTests() {
        return nodesAreAlive() == false;
    }

    @Before
    @After
    public void assertRequestBreakerReleased() throws Exception {
        if (previousFailureSkipsRemaining()) {
            return;
        }
        assertBusy(() -> {
            Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest(new Request("GET", "/_nodes/stats/breaker"))).get(
                "nodes"
            );
            for (Object node : nodes.values()) {
                Map<?, ?> request = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) node).get("breakers")).get("request");
                assertThat("request breaker still holds bytes: " + request, request.get("estimated_size_in_bytes"), equalTo(0));
            }
        });
    }

    /**
     * Creates {@code index} with {@link #LARGE_DOCS} documents, each with a unique {@code tag} keyword and a {@code big} field of
     * {@link #DOC_BYTES}, unless an earlier case already did. The index is merged into one segment per shard, so each shard runs one
     * query, one fetch and one aggregator over all of its documents.
     *
     * @param bigField the mapping of {@code big}
     * @param bigValue the {@code big} value, {@link #DOC_BYTES} characters that need no escaping in JSON
     */
    protected void indexLargeDocuments(String index, int shards, String bigField, String bigValue) throws IOException {
        assertEquals(DOC_BYTES, bigValue.length());
        indexDocuments(index, shards, Strings.format("""
            {"tag": {"type": "keyword"}, "big": %s}""", bigField), LARGE_DOCS, i -> Strings.format("""
            {"tag":"tag-%04d","big":"%s"}""", i, bigValue));
    }

    /**
     * Creates {@code index} with the given mapping properties and documents, unless an earlier case of the class already did.
     */
    protected void indexDocuments(String index, int shards, String properties, int docs, IntFunction<String> doc) throws IOException {
        if (indexExists(index)) {
            return;
        }
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": %d, "number_of_replicas": 0},
              "mappings": {"properties": %s}
            }
            """, shards, properties));
        client().performRequest(create);

        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < docs; i++) {
            bulk.append("{\"index\":{}}\n").append(doc.apply(i)).append('\n');
            // A few MB per bulk request, which a 256 MB node handles.
            if (bulk.length() >= 4 * 1024 * 1024 || i == docs - 1) {
                Request request = new Request("POST", "/" + index + "/_bulk");
                request.setJsonEntity(bulk.toString());
                assertFalse((Boolean) entityAsMap(client().performRequest(request)).get("errors"));
                bulk.setLength(0);
            }
        }
        Request forceMerge = new Request("POST", "/" + index + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        client().performRequest(forceMerge);
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
        ensureGreen(index);
    }

    /**
     * One {@code terms} bucket per tag of {@link #indexLargeDocuments}, each with a {@code top_hits} returning its single document under
     * the given {@code _source}.
     */
    protected static String topHitsPerTag(int tags, String source) {
        return Strings.format("""
            {
              "size": 0,
              "aggs": {
                "tags": {
                  "terms": {"field": "tag", "size": %d},
                  "aggs": {"top": {"top_hits": {"size": 1, "_source": %s}}}
                }
              }
            }
            """, tags, source);
    }

    protected static Request search(String index, String body) {
        Request search = new Request("POST", "/" + index + "/_search");
        // Bypass the shard request cache.
        search.addParameter("request_cache", "false");
        search.setJsonEntity(body);
        return search;
    }

    /**
     * Asserts that the request is rejected by a circuit breaker charge made under {@code label}, such as {@code fetch[source]}, and that
     * every node survives. Checking the label keeps a case from passing on some other charge. The whole request must fail, not return the
     * hits that did fit.
     */
    protected void assertCircuitBreaks(Request request, String label) throws IOException {
        Map<String, Object> body = errorBodyOrNull(request);
        assertNotNull("expected a circuit breaker to reject the request, but it succeeded", body);
        assertCircuitBreakingException(body, label);
        assertNodesAlive();
    }

    /**
     * Asserts that a circuit breaker charge made under one of {@code labels} rejects the request, or a part of it that the response
     * reports, as multi-search does per search, and that every node survives.
     */
    protected void assertCircuitBreaksInPart(Request request, String... labels) throws IOException {
        Map<String, Object> body;
        try {
            body = entityAsMap(performOrFailOnExit(request));
        } catch (ResponseException e) {
            body = entityAsMap(e.getResponse());
        }
        assertCircuitBreakingException(body, labels);
        assertNodesAlive();
    }

    /** Asserts that the request succeeds and that every node survives it. */
    protected void assertSucceeds(Request request) throws IOException {
        assertNull("expected the request to succeed", errorBodyOrNull(request));
        assertNodesAlive();
    }

    protected void assertNodesAlive() throws IOException {
        assertTrue("a node exited, most likely on an OutOfMemoryError", nodesAreAlive());
        assertNodeAlive();
    }

    private static void assertCircuitBreakingException(Map<String, Object> body, String... labels) {
        for (String label : labels) {
            if (containsCircuitBreakingException(body, "data for [" + label + "]")) {
                return;
            }
        }
        String text = body.toString();
        // A body with hits can be hundreds of MB.
        fail(
            "expected a circuit_breaking_exception for one of "
                + Arrays.toString(labels)
                + ", but got: "
                + text.substring(0, Math.min(text.length(), 4096))
        );
    }

    /** As in the base class, but a node exit is reported as the failure instead of the bare I/O error. */
    @Override
    protected Map<String, Object> errorBodyOrNull(Request request) throws IOException {
        try {
            performOrFailOnExit(request);
            return null;
        } catch (ResponseException e) {
            return entityAsMap(e.getResponse());
        }
    }

    /** Reports a connection lost because a node exited as such, rather than as the bare I/O error. */
    private Response performOrFailOnExit(Request request) throws IOException {
        try {
            return client().performRequest(request);
        } catch (ResponseException e) {
            throw e;
        } catch (IOException e) {
            if (aNodeExits()) {
                throw new AssertionError("a node exited while serving the request, most likely on an OutOfMemoryError", e);
            }
            throw e;
        }
    }

    private boolean nodesAreAlive() {
        for (int node = 0; node < cluster().getNumNodes(); node++) {
            if (nodeProcess(node).map(ProcessHandle::isAlive).orElse(false) == false) {
                return false;
            }
        }
        return true;
    }

    /** A node killed by an {@link OutOfMemoryError} can drop its connections a moment before its process is gone, so wait for it. */
    private boolean aNodeExits() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (int node = 0; node < cluster().getNumNodes(); node++) {
            Optional<ProcessHandle> process = nodeProcess(node);
            if (process.isEmpty()) {
                return true;
            }
            try {
                process.get().onExit().get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                return true;
            } catch (TimeoutException e) {
                // check the next node
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
        return false;
    }

    private Optional<ProcessHandle> nodeProcess(int node) {
        try {
            return ProcessHandle.of(cluster().getPid(node));
        } catch (RuntimeException e) {
            logger.info("could not read the pid of node [{}]", node, e);
            return Optional.empty();
        }
    }

    /** The breaker error is nested at a depth that depends on the phase that failed, so search the whole body. */
    private static boolean containsCircuitBreakingException(Object obj, String reasonFragment) {
        if (obj instanceof Map<?, ?> map) {
            if ("circuit_breaking_exception".equals(map.get("type"))
                && map.get("reason") instanceof String reason
                && reason.contains(reasonFragment)) {
                return true;
            }
            for (Object value : map.values()) {
                if (containsCircuitBreakingException(value, reasonFragment)) {
                    return true;
                }
            }
        } else if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (containsCircuitBreakingException(item, reasonFragment)) {
                    return true;
                }
            }
        }
        return false;
    }
}
