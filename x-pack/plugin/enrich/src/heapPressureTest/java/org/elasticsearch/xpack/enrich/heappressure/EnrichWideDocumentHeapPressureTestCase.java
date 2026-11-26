/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.heappressure;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Base class for the enrich heap pressure scenarios. A bulk of small documents goes through a pipeline whose {@code enrich} processor
 * matches a very wide document (10,000 numeric fields): every document of the bulk expands to a boxed map of about 1 MB plus a
 * re-serialized source of about 270 KB while the coordinating indexing pressure keeps charging the pre-ingest wire size. Nothing bounds
 * the expanded documents on the ingest node: each document that completes the pipeline retains its re-serialized source until the whole
 * bulk moves on to the primary, so the 2,048-document bulk (about 540 MB of re-serialized sources) must exhaust the ingest node's
 * 512 MB heap by arithmetic alone, independent of GC timing. All scenarios are expected to fail until expansion is bounded.
 *
 * <p>Two nodes: node 0 is ingest only and takes the REST traffic, so every lookup response crosses the transport; node 1 holds the
 * enrich index and the target index on a larger heap so that it does not die first while serving 128-hit responses.
 *
 * <p>Each scenario is its own class, because a node that died cannot be reused by a second test method and the source set gives every
 * class its own cluster.
 */
public abstract class EnrichWideDocumentHeapPressureTestCase extends ESRestTestCase {

    protected static final String SOURCE_INDEX = "enrich-src";
    protected static final String TARGET_INDEX = "target";
    protected static final String POLICY = "wide-policy";
    protected static final String PIPELINE = "wide-pipeline";
    /** Fields per enrich source document; the width that makes each expanded document about a megabyte. */
    protected static final int FIELD_COUNT = 10_000;
    /**
     * Enrich source documents, and documents in the bulk under test: one source document per distinct key of the distinct-keys
     * scenario. Each bulk document that completes the pipeline retains a re-serialized source of about 270 KB until the whole bulk is
     * handed to the primary, so 2,048 documents demand about 540 MB on the ingest node's 512 MB heap: the live set exceeds the heap at
     * around document 1,800, and the node dies no matter how the collector schedules.
     */
    protected static final int DOC_COUNT = 2048;
    /** Documents per upload bulk: about 10 MB per request, under the default 100 MB body limit. */
    private static final int UPLOAD_BULK_SIZE = 32;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .module("analysis-common")
        .module("ingest-common")
        .module("mapper-extras")
        .module("x-pack-enrich")
        .module("x-pack-ilm")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "basic")
        // Policy execution reindexes the wide documents in pages; the default page of 10,000 would hold all of them at once on node 1.
        .setting("enrich.fetch_size", "64")
        // The default capacity (max_concurrent_requests x max_lookups_per_request = 1,024) would racily reject the second half of the
        // 2,048-document bulk with a 429 before it expands. Production reaches the same accumulation through many concurrent bulks,
        // which the single bulk of the test stands in for; the larger queue makes the accepted-document count deterministic.
        .setting("enrich.coordinator_proxy.queue_capacity", "4096")
        // Ingest only, with the search pool of a 4-vCPU pod (7 threads), so up to 7 lookup responses are expanded at once.
        // The 512 MB heap is what the bulk sizes of the scenarios are calibrated against (the test-clusters default today,
        // pinned so a default change does not silently resize the scenario).
        .node(0, n -> n.setting("node.roles", "[ingest]").setting("node.processors", "4").jvmArg("-Xms512m").jvmArg("-Xmx512m"))
        // Holds the data; larger only so it survives serving the lookup responses and the reindex.
        .node(1, n -> n.jvmArg("-Xms2g").jvmArg("-Xmx2g"))
        .build();

    private static String metricsJson;
    private static int metricsSmileLength;

    private final Map<String, Map<String, Long>> samplerMaxima = new ConcurrentHashMap<>();
    private final AtomicInteger samplerSamples = new AtomicInteger();
    private final AtomicInteger samplerFailures = new AtomicInteger();
    private final AtomicReference<String> samplerLastFailure = new AtomicReference<>();
    private volatile boolean samplerRunning;
    private Thread sampler;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(0);
    }

    @Override
    protected Settings restClientSettings() {
        // The bulk under test expands every document before it answers; a dropped connection, not a timeout, is the death signal.
        return Settings.builder().put(super.restClientSettings()).put(CLIENT_SOCKET_TIMEOUT, "10m").build();
    }

    @After
    public void stopSamplerAndCleanUp() throws InterruptedException {
        stopSampler();
        for (Request request : List.of(
            new Request("DELETE", "/_ingest/pipeline/" + PIPELINE),
            new Request("DELETE", "/_enrich/policy/" + POLICY)
        )) {
            try {
                adminClient().performRequest(request);
            } catch (Exception e) {
                logger.info("cleanup [{}] failed: {}", request.getEndpoint(), e.getMessage());
            }
        }
    }

    /**
     * Index of the key a bulk document at position {@code i} carries: distinct per document, or the same one every time. Must be less
     * than {@link #DOC_COUNT}, the number of source documents.
     */
    protected abstract int keyIndexForBulkDocument(int i);

    /** Whether one document goes through the pipeline, and is acknowledged, before the bulk, so that its key is a cache hit. */
    protected abstract boolean primeCache();

    /**
     * Runs the scenario: builds the enrich index and the pipeline, optionally primes the cache, then sends the bulk and asserts that
     * every item was accepted or rejected with a 429 and that node 0 still answers.
     */
    protected void runScenario() throws Exception {
        setUpEnrichIndexAndPipeline();
        if (primeCache()) {
            Request prime = new Request("PUT", "/" + TARGET_INDEX + "/_doc/prime");
            prime.addParameter("pipeline", PIPELINE);
            prime.addParameter("refresh", "true");
            prime.setJsonEntity("{\"key\":\"" + key(keyIndexForBulkDocument(0)) + "\"}");
            Response response = client().performRequest(prime);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
            logEnrichStats("after priming");
        }

        startSampler();
        BulkOutcome outcome;
        try {
            outcome = sendBulk();
        } finally {
            stopSampler();
        }
        logSamplerMaxima();
        logEnrichStats("after the bulk");
        logger.info("bulk statuses: {}", outcome.statusCounts);
        for (Map.Entry<String, String> entry : outcome.rejectionBodies.entrySet()) {
            logger.info("429 body [{}]: {}", entry.getKey(), entry.getValue());
        }
        if (outcome.transportFailure != null) {
            logger.info("bulk transport failure: {}", outcome.transportFailure);
        }

        assertNull("the bulk did not get a response, the node is likely dead: " + outcome.transportFailure, outcome.transportFailure);
        List<Integer> unexpected = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : outcome.statusCounts.entrySet()) {
            int status = entry.getKey();
            if ((status >= 200 && status < 300) == false && status != 429) {
                unexpected.add(status);
            }
        }
        assertThat("expected only 2xx or 429 statuses, got " + outcome.statusCounts, unexpected, hasSize(0));
        assertThat(client().performRequest(new Request("GET", "/_cluster/health")).getStatusLine().getStatusCode(), equalTo(200));
    }

    protected static String key(int index) {
        return Strings.format("k-%05d", index);
    }

    private static String fieldName(int index) {
        return Strings.format("metric_%05d", index);
    }

    /** The wide document body shared by every source document, built once, with its SMILE length for the cache size check. */
    private static synchronized void buildMetrics() throws IOException {
        if (metricsJson != null) {
            return;
        }
        StringBuilder json = new StringBuilder();
        try (XContentBuilder smile = XContentFactory.smileBuilder()) {
            smile.startObject();
            smile.field("key", key(0));
            for (int f = 1; f <= FIELD_COUNT; f++) {
                double value = f + 0.123456;
                json.append(",\"").append(fieldName(f)).append("\":").append(value);
                smile.field(fieldName(f), value);
            }
            smile.endObject();
            metricsSmileLength = BytesReference.bytes(smile).length();
        }
        metricsJson = json.toString();
    }

    private void setUpEnrichIndexAndPipeline() throws IOException {
        buildMetrics();
        logger.info(
            "source document: {} fields, JSON length {} bytes, SMILE length {} bytes",
            FIELD_COUNT,
            ("{\"key\":\"" + key(0) + "\"" + metricsJson + "}").length(),
            metricsSmileLength
        );

        Request createSource = new Request("PUT", "/" + SOURCE_INDEX);
        createSource.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0},
              "mappings": {"dynamic": false, "properties": {"key": {"type": "keyword"}}}
            }
            """);
        assertOK(client().performRequest(createSource));

        for (int from = 0; from < DOC_COUNT; from += UPLOAD_BULK_SIZE) {
            StringBuilder bulk = new StringBuilder();
            for (int i = from; i < Math.min(from + UPLOAD_BULK_SIZE, DOC_COUNT); i++) {
                bulk.append("{\"index\":{\"_id\":\"").append(key(i)).append("\"}}\n");
                bulk.append("{\"key\":\"").append(key(i)).append('"').append(metricsJson).append("}\n");
            }
            Request upload = new Request("POST", "/" + SOURCE_INDEX + "/_bulk");
            upload.setJsonEntity(bulk.toString());
            Map<String, Object> body = entityAsMap(client().performRequest(upload));
            assertThat("upload bulk from " + from + " had errors", body.get("errors"), equalTo(false));
        }
        assertOK(client().performRequest(new Request("POST", "/" + SOURCE_INDEX + "/_refresh")));

        StringBuilder policy = new StringBuilder();
        policy.append("{\"match\":{\"indices\":\"").append(SOURCE_INDEX).append("\",\"match_field\":\"key\",\"enrich_fields\":[");
        for (int f = 1; f <= FIELD_COUNT; f++) {
            if (f > 1) {
                policy.append(',');
            }
            policy.append('"').append(fieldName(f)).append('"');
        }
        policy.append("]}}");
        Request putPolicy = new Request("PUT", "/_enrich/policy/" + POLICY);
        putPolicy.setJsonEntity(policy.toString());
        assertOK(client().performRequest(putPolicy));
        assertOK(client().performRequest(new Request("POST", "/_enrich/policy/" + POLICY + "/_execute")));

        Request putPipeline = new Request("PUT", "/_ingest/pipeline/" + PIPELINE);
        putPipeline.setJsonEntity(Strings.format("""
            {
              "processors": [
                {"enrich": {"policy_name": "%s", "field": "key", "target_field": "enriched", "max_matches": 1}}
              ]
            }
            """, POLICY));
        assertOK(client().performRequest(putPipeline));

        Request createTarget = new Request("PUT", "/" + TARGET_INDEX);
        createTarget.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0},
              "mappings": {"dynamic": false, "properties": {"key": {"type": "keyword"}}}
            }
            """);
        assertOK(client().performRequest(createTarget));
    }

    /** Status counts of the bulk items, one 429 body per distinct reason, and the transport failure if the bulk got no response. */
    protected record BulkOutcome(Map<Integer, Integer> statusCounts, Map<String, String> rejectionBodies, String transportFailure) {}

    private BulkOutcome sendBulk() {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.append("{\"index\":{}}\n{\"key\":\"").append(key(keyIndexForBulkDocument(i))).append("\"}\n");
        }
        logger.info("sending a bulk of {} documents, {} bytes on the wire", DOC_COUNT, bulk.length());
        Request request = new Request("POST", "/" + TARGET_INDEX + "/_bulk");
        request.addParameter("pipeline", PIPELINE);
        request.setJsonEntity(bulk.toString());

        Map<Integer, Integer> statusCounts = new TreeMap<>();
        Map<String, String> rejectionBodies = new TreeMap<>();
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            List<?> items = (List<?>) body.get("items");
            for (Object item : items) {
                Map<?, ?> action = (Map<?, ?>) ((Map<?, ?>) item).values().iterator().next();
                int status = ((Number) action.get("status")).intValue();
                statusCounts.merge(status, 1, Integer::sum);
                if (status == 429) {
                    Object error = action.get("error");
                    String reason = error instanceof Map<?, ?> m ? String.valueOf(m.get("type")) : "unknown";
                    rejectionBodies.putIfAbsent(reason, String.valueOf(error));
                }
            }
            return new BulkOutcome(statusCounts, rejectionBodies, null);
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            statusCounts.merge(status, DOC_COUNT, Integer::sum);
            String body;
            try {
                body = EntityUtils.toString(e.getResponse().getEntity());
            } catch (IOException ioe) {
                body = ioe.toString();
            }
            if (status == 429) {
                rejectionBodies.put("whole bulk", body);
            } else {
                logger.info("bulk rejected with {}: {}", status, body);
            }
            return new BulkOutcome(statusCounts, rejectionBodies, null);
        } catch (IOException e) {
            return new BulkOutcome(statusCounts, rejectionBodies, e.toString());
        }
    }

    private void logEnrichStats(String when) {
        try {
            Response response = adminClient().performRequest(new Request("GET", "/_enrich/_stats"));
            logger.info("enrich stats {}: {}", when, EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            logger.info("enrich stats {} unavailable: {}", when, e.getMessage());
        }
    }

    private static final String[] SAMPLED = new String[] {
        "jvm.mem.heap_used_in_bytes",
        "jvm.mem.heap_max_in_bytes",
        "indexing_pressure.memory.current.coordinating_in_bytes",
        "indexing_pressure.memory.current.primary_in_bytes",
        "indexing_pressure.memory.current.all_in_bytes",
        "indexing_pressure.memory.total.coordinating_rejections",
        "breakers.request.estimated_size_in_bytes",
        "breakers.in_flight_requests.estimated_size_in_bytes",
        "breakers.parent.estimated_size_in_bytes",
        "breakers.parent.tripped" };

    /** Polls the node stats every 250 ms on the admin client and keeps the maximum of each sampled value per node. */
    private void startSampler() {
        samplerRunning = true;
        sampler = new Thread(() -> {
            while (samplerRunning) {
                try {
                    sample();
                    samplerSamples.incrementAndGet();
                } catch (Exception e) {
                    samplerFailures.incrementAndGet();
                    samplerLastFailure.set(e.toString());
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "node-stats-sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    /** Stops the sampler and waits for it, so that no poll still in flight updates the maxima after they are logged. */
    private void stopSampler() throws InterruptedException {
        samplerRunning = false;
        if (sampler != null) {
            sampler.interrupt();
            sampler.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse("the node stats sampler did not stop", sampler.isAlive());
            sampler = null;
        }
    }

    @SuppressWarnings("unchecked")
    private void sample() throws IOException {
        Request request = new Request("GET", "/_nodes/stats/indexing_pressure,jvm,breaker");
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(adminClient().performRequest(request)).get("nodes");
        for (Object value : nodes.values()) {
            Map<String, Object> node = (Map<String, Object>) value;
            Map<String, Long> maxima = samplerMaxima.computeIfAbsent((String) node.get("name"), k -> new ConcurrentHashMap<>());
            for (String path : SAMPLED) {
                Object sampled = XContentMapValues.extractValue(path, node);
                if (sampled instanceof Number number) {
                    maxima.merge(path, number.longValue(), Math::max);
                }
            }
        }
    }

    private void logSamplerMaxima() {
        logger.info("sampler: {} samples, {} failed polls, last failure: {}", samplerSamples, samplerFailures, samplerLastFailure.get());
        for (Map.Entry<String, Map<String, Long>> node : new TreeMap<>(samplerMaxima).entrySet()) {
            logger.info("sampler maxima on [{}]: {}", node.getKey(), new TreeMap<>(node.getValue()));
        }
    }
}
