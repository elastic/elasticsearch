/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Base class for tests that build large automatons on a {@code wildcard} field. Each subclass provides its own {@code @ClassRule}
 * cluster, because the heap size is fixed at node startup.
 *
 * <p>Request caching is disabled on every search so that each request builds its automaton on every shard rather than being
 * answered from the shard request cache.
 */
public abstract class WildcardFieldHeapPressureTestCase extends ESRestTestCase {

    private static final String INDEX = "wildcard-heap-pressure";
    private static final int SHARDS = 5;
    // Set this to true to disable the circuit breaker and validate that the OOM actually happens instead of being caught by the breaker.
    private static final boolean VALIDATE_OOM = false;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.DEFAULT)
        .module("test-pausable-field")
        .setting("xpack.security.enabled", "false")
        // The ML native controller isn't needed, and fails to start on some machines
        .setting("xpack.ml.enabled", "false")
        // Test nodes run with two processors, which gives a search pool of only four threads. Parked requests hold one thread each, so
        // the pool has to be large enough for their automatons to add up to more than the heap.
        .setting("thread_pool.search.size", "32")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        .apply(c -> {
            if (VALIDATE_OOM) {
                c.setting("indices.breaker.request.limit", "-1");
                c.setting("indices.breaker.total.use_real_memory", "false");
                c.setting("indices.breaker.total.limit", "100gb");
            }
        })
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": %d, "number_of_replicas": 0},
              "mappings": {
                "runtime": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
                "properties": {"w": {"type": "wildcard"}}
              }
            }
            """, SHARDS));
        client().performRequest(create);

        // Automatons are built per shard however few documents it holds, so a few per shard is enough.
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < SHARDS * 10; i++) {
            bulk.append("{\"index\":{}}\n{\"w\":\"value-").append(i).append("-abcabcabc\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }

    /** Never leave the field blocked: a blocked script would hang every later search on the node. */
    @After
    public void unblockPausableField() throws IOException {
        unblockPauseField();
    }

    /**
     * A wildcard search that also filters on the pausable field {@code p}. The automaton is built with the query, and the script only
     * runs later, when documents are scored, so while the field is blocked every such request keeps its automaton alive and parks a
     * search thread.
     */
    protected static Request pausableWildcardSearch(String pattern) {
        return pausableSearch(Strings.format("""
            {"wildcard": {"w": {"value": "%s"}}}""", pattern));
    }

    /** Like {@link #pausableWildcardSearch}, for a regexp query. */
    protected static Request pausableRegexpSearch(String pattern, int maxDeterminizedStates) {
        return pausableSearch(Strings.format("""
            {"regexp": {"w": {"value": "%s", "max_determinized_states": %d}}}""", pattern, maxDeterminizedStates));
    }

    private static Request pausableSearch(String mustClause) {
        return search(Strings.format("""
            {
              "size": 0,
              "query": {
                "bool": {
                  "must": [%s],
                  "filter": [{"term": {"p": 1}}]
                }
              }
            }
            """, mustClause));
    }

    protected static void blockPauseField() throws IOException {
        // The control requests go through the admin client, which has its own connection pool: with every connection of client() held by a
        // parked search, they would otherwise wait for a connection that only they can free.
        adminClient().performRequest(new Request("POST", "/_pause_field/block"));
    }

    protected static void unblockPauseField() throws IOException {
        adminClient().performRequest(new Request("POST", "/_pause_field/unblock"));
    }

    /**
     * Waits until enough requests overlap: at least {@code minActive} search threads are busy, which with the field blocked
     * means each is holding an automaton, or the request breaker has already rejected one. Either shows the requests overlapped, and
     * which of the two happens depends on whether the automatons are charged to the breaker: without the charge the pool fills, with it
     * the breaker rejects before the pool does. Fails if neither happens, rather than carrying on without any overlap.
     */
    protected void waitForOverlappingRequests(int minActive) throws Exception {
        assertBusy(() -> {
            Map<String, Object> node = nodeStats();
            Number active = (Number) XContentMapValues.extractValue("thread_pool.search.active", node);
            Number tripped = (Number) XContentMapValues.extractValue("breakers.request.tripped", node);
            assertTrue(
                Strings.format("expected %d active search threads or a tripped request breaker, got %s and %s", minActive, active, tripped),
                active.intValue() >= minActive || tripped.longValue() > 0
            );
            // Diagnostic for sizing the patterns: how much the breaker charged and the heap actually in use at the point of overlap.
            logger.info(
                "overlap reached: active={} tripped={} request breaker={}/{} parent breaker={} heap used={}",
                active,
                tripped,
                XContentMapValues.extractValue("breakers.request.estimated_size_in_bytes", node),
                XContentMapValues.extractValue("breakers.request.limit_size_in_bytes", node),
                XContentMapValues.extractValue("breakers.parent.estimated_size_in_bytes", node),
                XContentMapValues.extractValue("jvm.mem.heap_used_in_bytes", node)
            );
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> nodeStats() throws IOException {
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(
            adminClient().performRequest(new Request("GET", "/_nodes/stats/thread_pool,breaker,jvm"))
        ).get("nodes");
        return (Map<String, Object>) nodes.values().iterator().next();
    }

    protected static Request wildcardSearch(String pattern) {
        return search(Strings.format("""
            {"size": 0, "query": {"wildcard": {"w": {"value": "%s"}}}}
            """, pattern));
    }

    protected static Request regexpSearch(String pattern, int maxDeterminizedStates) {
        return search(Strings.format("""
            {"size": 0, "query": {"regexp": {"w": {"value": "%s", "max_determinized_states": %d}}}}
            """, pattern, maxDeterminizedStates));
    }

    private static Request search(String body) {
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.addParameter("request_cache", "false");
        search.setJsonEntity(body);
        return search;
    }

    /**
     * Performs the request and returns the parsed error body, or {@code null} if it succeeded. The entity is read here so that
     * it is consumed on the calling thread before the HTTP connection is returned to the pool.
     */
    protected Map<String, Object> errorBodyOrNull(Request request) throws IOException {
        try {
            client().performRequest(request);
            return null;
        } catch (ResponseException e) {
            return entityAsMap(e.getResponse());
        }
    }

    /**
     * Submits {@code count} identical callables that all start at the same instant via a latch. The pool is always shut down
     * in the finally block so a test failure does not leave threads running.
     */
    protected <T> List<Future<T>> submit(int count, Callable<T> task) {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return task.call();
                }));
            }
            start.countDown();
            return futures;
        } finally {
            pool.shutdown();
        }
    }

    /** The breaker error is nested at a depth that depends on the failing phase, so search the whole body. */
    protected static boolean containsCircuitBreakingException(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            if ("circuit_breaking_exception".equals(map.get("type"))) {
                return true;
            }
            for (Object value : map.values()) {
                if (containsCircuitBreakingException(value)) {
                    return true;
                }
            }
        } else if (obj instanceof List<?> list) {
            for (Object item : list) {
                if (containsCircuitBreakingException(item)) {
                    return true;
                }
            }
        }
        return false;
    }
}
