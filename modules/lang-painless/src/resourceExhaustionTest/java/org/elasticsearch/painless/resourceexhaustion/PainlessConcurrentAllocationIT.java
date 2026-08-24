/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.resourceexhaustion;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.equalTo;

/**
 * Stress-tests the Painless allocation limit under concurrent load in the {@code score} context.
 * Five threads fire search requests simultaneously; each script execution has its own independent
 * allocation counter — there is no shared budget across concurrent executions.
 *
 * <p>The cluster runs with a 512 MB heap and a 50 MB per-execution limit. A lower limit than
 * the single-execution tests is used here because peak heap is proportional to the number of
 * concurrent executions that reach the limit simultaneously (5 × 50 MB = 250 MB in the worst
 * case), which must remain well under the 512 MB ceiling alongside the server's own footprint.
 *
 * <p>Two scenarios are tested:
 * <ul>
 *   <li>All scripts stay under the per-execution budget: every concurrent request returns 200.
 *   <li>All scripts exceed the per-execution budget: every concurrent request independently
 *       trips its own limit and returns a {@code painless_error}, without any one execution
 *       interfering with another's accounting.
 * </ul>
 */
public class PainlessConcurrentAllocationIT extends ResourceExhaustionPainlessTestCase {

    private static final String INDEX = "painless-concurrent-alloc";
    // 1 MB per chunk gives fine-grained control over how close each execution gets to the limit.
    private static final int CHUNK_BYTES = 1024 * 1024;
    // 20 iterations × 1 MB = 20 MB per execution — safely under the 50 MB limit.
    private static final int SUCCESS_ITERS = 20;
    // 100 iterations × 1 MB would require 100 MB; the limit fires at the 50th chunk (50 MB).
    private static final int FAILURE_ITERS = 100;
    private static final int THREAD_COUNT = 5;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("lang-painless")
        .setting("xpack.security.enabled", "false")
        .setting("script.painless.max_allocation_bytes.context.score.limit", "50mb")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws Exception {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{\"settings\": {\"number_of_replicas\": 0}}");
        client().performRequest(create);

        Request doc = new Request("POST", "/" + INDEX + "/_doc");
        doc.setJsonEntity("{\"value\": 1}");
        client().performRequest(doc);

        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    public void testConcurrentScriptsUnderLimitAllSucceed() throws Exception {
        Request search = scoreSearch(SUCCESS_ITERS);
        List<Future<Integer>> futures = submit(THREAD_COUNT, () -> client().performRequest(search).getStatusLine().getStatusCode());
        for (int i = 0; i < futures.size(); i++) {
            assertThat("thread " + i + " expected 200", futures.get(i).get(), equalTo(200));
        }
    }

    public void testConcurrentScriptsOverLimitAllFail() throws Exception {
        Request search = scoreSearch(FAILURE_ITERS);
        // Read the entity on the background thread before the HTTP connection is released.
        List<Future<Map<String, Object>>> futures = submit(THREAD_COUNT, () -> {
            try {
                client().performRequest(search);
                return null; // signals unexpected success
            } catch (ResponseException e) {
                return entityAsMap(e.getResponse());
            }
        });
        for (int i = 0; i < futures.size(); i++) {
            Map<String, Object> body = futures.get(i).get();
            assertNotNull("thread " + i + " expected allocation limit to fire but script succeeded", body);
            assertAllocationLimitExceeded(body);
        }
    }

    /**
     * Submits {@code count} identical callables that all start at the same instant via a latch,
     * then waits for the pool to finish before returning. The pool is always shut down in the
     * finally block so a test failure does not leave threads running.
     */
    private <T> List<Future<T>> submit(int count, Callable<T> task) throws InterruptedException {
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

    private Request scoreSearch(int iterations) {
        // Build the script source via concatenation so integer literals are always ASCII,
        // regardless of the randomized test locale.
        String script = "long t = 0; for (int i = 0; i < "
            + iterations
            + "; i++) { byte[] c = new byte["
            + CHUNK_BYTES
            + "]; t += c.length; } return (double) t;";
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.setJsonEntity("""
            {
              "query": {
                "function_score": {
                  "query": { "match_all": {} },
                  "script_score": { "script": { "source": "%s" } }
                }
              }
            }
            """.formatted(script));
        return search;
    }
}
