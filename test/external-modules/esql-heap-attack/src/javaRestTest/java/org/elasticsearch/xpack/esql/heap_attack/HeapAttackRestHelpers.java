/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Shared REST-level helpers for heap-attack tests: scaling circuit-breaker assertion loops,
 * the query / runQuery pair with the in-flight {@code /_trigger_out_of_memory} backstop, and
 * the request-breaker liveness probe. Holds no {@code @ClassRule}, so subclasses can declare
 * whatever cluster setup they need (single internal cluster for the original suites; an S3
 * fixture rule chained with a datasource-enabled cluster for {@link HeapAttackExternalIT}).
 */
public abstract class HeapAttackRestHelpers extends ESRestTestCase {

    protected static final int MAX_ATTEMPTS = 5;

    @Override
    protected boolean shouldFailureSkipRemainingTests() {
        /*
         * Failures will frequently poison the cluster being tested, causing the next
         * test to fail because the cluster has OOMed or exploded in some fun way. So
         * we skip them.
         */
        return true;
    }

    protected interface TryCircuitBreaking {
        Map<String, Object> attempt(int attempt) throws IOException;
    }

    protected void assertCircuitBreaks(TryCircuitBreaking tryBreaking) throws IOException {
        assertCircuitBreaks(
            tryBreaking,
            matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
        );
    }

    protected void assertCircuitBreaks(TryCircuitBreaking tryBreaking, MapMatcher responseMatcher) throws IOException {
        int attempt = 1;
        while (attempt <= MAX_ATTEMPTS) {
            try {
                Map<String, Object> response = tryBreaking.attempt(attempt);
                logger.warn("{}: should have circuit broken but got {}", attempt, response);
                attempt++;
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                assertMap(map, responseMatcher);
                return;
            }
        }
        fail("giving up circuit breaking after " + MAX_ATTEMPTS + " attempts");
    }

    /**
     * Asserts that the given operation eventually trips the circuit breaker via the expected code
     * path, as confirmed by all {@code classes} appearing in the exception's stack trace.
     * <p>
     * Unlike {@link #assertCircuitBreaks} which stops on the first circuit-breaking exception
     * regardless of origin, this method continues to the next attempt if the exception came from
     * a different part of the pipeline (e.g. an upstream operator tripping before the operator
     * under test). Each attempt scales the load, so a later attempt is more likely to reach the
     * operator under test.
     */
    protected void assertCircuitBreaksVia(TryCircuitBreaking tryBreaking, String... classNames) throws IOException {
        List<String> expected = Arrays.asList(classNames);
        int attempt = 1;
        while (attempt <= MAX_ATTEMPTS) {
            logger.info("Attempt {} to circuit break via {}", attempt, expected);
            try {
                Map<String, Object> response = tryBreaking.attempt(attempt);
                logger.warn("{}: should have circuit broken but got {}", attempt, response);
            } catch (ResponseException e) {
                Map<?, ?> map = responseAsMap(e.getResponse());
                Object error = map.get("error");
                if (error instanceof Map<?, ?> errorMap
                    && "circuit_breaking_exception".equals(errorMap.get("type"))
                    && errorMap.get("stack_trace") instanceof String stackTrace) {
                    if (expected.stream().allMatch(stackTrace::contains)) {
                        assertMap(
                            map,
                            matchesMap().entry("status", 429)
                                .entry("error", matchesMap().extraOk().entry("type", "circuit_breaking_exception"))
                        );
                        return;
                    } else {
                        logger.warn(
                            "{}: circuit broke but not via expected classes {}. The stacktrace was: {}",
                            attempt,
                            expected,
                            stackTrace
                        );
                    }
                }
            }
            attempt++;
        }
        fail("giving up after " + (attempt - 1) + " attempts waiting for circuit break via " + expected);
    }

    protected Response query(String query, String filterPath) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("error_trace", "");
        if (filterPath != null) {
            request.addParameter("filter_path", filterPath);
        }
        request.setJsonEntity(query.replace("\n", "\\n"));
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(6).millis())).build())
                .setWarningsHandler(WarningsHandler.PERMISSIVE)
        );
        logger.info("Running query: {}", query);
        return runQuery(() -> client().performRequest(request));
    }

    protected Response runQuery(CheckedSupplier<Response, IOException> run) throws IOException {
        logger.info("--> test {} started querying", getTestName());
        final ThreadPool testThreadPool = new TestThreadPool(getTestName());
        final long startedTimeInNanos = System.nanoTime();
        Scheduler.Cancellable schedule = null;
        try {
            schedule = testThreadPool.schedule(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() throws Exception {
                    TimeValue elapsed = TimeValue.timeValueNanos(System.nanoTime() - startedTimeInNanos);
                    logger.info("--> test {} triggering OOM after {}", getTestName(), elapsed);
                    Request triggerOOM = new Request("POST", "/_trigger_out_of_memory");
                    client().performRequest(triggerOOM);
                }
            }, TimeValue.timeValueMinutes(5), testThreadPool.executor(ThreadPool.Names.GENERIC));
            Response resp = run.get();
            logger.info("--> test {} completed querying", getTestName());
            return resp;
        } finally {
            if (schedule != null) {
                schedule.cancel();
            }
            terminate(testThreadPool);
        }
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        settings = Settings.builder().put(settings).put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "6m").build();
        return super.buildClient(settings, hosts);
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        if (previousFailureSkipsRemaining()) {
            return;
        }
        final long allowedBytes = allowedRequestBreakerBaselineBytes();
        assertBusy(() -> {
            Response response = adminClient().performRequest(new Request("GET", "/_nodes/stats"));
            Map<?, ?> stats = responseAsMap(response);
            Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");
            for (Object n : nodes.values()) {
                Map<?, ?> node = (Map<?, ?>) n;
                Map<?, ?> breakers = (Map<?, ?>) node.get("breakers");
                Map<?, ?> request = (Map<?, ?>) breakers.get("request");
                if (allowedBytes == 0L) {
                    assertMap(request, matchesMap().extraOk().entry("estimated_size_in_bytes", 0).entry("estimated_size", "0b"));
                } else {
                    Number sizeInBytes = (Number) request.get("estimated_size_in_bytes");
                    if (sizeInBytes.longValue() > allowedBytes) {
                        throw new AssertionError(
                            "request breaker still holding " + sizeInBytes + " bytes (>" + allowedBytes + ") after test"
                        );
                    }
                }
            }
        });
    }

    /**
     * Maximum request-breaker bytes the parent's @Before/@After probe will tolerate at idle.
     * Defaults to {@code 0}, matching the strict expectation of the FROM-based heap-attack tests
     * (which run against a cluster that holds no baseline allocations on the request breaker).
     * The EXTERNAL variant overrides this because its datasource plugins maintain small steady-
     * state allocations on the request breaker even between queries.
     */
    protected long allowedRequestBreakerBaselineBytes() {
        return 0L;
    }

    protected void setRequestBreakerLimit(String limit) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"indices.breaker.request.limit\": " + (limit == null ? "null" : "\"" + limit + "\"") + "}}"
        );
        adminClient().performRequest(request);
    }

    protected static boolean isServerless() throws IOException {
        for (Map<?, ?> nodeInfo : getNodesInfo(adminClient()).values()) {
            for (Object module : (List<?>) nodeInfo.get("modules")) {
                Map<?, ?> moduleInfo = (Map<?, ?>) module;
                final String moduleName = moduleInfo.get("name").toString();
                if (moduleName.startsWith("serverless-")) {
                    return true;
                }
            }
        }
        return false;
    }

    protected static StringBuilder startQuery() {
        StringBuilder query = new StringBuilder();
        query.append("{\"query\":\"");
        return query;
    }
}
