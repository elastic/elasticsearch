/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.heappressure;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Wildcard queries on a {@code wildcard} field must be accounted by the request circuit breaker.
 *
 * <p>A wildcard pattern is bounded by the fixed determinization work limit (10,000), so a single one allocates about 50 MB and
 * retains about 10 MB per shard. That cannot exhaust a normal heap, so the cluster runs with a deliberately small one, where the
 * automatons of a few overlapping requests do. Charged to the breaker, the excess requests are rejected; uncharged, the node runs
 * out of memory.
 *
 * <p>Both tests run the node out of memory with {@code VALIDATE_OOM} set in {@link WildcardFieldHeapPressureTestCase}.
 */
public class WildcardFieldWildcardHeapPressureIT extends WildcardFieldHeapPressureTestCase {

    // '*a' followed by N '?' determinizes to 2^N states: 8192 for N=11, the most the default 10,000 work limit accepts. Each letter
    // occurs in the indexed values "value-N-abcabcabc" with at least 11 characters after it, so every pattern matches every document
    // and a shard filtered on the pause field always reaches it. Distinct patterns build distinct automatons.
    private static final List<String> HEAVY_WILDCARDS = List.of(
        "*v???????????*",
        "*a???????????*",
        "*l???????????*",
        "*u???????????*",
        "*e???????????*"
    );
    private static final String SMALL_WILDCARD = "*a????*";
    // The REST client's default number of connections per route.
    private static final int CLIENT_CONNECTIONS = 10;

    /**
     * A burst of concurrent heavy requests either completes or is rejected by a breaker, never anything else, and the node survives it.
     * The number of requests and clauses is random on purpose, so that different runs overlap the automaton builds and their retention
     * differently: how many requests are rejected depends on that and is only logged.
     */
    public void testBurstOfHeavyWildcardsDoesNotRunOutOfMemory() throws Exception {
        /* A pattern that fits comfortably under the breaker is unaffected. */
        assertNull(errorBodyOrNull(wildcardSearch(SMALL_WILDCARD)));

        // More requests than client connections, so that some are sent while others are still building or finishing.
        int requestCount = randomIntBetween(CLIENT_CONNECTIONS, 3 * CLIENT_CONNECTIONS);
        // Picked here because the randomness of the test isn't available on the threads that send the requests. A burst of single clause
        // requests doesn't always run the node out of memory without the breakers, and then wouldn't show that they are what saves it.
        String[] patterns = randomSubsetOf(randomIntBetween(2, 3), HEAVY_WILDCARDS).toArray(String[]::new);
        List<Future<Map<String, Object>>> futures = submit(requestCount, () -> errorBodyOrNull(wildcardSearch(patterns)));

        int rejected = 0;
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body != null) {
                assertTrue("expected only circuit_breaking_exception failures, but got: " + body, containsCircuitBreakingException(body));
                rejected++;
            }
        }
        // We do not check here exactly how many are rejected, since it may depend on timing, but we ensure that the node survives.
        logger.info(
            "[{}] of [{}] heavy requests with [{}] clauses were rejected by a circuit breaker",
            rejected,
            requestCount,
            patterns.length
        );
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * Automatons retained by shards held in flight stay charged to the request breaker until their requests finish, and that charge is
     * what rejects further shards. All requests are sent at once, and every one of their shards waits at the admission gate of the
     * pause field before building its query. Shards are then admitted one at a time, each only once the previous one has either parked
     * holding its automaton or been rejected, so only one build reservation is ever held and what fills the breaker is the automatons
     * of the parked shards. Which shards park depends on the order they are admitted in, so the requests mostly return partial results
     * and the rejections are counted per shard.
     */
    public void testRetainedHeavyWildcardsTripRequestBreaker() throws Exception {
        // Five shards each, enough to run the node out of memory if the retained automatons weren't charged, and few enough that all
        // their shards fit in the search pool and wait at the gate at the same time.
        final int requestCount = 5;
        final int shardCount = requestCount * SHARDS;
        // All the heavy patterns retain the same, so which one doesn't change the outcome.
        final String pattern = randomFrom(HEAVY_WILDCARDS);
        // The automaton of a shard retains about 8 MB. Uncharged, a parked shard adds next to nothing.
        final long minRetainedBytes = ByteSizeValue.ofMb(5).getBytes();

        stopPauseField();
        List<Future<Integer>> futures = submit(requestCount, () -> circuitBreakerRejectedShards(pausableWildcardSearch(pattern), SHARDS));
        assertBusy(
            () -> assertEquals("expected every shard to wait at the admission gate", shardCount, gatedCount()),
            30,
            TimeUnit.SECONDS
        );

        int parkedShards = 0;
        int trippedShards = 0;
        for (int i = 0; i < shardCount; i++) {
            int parkedBefore = parkedCount();
            long trippedBefore = trippedCount();
            long chargedBefore = requestBreakerUsedBytes();
            admitOne();
            assertBusy(
                () -> assertTrue(
                    "expected the admitted shard to park or be rejected",
                    parkedCount() > parkedBefore || trippedCount() > trippedBefore
                ),
                30,
                TimeUnit.SECONDS
            );
            if (parkedCount() > parkedBefore) {
                long charged = requestBreakerUsedBytes() - chargedBefore;
                assertThat("expected the automaton of a parked shard to stay charged", charged, greaterThanOrEqualTo(minRetainedBytes));
                parkedShards++;
            } else {
                trippedShards++;
            }
        }
        assertEquals("expected no shard left at the admission gate", 0, gatedCount());
        logger.info(
            "[{}] shards parked holding their automatons, the request breaker rejected the other [{}]",
            parkedShards,
            trippedShards
        );

        unblockPauseField();
        int rejectedShards = 0;
        for (Future<Integer> future : futures) {
            rejectedShards += future.get();
        }
        assertThat("expected some of the shards to park and hold their automatons", parkedShards, greaterThan(0));
        assertThat("expected the retained automatons to fill the request breaker", trippedShards, greaterThan(0));
        assertEquals("expected every rejected shard to be reported as a circuit breaker failure", trippedShards, rejectedShards);
        assertNodeAlive();
    }
}
