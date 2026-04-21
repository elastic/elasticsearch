/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class TranslogCircuitBreakerIT extends ESIntegTestCase {

    @After
    public void resetSettings() {
        updateClusterSettings(Settings.builder().putNull(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey()));
    }

    /// Verifies end-to-end that a REQUEST circuit breaker trip inside `TranslogWriter`'s write buffer
    /// fails the write request, closes the translog writer with a tragic event, fails the engine
    /// (when the next write encounters the closed translog), and that the shard recovers
    /// successfully because the on-disk translog was not corrupted.
    public void testTranslogWriteTripsCircuitBreakerAndShardRecovers() throws Exception {
        assumeFalse("circuit breaker disabled, skipping test", circuitBreakerDisabled());

        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 0)));

        prepareIndex("test").setId("1").setSource("field", "value").get();
        indicesAdmin().prepareFlush("test").setForce(true).get();

        final long cbTripsBeforeWrite = requestBreakerTrippedCount();
        final long primaryTermBefore = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index("test")
            .primaryTerm(0);

        // The outer serialization buffer (1 page) and the TranslogWriter write buffer (1 page) will be created
        // successfully, but writing a large source into the write buffer requires a third page, tripping the breaker.
        updateClusterSettings(
            Settings.builder()
                .put(
                    HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(),
                    ByteSizeValue.ofBytes(3L * PageCacheRecycler.BYTE_PAGE_SIZE).getStringRep()
                )
        );

        try {
            prepareIndex("test").setId("2").setSource("field", "x".repeat(2 * PageCacheRecycler.BYTE_PAGE_SIZE)).get();
            fail("expected the index request to fail due to the circuit breaker trip in the translog write buffer");
        } catch (final Exception ignored) {
            // Expected
        }

        assertThat(requestBreakerTrippedCount(), greaterThan(cbTripsBeforeWrite));

        // For PRIMARY writes, InternalEngine does not call failEngine directly on the CB exception
        // (treatDocumentFailureAsTragicError returns false). The engine fails when the next write
        // encounters AlreadyClosedException.
        try {
            prepareIndex("test").setId("3").setSource("field", "value").get();
        } catch (final Exception ignored) {
            // Expected
        }

        // The on-disk translog is intact (the CB trip was in the in-memory write buffer, not yet
        // flushed to disk). The primary term is incremented when the failed primary is re-allocated,
        // and once re-allocated the cluster returns to GREEN.
        assertBusy(() -> {
            assertThat(
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                    .get()
                    .getState()
                    .metadata()
                    .getProject()
                    .index("test")
                    .primaryTerm(0),
                greaterThan(primaryTermBefore)
            );
            assertThat(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, "test").get().getStatus(),
                equalTo(ClusterHealthStatus.GREEN)
            );
        });
    }

    private boolean circuitBreakerDisabled() {
        final NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        for (final NodeStats nodeStats : stats.getNodes()) {
            if (nodeStats.getBreaker().getStats(CircuitBreaker.REQUEST).getLimit() == NoopCircuitBreaker.LIMIT) {
                return true;
            }
        }
        return false;
    }

    private long requestBreakerTrippedCount() {
        long count = 0;
        final NodesStatsResponse stats = clusterAdmin().prepareNodesStats().setBreaker(true).get();
        for (final NodeStats stat : stats.getNodes()) {
            count += stat.getBreaker().getStats(CircuitBreaker.REQUEST).getTrippedCount();
        }
        return count;
    }
}
