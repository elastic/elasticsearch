/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that reindex-from-remote charges the REQUEST circuit breaker for the bytes of a parsed
 * response and surfaces a {@link CircuitBreakingException} when the response exceeds the limit —
 * without writing any documents to the destination.
 *
 * <p>Hits are accumulated locally and flushed to the breaker once {@code
 * cluster.reindex.memory_accounting_threshold} is crossed (1 MiB default, also the minimum the
 * setting allows). For modest batches like this one (5 docs × ~20 KiB ≈ 100 KiB total) the trip
 * happens on the final {@code flushRemaining} call rather than mid-batch; the per-hit mechanics
 * (incremental accumulation, mid-flush trip, release on close) are covered by unit tests in
 * {@code RemoteParseContextTests}.
 *
 * <p>Uses {@link ESSingleNodeTestCase} so that shard and coordinator always run on the same node.
 * This avoids cross-node transport serialization (which would use {@code RecyclerBytesStreamOutput}
 * and trip the breaker server-side before the HTTP response reaches our client-side tracking).
 *
 * <p>Doc / batch sizing is intentionally kept small enough that the source-side {@code FetchPhase}
 * does not trip the same REQUEST breaker on its own — we want the assertion below to be about the
 * remote-response label specifically.
 */
public class ReindexFromRemoteCircuitBreakerIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ReindexPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            // Sized above the version-lookup (~600 B) and open-PIT (~700 B) responses — both are
            // immediately released — but below the first remote search response (≈ 100 KiB for
            // 5 × 20 KiB random-alpha docs after JSON encoding) so the breaker trips when the
            // RemoteParseContext flushes its accumulated bytes at the end of parsing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "5kb")
            .build();
    }

    public void testCircuitBreakerTripsOnOversizedRemoteSearchResponse() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("dest"));

        // Single node: shard and coordinator are co-located, so the search response travels via
        // DirectResponseChannel (in-memory, no RecyclerBytesStreamOutput serialization). The 5 KiB
        // REQUEST limit is therefore not tripped by shard serialization, only by our remote-response
        // accounting in RemoteParseContext.
        assertAcked(
            indicesAdmin().prepareCreate("source").setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
        );
        int numDocs = 5;
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", randomAlphaOfLength(20_000)).get();
        }
        indicesAdmin().prepareRefresh("source").get();

        InetSocketAddress remoteAddress = node().injector()
            .getInstance(org.elasticsearch.http.HttpServerTransport.class)
            .boundAddress()
            .publishAddress()
            .address();
        RemoteInfo remote = new RemoteInfo(
            "http",
            remoteAddress.getHostString(),
            remoteAddress.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );

        ReindexRequest request = new ReindexRequest().setSourceIndices("source");
        request.setDestIndex("dest");
        request.setRemoteInfo(remote);

        ExecutionException thrown = expectThrows(ExecutionException.class, () -> client().execute(ReindexAction.INSTANCE, request).get());
        Throwable circuitBreakingCause = ExceptionsHelper.unwrap(thrown, CircuitBreakingException.class);
        assertThat("expected CircuitBreakingException in cause chain, got: " + thrown, circuitBreakingCause, notNullValue());
        assertThat(circuitBreakingCause.getMessage(), containsString("reindex_remote_response"));

        // No documents should have been written to the destination — the breaker trip aborts the
        // batch before any bulk request is sent.
        assertHitCount(client().prepareSearch("dest").setSize(0), 0);
    }
}
