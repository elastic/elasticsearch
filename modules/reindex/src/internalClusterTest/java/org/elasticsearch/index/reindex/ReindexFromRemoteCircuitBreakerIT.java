/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.RestStatus;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that reindex-from-remote fails with a circuit-breaker error when the REQUEST breaker
 * limit is too small to accommodate the response data, without writing any documents to the
 * destination.
 *
 * <p>With fetch accounting enabled, the REQUEST circuit breaker is charged by {@code FetchPhase}
 * for {@code fetch[source]} bytes before the HTTP response is returned to the reindex coordinator.
 * In a single-node setup the FetchPhase and {@code RemoteParseContext} share the same breaker, so
 * FetchPhase always trips first — the coordinator receives an HTTP 429 rather than accumulating
 * bytes in {@code RemoteParseContext}.
 *
 * <p>The per-hit mechanics of {@code RemoteParseContext} (incremental accumulation, mid-flush
 * trip, release on close) are covered by unit tests in {@code RemoteParseContextTests}.
 *
 * <p>Uses {@link ESSingleNodeTestCase} so that shard and coordinator always run on the same node,
 * avoiding cross-node transport serialization (which would use {@code RecyclerBytesStreamOutput}
 * and could introduce its own breaker charges).
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
            // immediately released — but below the fetch[source] charge (~100 KiB for 5 × 20 KiB
            // random-alpha docs) so the breaker trips during FetchPhase before the search response
            // is returned.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "5kb")
            .build();
    }

    public void testCircuitBreakerTripsOnOversizedRemoteSearchResponse() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("dest"));

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

        // FetchPhase charges fetch[source] (~100 KiB for 5 × 20 KiB docs) to the REQUEST breaker
        // before the HTTP response is returned. The 5 KiB limit is exceeded server-side, so the
        // coordinator receives HTTP 429 wrapped in ElasticsearchStatusException rather than a local
        // CircuitBreakingException from RemoteParseContext.
        assertThat(thrown.getCause(), instanceOf(ElasticsearchStatusException.class));
        ElasticsearchStatusException statusException = (ElasticsearchStatusException) thrown.getCause();
        assertThat(statusException.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(statusException.getMessage(), containsString("circuit_breaking_exception"));
        assertThat(statusException.getMessage(), containsString("fetch[source]"));

        // No documents should have been written to the destination — the breaker trip aborts the
        // batch before any bulk request is sent.
        assertHitCount(client().prepareSearch("dest").setSize(0), 0);
    }
}
