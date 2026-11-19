/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that a remote reindex of an index whose total source size exceeds the REQUEST
 * circuit-breaker limit still completes, provided each batch on its own fits under the limit.
 *
 * <p>This is the positive-path counterpart to {@link ReindexFromRemoteCircuitBreakerIT} (which
 * covers the negative path: a single oversized response trips the breaker and aborts the
 * reindex without writing). Here we verify the opposite — that each batch's
 * {@code RemoteParseContext} reservation is released in time for the next batch's HTTP response
 * to be parsed without cumulative pressure. A leak in that release path would push us past the
 * limit within a few batches and surface a
 * {@link org.elasticsearch.common.breaker.CircuitBreakingException} mid-flight.
 */
public class ReindexFromRemoteCircuitBreakerReleaseIT extends ESSingleNodeTestCase {

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
            // Shrink the REQUEST breaker from its default down to a value where the test corpus is several times
            // larger than the limit but each batch fits comfortably. Without this override the corpus would fit
            // trivially and the assertion would prove nothing about release-between-batches.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "5mb")
            .build();
    }

    public void testReindexOfCorpusLargerThanBreakerLimitSucceedsViaPerBatchRelease() throws Exception {
        // 400 × 50 KiB ≈ 20 MiB total source = 4× the 5 MiB REQUEST limit configured in nodeSettings().
        // Batches of 25 (≈ 1.25 MiB source per batch) exceed cluster.reindex.memory_accounting_threshold
        // (1 MiB default) so per-hit accountHit() flushes mid-batch and buildBulk() flushes mid-loop.
        // Per-batch peak ≈ 2.5 MiB on the breaker (parsed hit-source bytes + BulkRequest bytes),
        // ≈ 0.5× the limit — comfortable headroom, but only if each batch's reservations release
        // before the next batch's HTTP response is parsed.
        final int numDocs = 400;
        final int docSizeBytes = 50_000;
        final int batchSize = 25;

        assertAcked(indicesAdmin().prepareCreate("dest"));
        assertAcked(
            indicesAdmin().prepareCreate("source").setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
        );
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("source").setId(Integer.toString(i)).setSource("data", randomAlphaOfLength(docSizeBytes)).get();
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
        request.setRefresh(true);
        // Force multiple batches by capping the per-batch size below numDocs.
        SearchRequest searchRequest = request.getSearchRequest();
        searchRequest.source(new SearchSourceBuilder().size(batchSize));

        BulkByPaginatedSearchResponse response = client().execute(ReindexAction.INSTANCE, request).get();

        assertThat("bulk failures: " + response.getBulkFailures(), response.getBulkFailures(), empty());
        assertThat("search failures: " + response.getSearchFailures(), response.getSearchFailures(), empty());
        assertThat(response.getCreated(), equalTo((long) numDocs));
        assertHitCount(client().prepareSearch("dest").setSize(0), numDocs);
    }
}
