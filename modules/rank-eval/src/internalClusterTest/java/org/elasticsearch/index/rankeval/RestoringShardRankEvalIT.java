/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Integration tests documenting the current behaviour of {@code GET /{index}/_rank_eval}
 * against a shard being restored from a snapshot.
 *
 * <p>{@code _rank_eval} executes a series of {@code SearchRequest}s internally and therefore goes
 * through the same {@code SearchService.rewriteAndFetchShardRequest} path as {@code _search}.
 * When the target shard is INITIALIZING, each sub-search parks in {@code SearchReadyGate} and the
 * outer rank-eval request also hangs indefinitely.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are required,
 * which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardRankEvalIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, RankEvalPlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    // -------------------------------------------------------------------------
    // Rank eval — parks like _search (delegates to TransportSearchAction per query)
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_rank_eval} internally issues a {@code SearchRequest} per rated query.
     * Each sub-search goes through {@code SearchService.rewriteAndFetchShardRequest}, which calls
     * {@code IndexShard.waitForSearchReady()} and parks the request when the shard is INITIALIZING.
     * The outer rank-eval future therefore also hangs rather than returning a 503.
     */
    public void testRankEvalWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        RatedRequest ratedRequest = new RatedRequest(
            "test_query",
            List.of(new RatedDocument(INDEX, "some_id", 1)),
            new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
        );
        RankEvalSpec spec = new RankEvalSpec(List.of(ratedRequest), new PrecisionAtK());
        RankEvalRequest request = new RankEvalRequest(spec, new String[] { INDEX });
        var future = client().execute(RankEvalPlugin.ACTION, request);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try (var response = future.get(10, TimeUnit.SECONDS)) {
                // release ref-counted search hits held by RankEvalResponse
            } catch (Exception ignored) {}
        }
    }
}
