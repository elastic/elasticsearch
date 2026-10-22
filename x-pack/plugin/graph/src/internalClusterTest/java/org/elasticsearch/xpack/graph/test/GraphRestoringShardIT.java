/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.graph.test;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.graph.action.GraphExploreRequestBuilder;
import org.elasticsearch.xpack.graph.Graph;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Integration tests documenting the current behaviour of {@code GET /{index}/_graph/explore}
 * against a shard being restored from a snapshot.
 *
 * <p>{@code _graph/explore} works by issuing a series of {@code SearchRequest}s internally
 * ({@code TransportGraphExploreAction} orchestrates iterative hops). The first search leg goes
 * through {@code TransportSearchAction} → {@code SearchService.rewriteAndFetchShardRequest},
 * which parks in {@code SearchReadyGate} when the shard is INITIALIZING. The outer graph explore
 * future therefore also hangs rather than returning a 503.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are required,
 * which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class GraphRestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, Graph.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
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
    // Graph explore — parks on first search leg (routes through TransportSearchAction)
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_graph/explore} internally issues iterative {@code SearchRequest}s via
     * {@code TransportGraphExploreAction}. The first search leg goes through
     * {@code TransportSearchAction} → {@code SearchService.rewriteAndFetchShardRequest}, which
     * parks in {@code SearchReadyGate} when the shard is INITIALIZING. The outer graph explore
     * future hangs rather than returning an error immediately.
     */
    public void testGraphExploreWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        GraphExploreRequestBuilder builder = new GraphExploreRequestBuilder(client()).setIndices(INDEX);
        builder.createNextHop(null).addVertexRequest("field").minDocCount(1);
        var future = client().execute(GraphExploreAction.INSTANCE, builder.request());
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }
}
