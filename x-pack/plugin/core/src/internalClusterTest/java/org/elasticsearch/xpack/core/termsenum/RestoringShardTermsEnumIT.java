/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumResponse;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of {@code POST /{index}/_terms_enum}
 * against a shard being restored from a snapshot.
 *
 * <p>{@code _terms_enum} uses {@code TransportTermsEnumAction}, which fans out to individual shards
 * and calls {@code IndexShard.acquireSearcher()} directly. This path does NOT go through
 * {@code SearchReadyGate}, so requests do NOT park when the shard is INITIALIZING. The shard-level
 * call throws {@link org.elasticsearch.index.shard.IllegalIndexShardStateException}, but the
 * node-level failure is silently dropped by {@code TransportTermsEnumAction.onNodeFailure()}
 * (see the {@code TODO} comment in that method). As a result, the top-level response is HTTP 200
 * with zero total, successful, and failed shards — the failure is invisible to the caller.
 *
 * <p>This is distinct from {@code _search}, which parks via {@code SearchReadyGate}, and from
 * {@code _reindex}, which fails fast with 503.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardTermsEnumIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
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

    /**
     * {@code POST /{index}/_terms_enum} against a fully-restoring index returns HTTP 200 with
     * zero total, successful, and failed shards. Unlike {@code _search}, the terms-enum action
     * calls {@code IndexShard.acquireSearcher()} directly, bypassing {@code SearchReadyGate}.
     * The shard-level {@link org.elasticsearch.index.shard.IllegalIndexShardStateException} is
     * thrown on the data node, but {@code TransportTermsEnumAction.onNodeFailure()} silently drops
     * node-level failures (see the TODO comment in that method). The caller receives a response
     * that appears to cover zero shards.
     */
    public void testTermsEnumWhileRestoringReturns200WithSwallowedNodeFailure() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            TermsEnumRequest request = new TermsEnumRequest(INDEX).field("foo");
            TermsEnumResponse response = client().execute(TermsEnumAction.INSTANCE, request).actionGet();
            assertThat("node failure is silently dropped by onNodeFailure, so total shards is 0", response.getTotalShards(), equalTo(0));
            assertThat("node failure is silently dropped by onNodeFailure, so failed shards is 0", response.getFailedShards(), equalTo(0));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }
}
