/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of ES|QL queries against a shard
 * being restored from a snapshot.
 *
 * <p>Unlike {@code _search}, ES|QL calls {@code SearchService.createSearchContext()} rather than
 * {@code rewriteAndFetchShardRequest()}, which means it bypasses {@code SearchReadyGate} entirely.
 * When the target shard is INITIALIZING (routing state; internal {@code IndexShardState} is {@code RECOVERING}),
 * {@code IndexShard.acquireExternalSearcherSupplier()} throws
 * {@code IllegalIndexShardStateException} immediately. This surfaces as HTTP 404 (the status
 * reported by {@link org.elasticsearch.index.shard.IllegalIndexShardStateException#status()}).
 * The request fails fast rather than parking.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class EsqlRestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, TestEncryptionServicePlugin.class, EsqlPluginWithEnterpriseOrTrialLicense.class);
    }

    /**
     * Creates the repository and snapshot once for the entire suite. Each test restores from this
     * snapshot. The index is deleted (not closed) so repo reads are required during recovery, which
     * is what {@code blockAllDataNodes} actually blocks.
     */
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    /**
     * An ES|QL {@code _query/async} request against a fully-restoring index fails fast with
     * HTTP 404 ({@code IllegalIndexShardStateException}). The async submission path uses the
     * same underlying execution as the sync path and therefore also bypasses
     * {@code SearchReadyGate}. Because the query fails immediately, the failure is returned
     * in the initial response rather than as a deferred async result.
     */
    public void testAsyncEsqlQueryWhileRestoringFailsFastWithNotFound() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            EsqlQueryRequest request = asyncEsqlQueryRequest("FROM " + INDEX + " | LIMIT 1");
            request.waitForCompletionTimeout(TimeValue.timeValueSeconds(10));
            Exception e = expectThrows(
                Exception.class,
                () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet(TimeValue.timeValueSeconds(30))
            );
            assertThat(ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)), equalTo(RestStatus.NOT_FOUND));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * An ES|QL {@code FROM} query against a fully-restoring index fails fast with HTTP 404
     * ({@code IllegalIndexShardStateException}). Unlike {@code _search}, it does NOT park in
     * {@code SearchReadyGate} — the query fails immediately when the shard context cannot be
     * acquired.
     */
    public void testEsqlQueryWhileRestoringFailsFastWithNotFound() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + INDEX + " | LIMIT 1");
            Exception e = expectThrows(
                Exception.class,
                () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet(TimeValue.timeValueSeconds(30))
            );
            assertThat(ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)), equalTo(RestStatus.NOT_FOUND));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }
}
