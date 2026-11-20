/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of
 * {@code GET|POST /_scripts/painless/_execute} with an index context against a shard being
 * restored from a snapshot.
 *
 * <p>{@code PainlessExecuteAction.TransportAction} extends {@code TransportSingleShardAction}.
 * When an index context is provided, {@code shards()} uses
 * {@code randomAllActiveShardsIt()}, which returns only STARTED shards.
 * While the primary is INITIALIZING (routing state; internal {@code IndexShardState} is
 * {@code RECOVERING}), the iterator is empty and the action fails immediately with
 * {@link NoShardAvailableActionException} (HTTP 503).
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are required,
 * which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardPainlessExecuteIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, PainlessPlugin.class);
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
     * {@code POST /_scripts/painless/_execute} with an index context fails immediately with
     * {@link NoShardAvailableActionException} (HTTP 503) when the primary is INITIALIZING.
     * {@code PainlessExecuteAction.TransportAction.shards()} calls
     * {@code randomAllActiveShardsIt()}, which excludes INITIALIZING shards, so the shard
     * iterator is empty and the action fails before reaching any script execution.
     */
    public void testPainlessExecuteWithIndexContextWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            BytesReference doc = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject());
            var contextSetup = new PainlessExecuteAction.Request.ContextSetup(INDEX, doc, new MatchAllQueryBuilder());
            contextSetup.setXContentType(XContentType.JSON);
            var request = new PainlessExecuteAction.Request(new Script("true"), FilterScript.CONTEXT.name, contextSetup);
            Exception e = expectThrows(Exception.class, () -> client().execute(PainlessExecuteAction.INSTANCE, request).actionGet());
            assertThat(ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }
}
