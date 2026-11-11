/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.fleet.Fleet;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Integration tests documenting the current behaviour of
 * {@code GET /{index}/_fleet/global_checkpoints} against a shard being restored from a snapshot.
 *
 * <p>{@code GetGlobalCheckpointsAction} inspects cluster routing state rather than reading shard data.
 * When the target shard is INITIALIZING (routing state; internal {@code IndexShardState} is {@code RECOVERING},
 * and the shard is not yet STARTED):
 * <ul>
 *   <li>With {@code wait_for_index=false}: fails immediately with {@code UnavailableShardsException} (HTTP 503).</li>
 *   <li>With {@code wait_for_index=true}: parks via {@code ClusterStateObserver} until timeout, then also
 *       fails with {@code UnavailableShardsException} (HTTP 503).</li>
 * </ul>
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class FleetRestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";
    private static final long[] EMPTY_CHECKPOINTS = new long[0];

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, Fleet.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
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
     * {@code GET /{index}/_fleet/global_checkpoints} with {@code wait_for_index=false} fails immediately
     * with HTTP 503 ({@code UnavailableShardsException}) when the primary is INITIALIZING. The action
     * inspects cluster routing state and rejects immediately when primaries are not active.
     */
    public void testGetGlobalCheckpointsWithoutWaitWhileRestoringReturnsServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
                INDEX,
                false,
                false,
                EMPTY_CHECKPOINTS,
                TimeValue.timeValueSeconds(1)
            );
            Exception e = expectThrows(
                Exception.class,
                () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet(TimeValue.timeValueSeconds(5))
            );
            assertThat(e.getClass(), equalTo(UnavailableShardsException.class));
            assertThat(((UnavailableShardsException) e).status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code GET /{index}/_fleet/global_checkpoints} with {@code wait_for_index=true} parks via
     * {@code ClusterStateObserver} until the timeout elapses, then fails with HTTP 503
     * ({@code UnavailableShardsException}). The request never returns success while the shard is
     * INITIALIZING. {@code wait_for_advance} must also be true when {@code wait_for_index}
     * is true (API validation requirement).
     */
    public void testGetGlobalCheckpointsWithWaitWhileRestoringParksUntilTimeout() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
                INDEX,
                true,
                true,
                EMPTY_CHECKPOINTS,
                TimeValue.timeValueMillis(200)
            );
            Exception e = expectThrows(
                Exception.class,
                () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet(TimeValue.timeValueSeconds(30))
            );
            assertThat(e, instanceOf(UnavailableShardsException.class));
            assertThat(((UnavailableShardsException) e).status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    // -------------------------------------------------------------------------
    // Fleet multi-search (_fleet/_fleet_msearch) — parks like _msearch
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_fleet/_fleet_msearch} delegates to {@code TransportMultiSearchAction},
     * which fans out to {@code TransportSearchAction} per sub-request. Each sub-request goes through
     * {@code SearchService.rewriteAndFetchShardRequest} and parks in {@code SearchReadyGate} when
     * the shard is INITIALIZING. The outer msearch future also hangs.
     */
    public void testFleetMsearchWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        MultiSearchRequest msearchRequest = new MultiSearchRequest().add(new SearchRequest(INDEX));
        var future = client().execute(TransportMultiSearchAction.TYPE, msearchRequest);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }

    // -------------------------------------------------------------------------
    // Fleet search with wait_for_checkpoints — parks like _search
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_fleet/_fleet_search?wait_for_checkpoints=0} sets
     * {@code SearchRequest.waitForCheckpoints} and delegates to {@code TransportSearchAction}.
     * The search path still goes through {@code SearchService.rewriteAndFetchShardRequest} and
     * parks in {@code SearchReadyGate} when the shard is INITIALIZING. The fleet checkpoint gate
     * is reached only after the shard becomes searchable, so the request hangs on the search
     * gate rather than the checkpoint gate.
     */
    public void testFleetSearchWithCheckpointsWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.setWaitForCheckpoints(Collections.singletonMap(INDEX, new long[] { 0L }));
        searchRequest.setWaitForCheckpointsTimeout(TimeValue.timeValueMillis(500));
        var future = client().execute(TransportSearchAction.TYPE, searchRequest);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(10, TimeUnit.SECONDS).decRef();
            } catch (Exception ignored) {}
        }
    }
}
