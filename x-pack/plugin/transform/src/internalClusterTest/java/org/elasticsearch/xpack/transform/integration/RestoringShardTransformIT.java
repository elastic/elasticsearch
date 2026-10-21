/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.LocalStateTransform;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of a transform against a shard being
 * restored from a snapshot.
 *
 * <p>A batch transform's {@code _start} call parks rather than fails when its source shard is
 * INITIALIZING — see {@link #testBatchTransformSearchWhileRestoringParksRatherThanFails} for why
 * that happens during {@code _start} itself rather than the indexer's first search.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardTransformIT extends AbstractSnapshotIntegTestCase {

    private static final String SOURCE_INDEX = "test-restore-transform-source";
    private static final String REPO = "test-transform-repo";
    private static final String SNAPSHOT = "test-transform-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, LocalStateTransform.class, ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest, transform")
            .build();
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(SOURCE_INDEX);
        ensureGreen(SOURCE_INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(SOURCE_INDEX));
    }

    /**
     * {@code StartTransformAction} parks rather than fails when the transform's source shard is
     * INITIALIZING (being restored from snapshot).
     *
     * <p>{@code TransportStartTransformAction#masterOperation} always builds its own
     * {@code ValidateTransformAction.Request} with {@code deferValidation=false} (hard-coded,
     * independent of how the transform was created), so every {@code _start} call runs
     * {@code function.validateQuery()}/{@code deduceMappings()} against the source as a
     * pre-flight step, <strong>before</strong> the persistent task is created. That validation
     * search goes through {@code TransportSearchAction} and parks in {@code SearchReadyGate} just
     * like a plain {@code _search} against an INITIALIZING shard — so it is the {@code _start}
     * call itself that hangs, not (yet) the transform's indexer. Because the persistent task is
     * never created while this is outstanding, {@code GetTransformStats} reports the pre-start
     * {@code STOPPED} state throughout, not {@code INDEXING} or {@code FAILED}.
     *
     * <p>The {@code PutTransform} call below defers validation ({@code deferValidation=true}) so
     * that it isn't blocked by the same mechanism: {@code TransportPutTransformAction} only runs
     * the live validation search when {@code deferValidation=false}, and we want {@code PutTransform}
     * to persist the config immediately so {@code StartTransform} is the one under test.
     */
    public void testBatchTransformSearchWhileRestoringParksRatherThanFails() throws Exception {
        String transformId = "test-restoring-shard-transform";
        String destIndex = transformId + "-dest";

        blockAndStartRestore(REPO, SNAPSHOT, SOURCE_INDEX);
        ActionFuture<StartTransformAction.Response> startFuture = null;
        try {
            TransformConfig config = TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
                .setDest(new DestConfig(destIndex, null, null))
                .setLatestConfig(new LatestConfig(List.of("foo"), "foo"))
                .build();
            client().execute(PutTransformAction.INSTANCE, new PutTransformAction.Request(config, true, TimeValue.THIRTY_SECONDS))
                .actionGet(TimeValue.THIRTY_SECONDS);

            startFuture = client().execute(
                StartTransformAction.INSTANCE,
                new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS)
            );
            ActionFuture<StartTransformAction.Response> future = startFuture;
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));

            // No persistent task has been created yet (it's only created once the _start
            // pre-flight validation search above completes), so stats report the pre-start
            // STOPPED state rather than INDEXING or FAILED.
            GetTransformStatsAction.Response stats = client().execute(
                GetTransformStatsAction.INSTANCE,
                new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS);
            assertThat(stats.getTransformsStats().get(0).getState(), equalTo(TransformStats.State.STOPPED));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, SOURCE_INDEX);
            // Drain: once the repo is unblocked the parked validation search completes and
            // _start proceeds normally; consume the future so it isn't left outstanding.
            if (startFuture != null) {
                try {
                    startFuture.get(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }
            try {
                client().execute(
                    DeleteTransformAction.INSTANCE,
                    new DeleteTransformAction.Request(transformId, true, false, TimeValue.THIRTY_SECONDS)
                ).actionGet(TimeValue.THIRTY_SECONDS);
            } catch (Exception ignored) {}
        }
    }

    /**
     * {@code PutTransform} validation — triggered whenever {@code deferValidation=false} (the
     * REST default) — parks rather than fails when the transform's source shard is INITIALIZING.
     *
     * <p>{@code TransportPutTransformAction#masterOperation} only invokes
     * {@code ValidateTransformAction} when {@code deferValidation=false}. That pre-flight
     * validation runs {@code function.validateQuery()}/{@code deduceMappings()} against the
     * source, which goes through {@code TransportSearchAction} and parks in
     * {@code SearchReadyGate} just like a plain {@code _search} — and it happens
     * <strong>before the config is ever persisted</strong>: {@code TransportPutTransformAction
     * #putTransform} (which calls {@code TransformConfigManager#putTransformConfiguration}) only
     * runs once the whole validation chain succeeds. So while {@code PutTransform} is parked, no
     * transform config exists yet, and {@code GetTransformStats} returns an empty list rather
     * than any per-transform state — confirmed empirically (no exception, just {@code []}).
     *
     * <p>Cleanup drains the parked future but does not assert it succeeds:
     * {@code unblockAndDeleteRestoringIndex} deletes the source index immediately after
     * unblocking repository I/O, which cancels the in-flight restore out from under the parked
     * validation search and typically fails it (confirmed empirically) rather than letting it
     * complete — the same would happen to {@code _start}'s parked future in
     * {@link #testBatchTransformSearchWhileRestoringParksRatherThanFails} if it weren't drained
     * with the same ignore-on-cleanup pattern there.
     */
    public void testPutTransformValidationWhileRestoringParks() throws Exception {
        String transformId = "test-restoring-shard-transform-put";
        String destIndex = transformId + "-dest";

        blockAndStartRestore(REPO, SNAPSHOT, SOURCE_INDEX);
        ActionFuture<AcknowledgedResponse> putFuture = null;
        try {
            TransformConfig config = TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
                .setDest(new DestConfig(destIndex, null, null))
                .setLatestConfig(new LatestConfig(List.of("foo"), "foo"))
                .build();

            putFuture = client().execute(
                PutTransformAction.INSTANCE,
                new PutTransformAction.Request(config, false, TimeValue.THIRTY_SECONDS)
            );
            ActionFuture<AcknowledgedResponse> future = putFuture;
            // this blocks because the SOURCE_INDEX can't recover because the repo is blocked
            expectThrows(TimeoutException.class, () -> future.get(randomIntBetween(50, 500), TimeUnit.MILLISECONDS));

            // The put creates the internal index itself, so its primary may still be initializing here, and searching it
            // by the GetTransformStatsAction bellow would fail with NoShardAvailableActionException.
            assertBusy(() -> {
                assertTrue(indexExists(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME));
                ensureGreen(TransformInternalIndexConstants.LATEST_INDEX_VERSIONED_NAME);
            });

            // No transform config has been persisted yet (it's only written once the validation
            // search above completes), so stats report no transforms at all.
            GetTransformStatsAction.Response stats = client().execute(
                GetTransformStatsAction.INSTANCE,
                new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS);
            assertThat(stats.getTransformsStats(), empty());
        } finally {
            unblockAndDeleteRestoringIndex(REPO, SOURCE_INDEX);
            // Drain: deleting the source index above cancels the in-flight restore, so the
            // parked validation search typically fails rather than succeeding once released —
            // we only care that it's no longer outstanding, not how it resolves.
            if (putFuture != null) {
                try {
                    putFuture.get(30, TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }
            try {
                client().execute(
                    DeleteTransformAction.INSTANCE,
                    new DeleteTransformAction.Request(transformId, true, false, TimeValue.THIRTY_SECONDS)
                ).actionGet(TimeValue.THIRTY_SECONDS);
            } catch (Exception ignored) {}
        }
    }
}
