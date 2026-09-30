/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

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
import org.elasticsearch.xpack.transform.LocalStateTransform;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests documenting the current behaviour of a transform against a shard being
 * restored from a snapshot.
 *
 * <p>A batch transform's initial search goes through {@code TransportSearchAction} and parks in
 * {@code SearchReadyGate} when the target shard is INITIALIZING. The transform task stays in
 * {@code STARTED} or {@code INDEXING} state — it does not fail — while the repository is blocked.
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
     * A batch transform whose source shard is INITIALIZING (being restored from snapshot) parks
     * its initial search in {@code SearchReadyGate}. The transform task transitions to
     * {@code INDEXING} state — the search is outstanding but has not failed — rather than
     * immediately becoming {@code FAILED}.
     */
    public void testBatchTransformSearchWhileRestoringParksRatherThanFails() throws Exception {
        String transformId = "test-restoring-shard-transform";
        String destIndex = transformId + "-dest";

        blockAndStartRestore(REPO, SNAPSHOT, SOURCE_INDEX);
        try {
            TransformConfig config = TransformConfig.builder()
                .setId(transformId)
                .setSource(new SourceConfig(new String[] { SOURCE_INDEX }, QueryConfig.matchAll(), Map.of(), null))
                .setDest(new DestConfig(destIndex, null, null))
                .setLatestConfig(new LatestConfig(List.of("foo"), "foo"))
                .build();
            client().execute(PutTransformAction.INSTANCE, new PutTransformAction.Request(config, false, TimeValue.THIRTY_SECONDS))
                .actionGet(TimeValue.THIRTY_SECONDS);
            client().execute(StartTransformAction.INSTANCE, new StartTransformAction.Request(transformId, null, TimeValue.THIRTY_SECONDS))
                .actionGet(TimeValue.THIRTY_SECONDS);

            // Wait until the indexer has advanced past the initial STARTED state (meaning it has
            // issued its first search, which is now parked in SearchReadyGate).
            assertBusy(() -> {
                GetTransformStatsAction.Response s = client().execute(
                    GetTransformStatsAction.INSTANCE,
                    new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
                ).actionGet(TimeValue.THIRTY_SECONDS);
                assertThat(
                    s.getTransformsStats().get(0).getState(),
                    anyOf(equalTo(TransformStats.State.INDEXING), equalTo(TransformStats.State.FAILED))
                );
            }, 30, TimeUnit.SECONDS);

            // Confirm the parked search has not caused a failure
            GetTransformStatsAction.Response stats = client().execute(
                GetTransformStatsAction.INSTANCE,
                new GetTransformStatsAction.Request(transformId, TimeValue.THIRTY_SECONDS, false)
            ).actionGet(TimeValue.THIRTY_SECONDS);
            assertThat(stats.getTransformsStats().get(0).getState(), not(equalTo(TransformStats.State.FAILED)));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, SOURCE_INDEX);
            try {
                client().execute(
                    DeleteTransformAction.INSTANCE,
                    new DeleteTransformAction.Request(transformId, true, false, TimeValue.THIRTY_SECONDS)
                ).actionGet(TimeValue.THIRTY_SECONDS);
            } catch (Exception ignored) {}
        }
    }
}
