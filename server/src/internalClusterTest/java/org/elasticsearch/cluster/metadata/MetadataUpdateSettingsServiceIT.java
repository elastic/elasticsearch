/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class MetadataUpdateSettingsServiceIT extends ESIntegTestCase {

    public void testThatNonDynamicSettingChangesTakeEffect() throws Exception {
        /*
         * This test makes sure that when non-dynamic settings are updated that they actually take effect (as opposed to just being set
         * in the cluster state).
         */
        createIndex("test", Settings.EMPTY);
        MetadataUpdateSettingsService metadataUpdateSettingsService = internalCluster().getCurrentMasterNodeInstance(
            MetadataUpdateSettingsService.class
        );
        List<Index> indicesList = new ArrayList<>();
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                indicesList.add(indexService.index());
            }
        }
        final var indices = indicesList.toArray(Index.EMPTY_ARRAY);

        final Function<UpdateSettingsClusterStateUpdateRequest.OnStaticSetting, UpdateSettingsClusterStateUpdateRequest> requestFactory =
            onStaticSetting -> new UpdateSettingsClusterStateUpdateRequest(
                TEST_REQUEST_TIMEOUT,
                TimeValue.ZERO,
                Settings.builder().put("index.codec", "FastDecompressionCompressingStoredFieldsData").build(),
                UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                onStaticSetting,
                indices
            );

        // First make sure it fails if reopenShards is not set on the request:
        AtomicBoolean expectedFailureOccurred = new AtomicBoolean(false);
        metadataUpdateSettingsService.updateSettings(
            requestFactory.apply(UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REJECT),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    fail("Should have failed updating a non-dynamic setting without reopenShards set to true");
                }

                @Override
                public void onFailure(Exception e) {
                    expectedFailureOccurred.set(true);
                }
            }
        );
        assertBusy(() -> assertThat(expectedFailureOccurred.get(), equalTo(true)));

        // Now we set reopenShards and expect it to work:
        AtomicBoolean success = new AtomicBoolean(false);
        metadataUpdateSettingsService.updateSettings(
            requestFactory.apply(UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    success.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );
        assertBusy(() -> assertThat(success.get(), equalTo(true)));

        // Now we look into the IndexShard objects to make sure that the code was actually updated (vs just the setting):
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                assertBusy(() -> {
                    for (IndexShard indexShard : indexService) {
                        final Engine engine = indexShard.getEngineOrNull();
                        assertNotNull("engine is null for " + indexService.index().getName(), engine);
                        assertThat(engine.getEngineConfig().getCodec().getName(), equalTo("FastDecompressionCompressingStoredFieldsData"));
                    }
                });
            }
        }
    }

    public void testThatNonDynamicSettingChangesDoNotUnncessesarilyCauseReopens() throws Exception {
        /*
         * This test makes sure that if a setting change request for a non-dynamic setting is made on an index that already has that
         * value we don't unassign the shards to apply the change -- there is no need. First we set a non-dynamic setting for the
         * first time, and see that the shards for the index are unassigned. Then we set a different dynamic setting, and include setting
         * the original non-dynamic setting to the same value as the previous request. We make sure that the new setting comes through
         * but that the shards are not unassigned.
         */
        final String indexName = "test";
        createIndex(indexName, Settings.EMPTY);
        MetadataUpdateSettingsService metadataUpdateSettingsService = internalCluster().getCurrentMasterNodeInstance(
            MetadataUpdateSettingsService.class
        );
        List<Index> indicesList = new ArrayList<>();
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                indicesList.add(indexService.index());
            }
        }
        final var indices = indicesList.toArray(Index.EMPTY_ARRAY);

        final Function<Settings.Builder, UpdateSettingsClusterStateUpdateRequest> requestFactory =
            settings -> new UpdateSettingsClusterStateUpdateRequest(
                TEST_REQUEST_TIMEOUT,
                TimeValue.ZERO,
                settings.build(),
                UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES,
                indices
            );

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        AtomicBoolean shardsUnassigned = new AtomicBoolean(false);
        AtomicBoolean expectedSettingsChangeInClusterState = new AtomicBoolean(false);
        AtomicReference<String> expectedSetting = new AtomicReference<>("index.codec");
        AtomicReference<String> expectedSettingValue = new AtomicReference<>("FastDecompressionCompressingStoredFieldsData");
        clusterService.addListener(event -> {
            // We want the cluster change event where the setting is applied. This will be the same one where shards are unassigned
            if (event.metadataChanged()
                && event.state().metadata().index(indexName) != null
                && expectedSettingValue.get().equals(event.state().metadata().index(indexName).getSettings().get(expectedSetting.get()))) {
                expectedSettingsChangeInClusterState.set(true);
                if (event.routingTableChanged() && event.state().routingTable().indicesRouting().containsKey(indexName)) {
                    if (hasUnassignedShards(event.state(), indexName)) {
                        shardsUnassigned.set(true);
                    }
                }
            }
        });

        AtomicBoolean success = new AtomicBoolean(false);
        // Make the first request, just to set things up:
        metadataUpdateSettingsService.updateSettings(
            requestFactory.apply(Settings.builder().put("index.codec", "FastDecompressionCompressingStoredFieldsData")),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    success.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );
        assertBusy(() -> assertThat(success.get(), equalTo(true)));
        assertBusy(() -> assertThat(expectedSettingsChangeInClusterState.get(), equalTo(true)));
        assertThat(shardsUnassigned.get(), equalTo(true));

        assertBusy(() -> assertThat(hasUnassignedShards(clusterService.state(), indexName), equalTo(false)));

        success.set(false);
        expectedSettingsChangeInClusterState.set(false);
        shardsUnassigned.set(false);
        expectedSetting.set("index.max_result_window");
        expectedSettingValue.set("1500");
        // Making this request ought to add this new setting but not unassign the shards:
        metadataUpdateSettingsService.updateSettings(
            // Same request, except now we'll also set the dynamic "index.max_result_window" setting:
            requestFactory.apply(
                Settings.builder().put("index.codec", "FastDecompressionCompressingStoredFieldsData").put("index.max_result_window", "1500")
            ),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    success.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );

        assertBusy(() -> assertThat(success.get(), equalTo(true)));
        assertBusy(() -> assertThat(expectedSettingsChangeInClusterState.get(), equalTo(true)));
        assertThat(shardsUnassigned.get(), equalTo(false));

    }

    private boolean hasUnassignedShards(ClusterState state, String indexName) {
        return state.routingTable()
            .indicesRouting()
            .get(indexName)
            .allShards()
            .anyMatch(shardRoutingTable -> shardRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size() > 0);
    }
}
