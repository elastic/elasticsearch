/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
        UpdateSettingsClusterStateUpdateRequest request = new UpdateSettingsClusterStateUpdateRequest();
        List<Index> indices = new ArrayList<>();
        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                indices.add(indexService.index());
            }
        }
        request.indices(indices.toArray(Index.EMPTY_ARRAY));
        request.settings(Settings.builder().put("index.codec", "FastDecompressionCompressingStoredFieldsData").build());

        // First make sure it fails if reopenShards is not set on the request:
        AtomicBoolean expectedFailureOccurred = new AtomicBoolean(false);
        metadataUpdateSettingsService.updateSettings(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                fail("Should have failed updating a non-dynamic setting without reopenShards set to true");
            }

            @Override
            public void onFailure(Exception e) {
                expectedFailureOccurred.set(true);
            }
        });
        assertBusy(() -> assertThat(expectedFailureOccurred.get(), equalTo(true)));

        // Now we set reopenShards and expect it to work:
        request.reopenShards(true);
        AtomicBoolean success = new AtomicBoolean(false);
        metadataUpdateSettingsService.updateSettings(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                success.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
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

}
