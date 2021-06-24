/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.AbstractFrozenAutoscalingIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FrozenStorageDeciderIT extends AbstractFrozenAutoscalingIntegTestCase {

    public void testScale() throws Exception {
        setupRepoAndPolicy();
        createAndMountIndex();

        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .stats(new IndicesStatsRequest().indices(restoredIndexName))
            .actionGet();
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        assertThat(
            capacity().results().get("frozen").requiredCapacity().total().storage(),
            equalTo(
                ByteSizeValue.ofBytes(
                    (long) (statsResponse.getPrimaries().store.totalDataSetSize().getBytes()
                        * FrozenStorageDeciderService.DEFAULT_PERCENTAGE) / 100
                )
            )
        );
    }

    @Override
    protected String deciderName() {
        return FrozenStorageDeciderService.NAME;
    }

    @Override
    protected Settings.Builder addDeciderSettings(Settings.Builder builder) {
        return builder.put(FrozenStorageDeciderService.PERCENTAGE.getKey(), FrozenStorageDeciderService.DEFAULT_PERCENTAGE);
    }
}
