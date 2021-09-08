/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.shards;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.equalTo;

public class FrozenShardsDeciderIT extends org.elasticsearch.xpack.autoscaling.AbstractFrozenAutoscalingIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testScale() throws Exception {
        setupRepoAndPolicy();
        createAndMountIndex();

        assertThat(
            capacity().results().get("frozen").requiredCapacity().total().memory(),
            equalTo(FrozenShardsDeciderService.DEFAULT_MEMORY_PER_SHARD)
        );
    }

    @Override
    protected String deciderName() {
        return FrozenShardsDeciderService.NAME;
    }

    @Override
    protected Settings.Builder addDeciderSettings(Settings.Builder builder) {
        return builder.put(FrozenShardsDeciderService.MEMORY_PER_SHARD.getKey(), FrozenShardsDeciderService.DEFAULT_MEMORY_PER_SHARD);
    }
}
