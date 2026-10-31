/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CheapShardParkedAllocationDeciderTests extends ESTestCase {

    public void testOnlyPromotableCopiesOfParkedIndicesAreKeptUnassigned() {
        final IndexMetadata parked = indexMetadata(true);
        final IndexMetadata unparked = indexMetadata(randomBoolean() ? null : false);
        final ShardId shardId = new ShardId(parked.getIndex(), 0);
        final ShardRouting indexing = TestShardRouting.newShardRouting(
            shardId,
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            ShardRouting.Role.INDEX_ONLY
        );
        final ShardRouting search = TestShardRouting.newShardRouting(
            shardId,
            null,
            false,
            ShardRoutingState.UNASSIGNED,
            ShardRouting.Role.SEARCH_ONLY
        );

        assertThat(CheapShardParkedAllocationDecider.decide(indexing, parked).type(), equalTo(Decision.Type.NO));
        assertThat(CheapShardParkedAllocationDecider.decide(search, parked).type(), equalTo(Decision.Type.YES));
        assertThat(CheapShardParkedAllocationDecider.decide(indexing, unparked).type(), equalTo(Decision.Type.YES));
        assertThat(CheapShardParkedAllocationDecider.decide(search, unparked).type(), equalTo(Decision.Type.YES));
        assertThat(CheapShardParkedAllocationDecider.decide(indexing, null).type(), equalTo(Decision.Type.YES));
    }

    private static IndexMetadata indexMetadata(Boolean parked) {
        final Settings.Builder settings = indexSettings(IndexVersion.current(), 1, 1);
        if (parked != null) {
            settings.put(CheapShardParkedAllocationDecider.PARKED_SETTING.getKey(), parked);
        }
        return IndexMetadata.builder(randomIdentifier()).settings(settings).build();
    }
}
