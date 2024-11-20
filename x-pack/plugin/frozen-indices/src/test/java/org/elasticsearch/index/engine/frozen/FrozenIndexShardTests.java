/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.engine.frozen;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class FrozenIndexShardTests extends IndexShardTestCase {

    /**
     * Make sure we can recover from a frozen engine
     */
    public void testRecoverFromFrozenPrimary() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        indexDoc(indexShard, "_doc", "1");
        indexDoc(indexShard, "_doc", "2");
        indexDoc(indexShard, "_doc", "3");
        flushAndCloseShardNoCheck(indexShard);
        final ShardRouting shardRouting = indexShard.routingEntry();
        IndexShard frozenShard = reinitShard(
            indexShard,
            ShardRoutingHelper.initWithSameId(
                shardRouting,
                shardRouting.primary() ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
            ),
            indexShard.indexSettings().getIndexMetadata(),
            config -> new FrozenEngine(config, true, randomBoolean())
        );
        recoverShardFromStore(frozenShard);
        assertThat(frozenShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(frozenShard.seqNoStats().getMaxSeqNo()));
        assertDocCount(frozenShard, 3);

        IndexShard replica = newShard(false, Settings.EMPTY, config -> new FrozenEngine(config, true, randomBoolean()));
        recoverReplica(replica, frozenShard, true);
        assertDocCount(replica, 3);
        closeShards(frozenShard, replica);
    }
}
