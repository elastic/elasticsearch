/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;

import java.io.IOException;

import static org.elasticsearch.cluster.routing.ShardRoutingHelper.initWithSameId;

public class NoOpEngineRecoveryTests extends IndexShardTestCase {

    public void testRecoverFromNoOp() throws IOException {
        final int nbDocs = scaledRandomIntBetween(1, 100);

        final IndexShard indexShard = newStartedShard(true);
        for (int i = 0; i < nbDocs; i++) {
            indexDoc(indexShard, "_doc", String.valueOf(i));
        }
        indexShard.close("test", true);

        final ShardRouting shardRouting = indexShard.routingEntry();
        IndexShard primary = reinitShard(
            indexShard,
            initWithSameId(shardRouting, ExistingStoreRecoverySource.INSTANCE),
            indexShard.indexSettings().getIndexMetadata(),
            NoOpEngine::new
        );
        recoverShardFromStore(primary);
        assertEquals(primary.seqNoStats().getMaxSeqNo(), primary.getMaxSeqNoOfUpdatesOrDeletes());
        assertEquals(nbDocs, primary.docStats().getCount());

        IndexShard replica = newShard(false, Settings.EMPTY, NoOpEngine::new);
        recoverReplica(replica, primary, true);
        assertEquals(replica.seqNoStats().getMaxSeqNo(), replica.getMaxSeqNoOfUpdatesOrDeletes());
        assertEquals(nbDocs, replica.docStats().getCount());
        closeShards(primary, replica);
    }
}
