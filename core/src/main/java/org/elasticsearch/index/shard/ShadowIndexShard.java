/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import java.io.IOException;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexServicesProvider;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;

/**
 * ShadowIndexShard extends {@link IndexShard} to add file synchronization
 * from the primary when a flush happens. It also ensures that a replica being
 * promoted to a primary causes the shard to fail, kicking off a re-allocation
 * of the primary shard.
 */
public final class ShadowIndexShard extends IndexShard {

    public ShadowIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, ShardPath path, Store store, IndexServicesProvider provider) throws IOException {
        super(shardId, indexSettings, path, store, provider);
    }

    /**
     * In addition to the regular accounting done in
     * {@link IndexShard#updateRoutingEntry(org.elasticsearch.cluster.routing.ShardRouting, boolean)},
     * if this shadow replica needs to be promoted to a primary, the shard is
     * failed in order to allow a new primary to be re-allocated.
     */
    @Override
    public void updateRoutingEntry(ShardRouting newRouting, boolean persistState) {
        if (newRouting.primary() == true) {// becoming a primary
            throw new IllegalStateException("can't promote shard to primary");
        }
        super.updateRoutingEntry(newRouting, persistState);
    }

    @Override
    public MergeStats mergeStats() {
        return new MergeStats();
    }

    @Override
    public boolean canIndex() {
        return false;
    }

    @Override
    protected Engine newEngine(boolean skipInitialTranslogRecovery, EngineConfig config) {
        assert this.shardRouting.primary() == false;
        assert skipInitialTranslogRecovery : "can not recover from gateway";
        config.setCreate(false); // hardcoded - we always expect an index to be present
        return engineFactory.newReadOnlyEngine(config);
    }

    @Override
    public boolean shouldFlush() {
        // we don't need to flush since we don't write - all dominated by the primary
        return false;
    }

    public boolean allowsPrimaryPromotion() {
        return false;
    }

    @Override
    public TranslogStats translogStats() {
        return null; // shadow engine has no translog
    }
}
