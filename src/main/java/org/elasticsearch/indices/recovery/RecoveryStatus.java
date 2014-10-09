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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class RecoveryStatus {

    final ShardId shardId;
    final long recoveryId;
    final InternalIndexShard indexShard;
    final RecoveryState recoveryState;
    final DiscoveryNode sourceNode;

    public RecoveryStatus(long recoveryId, InternalIndexShard indexShard, DiscoveryNode sourceNode) {
        this.recoveryId = recoveryId;
        this.indexShard = indexShard;
        this.sourceNode = sourceNode;
        this.shardId = indexShard.shardId();
        this.recoveryState = new RecoveryState(shardId);
        recoveryState.getTimer().startTime(System.currentTimeMillis());
    }

    volatile Thread recoveryThread;
    private volatile boolean canceled;
    volatile boolean sentCanceledToSource;

    private volatile ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    public final Store.LegacyChecksums legacyChecksums = new Store.LegacyChecksums();

    public DiscoveryNode sourceNode() {
        return this.sourceNode;
    }

    public RecoveryState recoveryState() {
        return recoveryState;
    }

    public void stage(RecoveryState.Stage stage) {
        recoveryState.setStage(stage);
    }

    public RecoveryState.Stage stage() {
        return recoveryState.getStage();
    }

    public boolean isCanceled() {
        return canceled;
    }
    
    public synchronized void cancel() {
        canceled = true;
    }
    
    public IndexOutput getOpenIndexOutput(String key) {
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        if (canceled || outputs == null) {
            return null;
        }
        return outputs.get(key);
    }

    public synchronized Set<Entry<String, IndexOutput>> cancelAndClearOpenIndexInputs() {
        cancel();
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        openIndexOutputs = null;
        if (outputs == null) {
            return null;
        }
        Set<Entry<String, IndexOutput>> entrySet = outputs.entrySet();
        return entrySet;
    }
    

    public IndexOutput removeOpenIndexOutputs(String name) {
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        if (outputs == null) {
            return null;
        }
        return outputs.remove(name);
    }

    public synchronized IndexOutput openAndPutIndexOutput(String key, String fileName, StoreFileMetaData metaData, Store store) throws IOException {
        if (isCanceled()) {
            return null;
        }
        final ConcurrentMap<String, IndexOutput> outputs = openIndexOutputs;
        IndexOutput indexOutput = store.createVerifyingOutput(fileName, IOContext.DEFAULT, metaData);
        outputs.put(key, indexOutput);
        return indexOutput;
    }
}
