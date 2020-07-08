/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public final class OnDemandRecoveryState extends RecoveryState {

    public OnDemandRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode);
    }

    @Override
    protected Index createIndex() {
        return new OnDemandIndex();
    }

    @Override
    protected Index createIndex(StreamInput in) throws IOException {
        return new OnDemandIndex(in);
    }

    @Override
    public synchronized RecoveryState setStage(Stage stage) {
        // The recovery will be performed in the background,
        // so we move to the latest stage of lazy recovery
        // instead of DONE
        if (stage == Stage.DONE) {
            stage = Stage.ON_DEMAND;
        }

        return super.setStage(stage);
    }

    public static class OnDemandIndex extends Index implements PersistentCacheTracker {
        public OnDemandIndex() {}

        public OnDemandIndex(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public synchronized void addFileDetail(String name, long length, boolean reused) {
            super.addFileDetail(name, length, false);
        }

        @Override
        public synchronized void trackPersistedBytesToFile(String name, long bytes) {
            addRecoveredBytesToFile(name, bytes);
        }

        public synchronized void trackFileEviction(String name) {
            File fileDetails = getFileDetails(name);
            assert fileDetails != null;
            fileDetails.resetRecovered();
        }

        @Override
        public synchronized void stop() {
            // do nothing
        }

        @Override
        public synchronized PersistentCacheTracker merge(PersistentCacheTracker recoveryTracker) {
            for (Map.Entry<String, Long> fileSize : recoveryTracker.getValues().entrySet()) {
                addRecoveredBytesToFile(fileSize.getKey(), fileSize.getValue());
            }
            return this;
        }

        @Override
        public Map<String, Long> getValues() {
            return Collections.emptyMap();
        }
    }
}
