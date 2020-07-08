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
import org.elasticsearch.index.store.cache.PersistentCacheTracker;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

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
        public synchronized void trackPersistedBytesForFile(String name, long bytes) {
            addRecoveredBytesToFile(name, bytes);
        }

        public synchronized void trackFileEviction(String name) {
            File fileDetails = getFileDetails(name);
            assert fileDetails != null;
            fileDetails.resetRecovered();
        }

        @Override
        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            File fileDetails = getFileDetails(name);
            assert fileDetails != null;
            // It's possible that a read on the cache on an overlapping range triggers
            // multiple concurrent writes for the same range, in that case we need to
            // track the minimal amount of written data
            bytes = Math.min(bytes, fileDetails.length() - fileDetails.recovered());
            super.addRecoveredBytesToFile(name, bytes);
        }

        @Override
        public synchronized void stop() {
            // Since this is an on demand recovery,
            // the timer will remain open forever.
        }

        @Override
        public Map<String, Long> getPersistedFilesSize() {
            return fileDetails().stream().collect(Collectors.toMap(File::name, File::recovered));
        }
    }
}
