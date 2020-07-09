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
import java.util.HashSet;
import java.util.Set;

public final class OnDemandRecoveryState extends RecoveryState {

    public OnDemandRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode);
    }

    @Override
    protected RecoveryState.Index createIndex() {
        return new Index();
    }

    @Override
    protected RecoveryState.Index createIndex(StreamInput in) throws IOException {
        return new Index(in);
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

    public static class Index extends RecoveryState.Index {
        public Index() {
            super(new SearchableSnapshotsRecoveryFiles());
        }

        public Index(StreamInput in) throws IOException {
            super(in, SearchableSnapshotsRecoveryFiles::new);
        }

        public synchronized void addSnapshotFile(String name, long bytes) {
            SearchableSnapshotsRecoveryFiles fd = (SearchableSnapshotsRecoveryFiles) fileDetails;
            fd.addSnapshotFileDetails(name, bytes);
        }

        public synchronized void resetRecoveredBytesOfFile(String name) {
            File fileDetails = getFileDetails(name);
            assert fileDetails != null;
            fileDetails.resetRecovered();
        }

        @Override
        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            File fileDetails = getFileDetails(name);
            assert fileDetails != null : "Unknown file " + name;
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
    }

    static class SearchableSnapshotsRecoveryFiles extends RecoveryFilesDetails {
        private final Set<String> snapshotFiles = new HashSet<>();

        SearchableSnapshotsRecoveryFiles() {}

        SearchableSnapshotsRecoveryFiles(StreamInput in) throws IOException {
            super(in);
        }

        void addSnapshotFileDetails(String name, long length) {
            addFileDetails(name, length, false);
            snapshotFiles.add(name);
        }

        @Override
        public void addFileDetails(String name, long length, boolean reused) {
            if (snapshotFiles.contains(name)) {
                return;
            }
            super.addFileDetails(name, length, reused);
        }

        @Override
        public void clear() {
            // During peer recovery it's possible that the recovery is retried until the shard
            // is active, the file details are cleared during those retries, but we should only
            // clear the files that don't belong to the snapshot
            fileDetails.entrySet().removeIf(entry -> snapshotFiles.contains(entry.getKey()) == false);
            complete = false;
        }
    }
}
