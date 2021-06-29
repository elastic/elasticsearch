/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.util.HashSet;
import java.util.Set;

public final class SearchableSnapshotRecoveryState extends RecoveryState {
    private boolean preWarmComplete;
    private boolean remoteTranslogSet;

    public SearchableSnapshotRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode, new Index());
    }

    @Override
    public synchronized RecoveryState setStage(Stage stage) {
        // The transition to the final state was done by #prewarmCompleted, just ignore the transition
        if (getStage() == Stage.DONE || stage == Stage.FINALIZE && remoteTranslogSet) {
            return this;
        }

        // Pre-warm is still running, hold the state transition
        // until the pre-warm process finishes
        if (preWarmComplete == false && stage == Stage.DONE) {
            validateCurrentStage(Stage.FINALIZE);
            return this;
        }

        if (stage == Stage.INIT) {
            remoteTranslogSet = false;
        }

        return super.setStage(stage);
    }

    @Override
    public synchronized RecoveryState setRemoteTranslogStage() {
        remoteTranslogSet = true;
        super.setStage(Stage.TRANSLOG);
        return super.setStage(Stage.FINALIZE);
    }

    @Override
    public synchronized void validateCurrentStage(Stage expected) {
        if (remoteTranslogSet == false) {
            super.validateCurrentStage(expected);
        } else {
            final Stage stage = getStage();
            // For small indices it's possible that pre-warming finished shortly
            // after transitioning to FINALIZE stage
            if (stage != Stage.FINALIZE && stage != Stage.DONE) {
                assert false : "expected stage [" + Stage.FINALIZE + " || " + Stage.DONE + "]; but current stage is [" + stage + "]";
                throw new IllegalStateException(
                    "expected stage [" + Stage.FINALIZE + " || " + Stage.DONE + "]; " + "but current stage is [" + stage + "]"
                );
            }
        }
    }

    // Visible for tests
    boolean isRemoteTranslogSet() {
        return remoteTranslogSet;
    }

    public synchronized void setPreWarmComplete() {
        // For small shards it's possible that the
        // cache is pre-warmed before the stage has transitioned
        // to FINALIZE, so the transition to the final state is delayed until
        // the recovery process catches up.
        if (getStage() == Stage.FINALIZE) {
            super.setStage(Stage.DONE);
        }

        SearchableSnapshotRecoveryState.Index index = (Index) getIndex();
        index.stopTimer();
        preWarmComplete = true;
    }

    public synchronized boolean isPreWarmComplete() {
        return preWarmComplete;
    }

    public synchronized void ignoreFile(String name) {
        SearchableSnapshotRecoveryState.Index index = (Index) getIndex();
        index.addFileToIgnore(name);
    }

    public synchronized void markIndexFileAsReused(String name) {
        SearchableSnapshotRecoveryState.Index index = (Index) getIndex();
        index.markFileAsReused(name);
    }

    private static final class Index extends RecoveryState.Index {
        // We ignore the files that won't be part of the pre-warming
        // phase since the information for those files won't be
        // updated and marking them as reused might be confusing,
        // as they are fetched on-demand from the underlying repository.
        private final Set<String> filesToIgnore = new HashSet<>();

        private Index() {
            super(new SearchableSnapshotRecoveryFilesDetails());
            // We start loading data just at the beginning
            super.start();
        }

        private synchronized void addFileToIgnore(String name) {
            filesToIgnore.add(name);
        }

        @Override
        public synchronized void addFileDetail(String name, long length, boolean reused) {
            if (filesToIgnore.contains(name)) {
                return;
            }

            super.addFileDetail(name, length, reused);
        }

        private synchronized void markFileAsReused(String name) {
            ((SearchableSnapshotRecoveryFilesDetails) fileDetails).markFileAsReused(name);
        }

        // We have to bypass all the calls to the timer
        @Override
        public synchronized void start() {}

        @Override
        public synchronized void stop() {}

        @Override
        public synchronized void reset() {}

        private synchronized void stopTimer() {
            super.stop();
        }
    }

    private static class SearchableSnapshotRecoveryFilesDetails extends RecoveryFilesDetails {
        @Override
        public void addFileDetails(String name, long length, boolean reused) {
            // We allow reporting the same file details multiple times as we populate the file
            // details before the recovery is executed (see SearchableSnapshotDirectory#prewarmCache)
            // and therefore we ignore the rest of the calls for the same files.
            // Additionally, it's possible that a segments_n file that wasn't part of the snapshot is
            // sent over during peer recoveries as after restore a new segments file is generated
            // (see StoreRecovery#bootstrap).
            FileDetail fileDetail = fileDetails.computeIfAbsent(name, n -> new FileDetail(name, length, reused));
            assert fileDetail == null || fileDetail.name().equals(name) && fileDetail.length() == length
                : "The file "
                    + name
                    + " was reported multiple times with different lengths: ["
                    + fileDetail.length()
                    + "] and ["
                    + length
                    + "]";
        }

        void markFileAsReused(String name) {
            final FileDetail fileDetail = fileDetails.get(name);
            assert fileDetail != null;
            fileDetails.put(name, new FileDetail(fileDetail.name(), fileDetail.length(), true));
        }

        @Override
        public void clear() {
            // Since we don't want to remove the recovery information that might have been
            // populated during cache pre-warming we just ignore clearing the file details.
            complete = false;
        }
    }
}
