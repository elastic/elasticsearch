/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.indices.recovery.RecoveryState;

public final class OnDemandRecoveryState extends RecoveryState {

    public OnDemandRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode, new Index());
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
            super(new SearchableSnapshotsRecoveryFiles(), UNKNOWN, UNKNOWN);
        }

        public void resetRecoveredBytesOfFile(String name) {
            fileDetails.resetRecoveredBytesOfFile(name);
        }

        @Override
        public void stop() {
            // Since this is an on demand recovery,
            // the timer will remain open forever.
        }
    }

    static class SearchableSnapshotsRecoveryFiles extends RecoveryFilesDetails {
        @Override
        public void addFileDetails(String name, long length, boolean reused) {
            // We allow reporting the same file details multiple times as we populate the file
            // details before the recovery is executed and therefore we ignore the rest
            // of the calls for the same files.
            fileDetails.computeIfAbsent(name, n -> new FileDetail(name, length, reused));
        }

        @Override
        public void clear() {
            // Since we don't want to remove the recovery information that might have been
            // populated during cache pre-warming we just ignore clearing the file details.
            complete.set(false);
        }
    }
}
