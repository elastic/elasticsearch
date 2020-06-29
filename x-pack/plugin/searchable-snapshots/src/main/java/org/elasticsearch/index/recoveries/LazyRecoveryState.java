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
import java.util.Collection;
import java.util.Collections;

public class LazyRecoveryState extends RecoveryState {

    public LazyRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode);
    }

    @Override
    protected Index createIndex() {
        return new LazyIndexStats();
    }

    @Override
    protected Index createIndex(StreamInput in) throws IOException {
        return new LazyIndexStats(in);
    }

    @Override
    public synchronized RecoveryState setStage(Stage stage) {
        // The recovery will be performed in the background,
        // therefor the reason for capturing the DONE state
        // and moving it to LAZY_RECOVERY instead.
        if (stage == Stage.DONE) {
            return super.setStage(Stage.LAZY_RECOVERY);
        }

        return super.setStage(stage);
    }

    private static class LazyIndexStats extends RecoveryState.Index {
        LazyIndexStats() {}

        LazyIndexStats(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public synchronized void addFileDetail(String name, long length, boolean reused) {
            File file = getFileDetails(name);

            if (file != null) {
                file.setLength(length);
            } else {
                // Since we're doing a lazy recovery, we ignore the
                // reused flag and mark the file as not reused
                addFileDetails(new File(name, length, false));
            }
        }

        @Override
        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            File file = getFileDetails(name);
            if (file == null) {
                addFileDetails(File.fileWithUnknownLength(name));
            }

            super.addRecoveredBytesToFile(name, bytes);
        }

        @Override
        protected synchronized Collection<File> getFileDetails() {
            // In some scenarios it's possible that this class
            // starts tracking files before we know their length
            // (i.e. during searchable snapshots cache pre-warming).
            // In order to avoid confusing results, we just return
            // an empty list until we know that there aren't more
            // files with unknown length.
            if (hasAnyUnknownLengthFile()) {
                return Collections.emptyList();
            }

            return super.getFileDetails();
        }

        @Override
        public synchronized int totalFileCount() {
            if (hasAnyUnknownLengthFile()) {
                return 0;
            }

            return super.totalFileCount();
        }

        private boolean hasAnyUnknownLengthFile() {
            return super.getFileDetails().stream().anyMatch(File::unknownLength);
        }
    }
}
