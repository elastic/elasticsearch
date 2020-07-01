/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.indices.recovery.RecoveryFileDetails;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class LazyRecoveryState extends RecoveryState {

    public LazyRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        super(shardRouting, targetNode, sourceNode);
    }

    @Override
    protected RecoveryFileDetails createFileDetails() {
        return new LazyRecoveryFileDetails();
    }

    @Override
    public synchronized RecoveryState setStage(Stage stage) {
        // The recovery will be performed in the background,
        // so we move to the latest stage of lazy recovery
        // instead of DONE
        if (stage == Stage.DONE) {
            return super.setStage(Stage.LAZY_RECOVERY);
        }

        return super.setStage(stage);
    }

    private static class LazyRecoveryFileDetails implements RecoveryFileDetails {
        private final Map<String, File> fileDetails = new HashMap<>();

        private int filesWithUnknownLength;

        @Override
        public File get(String name) {
            File file = fileDetails.get(name);
            // Hide files with unknown length
            if (file != null && file.unknownLength()) {
                return null;
            }
            return file;
        }

        @Override
        public int size() {
            if (containsAnyFileWithUnknownLength()) {
                return 0;
            }
            return fileDetails.size();
        }

        @Override
        public boolean isEmpty() {
            return containsAnyFileWithUnknownLength() || fileDetails.isEmpty();
        }

        @Override
        public void clear() {
            fileDetails.clear();
            filesWithUnknownLength = 0;
        }

        @Override
        public File addFileDetails(String name, File file) {
            File existing = fileDetails.get(name);

            if (existing != null) {
                existing.setLength(file.length());
                filesWithUnknownLength--;
                return null;
            }

            // Since we're doing a lazy recovery, we ignore the
            // reused flag and mark the file as not reused
            return fileDetails.put(name, new File(name, file.length(), false));
        }

        @Override
        public void addRecoveredBytesToFile(String name, long bytes) {
            // During a searchable snapshot shard recovery it's possible that
            // this class has to track files that we don't know its length beforehand
            // (i.e. when the cache that backs the directory starts pre-warming
            // its cache before the shard is initialized)
            File file = fileDetails.get(name);
            if (file == null) {
                file = File.fileWithUnknownLength(name);
                fileDetails.put(name, file);
                filesWithUnknownLength++;
            }

            file.addRecoveredBytes(bytes);
        }

        @Override
        public Collection<File> values() {
            // In some scenarios it's possible that this class
            // starts tracking files before we know their length
            // (i.e. during searchable snapshots cache pre-warming).
            // In order to avoid confusing results, we just return
            // an empty list until we know that there aren't more
            // files with unknown length.
            if (containsAnyFileWithUnknownLength()) {
                return Collections.emptyList();
            }

            return fileDetails.values();
        }

        private boolean containsAnyFileWithUnknownLength() {
            return filesWithUnknownLength > 0;
        }
    }
}
