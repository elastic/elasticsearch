/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.indices.recovery.RecoveryState;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

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
            super(new SearchableSnapshotsRecoveryFiles());
        }

        public synchronized void addCacheFileDetail(String fileName, CacheFile cacheFile) {
            CachedFileDetail fileDetails = (CachedFileDetail) getFileDetails(fileName);
            assert fileDetails != null : "Unknown file [" + fileName + "]. The file detail should have been declared during recovery";
            fileDetails.addCacheFile(cacheFile);
        }

        public synchronized void removeCacheFileDetail(String fileName, CacheFile cacheFile) {
            CachedFileDetail fileDetails = (CachedFileDetail) getFileDetails(fileName);
            assert fileDetails != null : "Unknown file [" + fileName + "]. The file detail should have been declared during recovery";
            fileDetails.removeCacheFile(cacheFile);
        }

        @Override
        public void stop() {
            // Since this is an on demand recovery, the timer will remain open forever.
        }
    }

    private static class SearchableSnapshotsRecoveryFiles extends RecoveryFilesDetails {
        @Override
        public void addFileDetails(String name, long length, boolean reused) {
            // We allow reporting the same file details multiple times as we populate the file
            // details before the recovery is executed and therefore we ignore the rest
            // of the calls for the same files.
            fileDetails.computeIfAbsent(name, n -> new CachedFileDetail(name, length, reused));
        }

        @Override
        public void clear() {
            // Since we don't want to remove the recovery information that might have been
            // populated during cache pre-warming we just ignore clearing the file details.
            complete = false;
        }
    }

    private static class CachedFileDetail extends FileDetail {
        private final Set<CacheFile> cacheFiles = Collections.newSetFromMap(new IdentityHashMap<>());

        private CachedFileDetail(String name, long length, boolean reused) {
            super(name, length, reused);
        }

        private void addCacheFile(CacheFile cacheFile) {
            // It's possible that multiple CachedBlobContainerIndexInput use the same
            // cacheFile, so that's the reason why we use a set and we don't assert that
            // the cacheFile is added only once.
            cacheFiles.add(cacheFile);
        }

        private void removeCacheFile(CacheFile cacheFile) {
            // It's possible that the file is evicted even before CachedBlobContainerIndexInput
            // was able to get a reference to the file (i.e. a small cache size), so we can
            // have notifications about unknown CacheFiles for this FileDetail
            cacheFiles.remove(cacheFile);
        }

        @Override
        public void addRecoveredBytes(long bytes) {
            // No-op
        }

        @Override
        public long recovered() {
            long recovered = 0;
            for (CacheFile cacheFile : cacheFiles) {
                recovered += cacheFile.getCachedLength();
            }
            return recovered;
        }
    }
}
