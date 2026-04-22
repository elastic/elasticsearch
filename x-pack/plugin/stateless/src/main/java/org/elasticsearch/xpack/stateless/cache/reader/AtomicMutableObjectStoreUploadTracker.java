/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An atomic reference is used to determine whether a commit has been uploaded to the object store in a thread-safe manner.
 */
public class AtomicMutableObjectStoreUploadTracker implements MutableObjectStoreUploadTracker {

    // Null means no commit has been uploaded yet.
    private final AtomicReference<PrimaryTermAndGeneration> latestUploaded = new AtomicReference<>(null);

    // Keep the latest non uploaded commit that has been notified (null means no commit has been notified yet)
    private record LatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {}

    private final AtomicReference<LatestCommitInfo> latestCommitInfo = new AtomicReference<>(null);

    public AtomicMutableObjectStoreUploadTracker() {}

    /**
     * Updates the last uploaded term and generation if the provided term and generation is greater than the current one.
     */
    @Override
    public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        latestUploaded.updateAndGet((current) -> {
            if (current == null || (latestUploadedBccTermAndGen != null && latestUploadedBccTermAndGen.compareTo(current) > 0)) {
                return latestUploadedBccTermAndGen;
            }
            return current;
        });
    }

    /**
     * Updates the latest commit term and generation information if the provided term and generation is greater than the current one, and
     * also updates the preferred indexing node ID to reach out to read it in case it has not been uploaded yet.
     */
    @Override
    public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        latestCommitInfo.updateAndGet((current) -> {
            if (current == null || (ccTermAndGen != null && ccTermAndGen.compareTo(current.ccTermAndGen) > 0)) {
                return new LatestCommitInfo(ccTermAndGen, nodeId);
            }
            return current;
        });
    }

    private String getLatestNodeIdOrNull() {
        var latest = latestCommitInfo.get();
        return latest != null ? latest.nodeId : null;
    }

    @Override
    public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
        // capture the preferred node id at the time the file is opened (can be null if no commit have been notified yet, in which case the
        // search shard should not try to reach the indexing shard)
        var preferredNodeId = getLatestNodeIdOrNull();
        return new UploadInfo() {
            @Override
            public boolean isUploaded() {
                var uploaded = latestUploaded.get();
                return uploaded != null && bccTermAndGen.compareTo(uploaded) <= 0;
            }

            @Override
            public String preferredNodeId() {
                return preferredNodeId;
            }
        };
    }
}
