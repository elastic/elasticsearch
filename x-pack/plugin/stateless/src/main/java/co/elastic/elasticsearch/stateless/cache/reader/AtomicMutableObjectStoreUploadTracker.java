/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

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
    public void updateLatestUploadInfo(
        PrimaryTermAndGeneration latestUploadedBccTermAndGen,
        PrimaryTermAndGeneration ccTermAndGen,
        String nodeId
    ) {
        latestUploaded.updateAndGet((current) -> {
            if (current == null || (latestUploadedBccTermAndGen != null && latestUploadedBccTermAndGen.compareTo(current) > 0)) {
                return latestUploadedBccTermAndGen;
            }
            return current;
        });
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
