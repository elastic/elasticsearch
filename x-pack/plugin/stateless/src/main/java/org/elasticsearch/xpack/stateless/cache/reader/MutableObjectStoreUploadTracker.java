/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/**
 * Extends {@link ObjectStoreUploadTracker} to allow updating the last uploaded term and generation.
 */
public interface MutableObjectStoreUploadTracker extends ObjectStoreUploadTracker {

    UploadInfo UPLOADED = new UploadInfo() {

        @Override
        public boolean isUploaded() {
            return true;
        }

        @Override
        public String preferredNodeId() {
            assert false : "method should not be called";
            return null;
        }
    };

    MutableObjectStoreUploadTracker ALWAYS_UPLOADED = new MutableObjectStoreUploadTracker() {

        @Override
        public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
            return UPLOADED;
        }

        @Override
        public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
            // no op
        }

        @Override
        public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
            // no op
        }
    };

    /**
     * Updates the latest uploaded {@link BatchedCompoundCommit} information.
     * The implementation should ignore the argument if the latest information is already more recent.
     */
    void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen);

    /**
     * Updates the latest {@link StatelessCompoundCommit} information, and the preferred indexing
     * node that can be reached out to read it in case it is not uploaded yet to the object store.
     * The implementation should ignore the arguments if the latest information is already more recent.
     */
    void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId);
}
