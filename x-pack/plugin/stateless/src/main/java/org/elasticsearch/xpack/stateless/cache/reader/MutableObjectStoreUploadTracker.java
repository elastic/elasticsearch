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
        public void updateLatestUploadInfo(
            PrimaryTermAndGeneration latestUploadedBccTermAndGen,
            PrimaryTermAndGeneration ccTermAndGen,
            String nodeId
        ) {
            // no op
        }
    };

    void updateLatestUploadInfo(PrimaryTermAndGeneration latestUploadedBccTermAndGen, PrimaryTermAndGeneration ccTermAndGen, String nodeId);
}
