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
    private final AtomicReference<PrimaryTermAndGeneration> lastestUploaded = new AtomicReference<>(null);

    public AtomicMutableObjectStoreUploadTracker() {}

    /**
     * Updates the last uploaded term and generation if the provided term and generation is greater than the current one.
     */
    public void updateLatestUploaded(PrimaryTermAndGeneration termAndGen) {
        lastestUploaded.updateAndGet((current) -> {
            if (current == null || (termAndGen != null && termAndGen.compareTo(current) > 0)) {
                return termAndGen;
            }
            return current;
        });
    }

    public boolean isUploaded(PrimaryTermAndGeneration termAndGen) {
        var uploaded = lastestUploaded.get();
        return uploaded != null && termAndGen.compareTo(uploaded) <= 0;
    }
}
