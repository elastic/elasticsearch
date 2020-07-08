/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import java.util.Collections;
import java.util.Map;

/**
 * Tracks progress of the underlying writes performed during the usage of a {@link CachedBlobContainerIndexInput}
 */
public interface PersistentCacheTracker {
    PersistentCacheTracker NO_OP = new PersistentCacheTracker() {
        @Override
        public void trackPersistedBytesForFile(String name, long bytes) {
        }

        @Override
        public void trackFileEviction(String name) {
        }

        @Override
        public Map<String, Long> getPersistedFilesSize() {
            return Collections.emptyMap();
        }
    };

    /**
     * Called after {@param bytes} have been written to disk for file {@param name}.
     * This method can be called multiple times with the same information, as the cache
     * can try to fetch and write the same file range concurrently.
     *
     * @param name the file written to disk
     * @param bytes the amount of data written to disk
     */
    void trackPersistedBytesForFile(String name, long bytes);

    /**
     * Called after {@param file} has been evicted from the persistent cache
     * @param name the file being evited from the persistent cache
     */
    void trackFileEviction(String name);

    /**
     * Returns the file sizes in bytes that this tracker has tracked so far
     */
    Map<String, Long> getPersistedFilesSize();
}
