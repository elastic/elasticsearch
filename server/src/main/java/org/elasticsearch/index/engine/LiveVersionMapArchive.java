/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;

/**
 * Keeps track of the old map of a LiveVersionMap that gets evacuated on a refresh
 */
public interface LiveVersionMapArchive {
    /**
     * Archive the old map evacuated due to a refresh
     *
     * @param old is the old map that is evacuated on a refresh
     */
    void afterRefresh(LiveVersionMap.VersionLookup old);

    /**
     * Look up the given uid in the archive
     */
    VersionValue get(BytesRef uid);

    /**
     * Returns the min delete timestamp across all archived maps.
     */
    long getMinDeleteTimestamp();

    /**
     * Returns whether the archive has seen an unsafe old map (passed via {@link LiveVersionMapArchive#afterRefresh})
     * which has not yet been refreshed on the unpromotable shards.
     */
    default boolean isUnsafe() {
        return false;
    }

    /**
     * Returns the total memory usage if the Archive.
     */
    default long getRamBytesUsed() {
        return 0L;
    }

    /**
     * Returns how much memory could be freed up by creating a new commit and issuing a new unpromotable refresh.
     */
    default long getReclaimableRamBytes() {
        return 0;
    }

    /**
     * Returns how much memory will be freed once the current ongoing unpromotable refresh is finished.
     */
    default long getRefreshingRamBytes() {
        return 0;
    }

    LiveVersionMapArchive NOOP_ARCHIVE = new LiveVersionMapArchive() {
        @Override
        public void afterRefresh(LiveVersionMap.VersionLookup old) {}

        @Override
        public VersionValue get(BytesRef uid) {
            return null;
        }

        @Override
        public long getMinDeleteTimestamp() {
            return Long.MAX_VALUE;
        }
    };
}
