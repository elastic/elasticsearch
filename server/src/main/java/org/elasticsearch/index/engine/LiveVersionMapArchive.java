/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
