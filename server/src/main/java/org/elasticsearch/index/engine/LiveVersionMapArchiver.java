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
public interface LiveVersionMapArchiver {
    /**
     * Archive the old map evacuated due to a refresh
     *
     * @param old is the old map that is evacuated on a refresh
     */
    void afterRefresh(LiveVersionMap.VersionLookup old);

    /**
     * Trigger a cleanup of the archive based on the given generation
     *
     * @param generation the generation of the commit caused by the flush
     */
    void afterFlush(long generation);

    /**
     * Look up the given uid in the archive
     */
    VersionValue get(BytesRef uid);

    LiveVersionMapArchiver NOOP_ARCHIVER = new LiveVersionMapArchiver() {
        @Override
        public void afterRefresh(LiveVersionMap.VersionLookup old) {}

        @Override
        public void afterFlush(long generation) {}

        @Override
        public VersionValue get(BytesRef uid) {
            return null;
        }
    };
}
