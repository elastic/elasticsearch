/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.util.BytesRef;

// Allows keeping track of the old map of a LiveVersionMap that gets evacuated on a refresh
public interface LiveVersionMapArchiver {
    /**
     * @param old is the old map that is evacuated on a refresh
     */
    void afterRefresh(LiveVersionMap.VersionLookup old);

    /**
     * @param generation the generation of the commit caused by the flush
     */
    void afterFlush(long generation);

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
