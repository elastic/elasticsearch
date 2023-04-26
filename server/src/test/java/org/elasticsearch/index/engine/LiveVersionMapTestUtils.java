/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;

public class LiveVersionMapTestUtils {

    public static VersionValue get(LiveVersionMap map, String id) {
        try (Releasable r = acquireLock(map, uid(id))) {
            return map.getUnderLock(uid(id));
        }
    }

    public static void putIndex(LiveVersionMap map, String id, IndexVersionValue version) {
        try (Releasable r = acquireLock(map, uid(id))) {
            map.putIndexUnderLock(uid(id), version);
        }
    }

    public static void putDelete(LiveVersionMap map, String id, DeleteVersionValue version) {
        try (Releasable r = acquireLock(map, uid(id))) {
            map.putDeleteUnderLock(uid(id), version);
        }
    }

    public static void pruneTombstones(LiveVersionMap map, long maxTimestampToPrune, long maxSeqNoToPrune) {
        map.pruneTombstones(maxTimestampToPrune, maxSeqNoToPrune);
    }

    public static int VersionLookupSize(LiveVersionMap.VersionLookup lookup) {
        return lookup.size();
    }

    private static Releasable acquireLock(LiveVersionMap map, BytesRef uid) {
        return map.acquireLock(uid);
    }

    private static BytesRef uid(String id) {
        return new Term(IdFieldMapper.NAME, Uid.encodeId(id)).bytes();
    }
}
