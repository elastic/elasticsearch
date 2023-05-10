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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.translog.Translog;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;

public class LiveVersionMapTestUtils {

    public static LiveVersionMap newLiveVersionMap(LiveVersionMapArchive archive) {
        return new LiveVersionMap(archive);
    }

    public static DeleteVersionValue newDeleteVersionValue(long version, long seqNo, long term, long time) {
        return new DeleteVersionValue(version, seqNo, term, time);
    }

    public static IndexVersionValue newIndexVersionValue(Translog.Location location, long version, long seqNo, long term) {
        return new IndexVersionValue(location, version, seqNo, term);
    }

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

    static IndexVersionValue randomIndexVersionValue() {
        return new IndexVersionValue(randomTranslogLocation(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    static Translog.Location randomTranslogLocation() {
        if (randomBoolean()) {
            return null;
        } else {
            return new Translog.Location(randomNonNegativeLong(), randomNonNegativeLong(), randomInt());
        }
    }

    public static int versionLookupSize(LiveVersionMap.VersionLookup lookup) {
        return lookup.size();
    }

    private static Releasable acquireLock(LiveVersionMap map, BytesRef uid) {
        return map.acquireLock(uid);
    }

    public static BytesRef uid(String id) {
        return new Term(IdFieldMapper.NAME, Uid.encodeId(id)).bytes();
    }

    public static boolean isUnsafe(LiveVersionMap map) {
        return map.isUnsafe();
    }

    public static boolean isSafeAccessRequired(LiveVersionMap map) {
        return map.isSafeAccessRequired();
    }
}
