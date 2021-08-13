/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * An alternative of {@link InternalEngine} that allows tweaking internals to reduce noise in engine tests.
 */
class InternalTestEngine extends InternalEngine {
    private final Map<String, Long> idToMaxSeqNo = ConcurrentCollections.newConcurrentMap();

    InternalTestEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    InternalTestEngine(EngineConfig engineConfig, int maxDocs,
                       BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig, maxDocs, localCheckpointTrackerSupplier);
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        if (index.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            idToMaxSeqNo.compute(index.id(), (id, existing) -> {
                if (existing == null) {
                    return index.seqNo();
                } else {
                    long maxSeqNo = Math.max(index.seqNo(), existing);
                    advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNo);
                    return maxSeqNo;
                }
            });
        }
        return super.index(index);
    }

    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        if (delete.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            final long maxSeqNo = idToMaxSeqNo.compute(delete.id(), (id, existing) -> {
                if (existing == null) {
                    return delete.seqNo();
                } else {
                    return Math.max(delete.seqNo(), existing);
                }
            });
            advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNo);
        }
        return super.delete(delete);
    }
}
