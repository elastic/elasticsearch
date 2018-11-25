/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    InternalTestEngine(EngineConfig engineConfig, BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier) {
        super(engineConfig, localCheckpointTrackerSupplier);
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
