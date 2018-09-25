/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;

/**
 * An engine implementation for following shards.
 */
public final class FollowingEngine extends InternalEngine {

    private final CounterMetric numOfOptimizedIndexing = new CounterMetric();

    /**
     * Construct a new following engine with the specified engine configuration.
     *
     * @param engineConfig the engine configuration
     */
    FollowingEngine(final EngineConfig engineConfig) {
        super(validateEngineConfig(engineConfig));
    }

    private static EngineConfig validateEngineConfig(final EngineConfig engineConfig) {
        if (CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(engineConfig.getIndexSettings().getSettings()) == false) {
            throw new IllegalArgumentException("a following engine can not be constructed for a non-following index");
        }
        return engineConfig;
    }

    private void preFlight(final Operation operation) {
        /*
         * We assert here so that this goes uncaught in unit tests and fails nodes in standalone tests (we want a harsh failure so that we
         * do not have a situation where a shard fails and is recovered elsewhere and a test subsequently passes). We throw an exception so
         * that we also prevent issues in production code.
         */
        assert operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
        if (operation.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            throw new IllegalStateException("a following engine does not accept operations without an assigned sequence number");
        }
        assert (operation.origin() == Operation.Origin.PRIMARY) == (operation.versionType() == VersionType.EXTERNAL) :
            "invalid version_type in a following engine; version_type=" + operation.versionType() + "origin=" + operation.origin();
    }

    @Override
    protected InternalEngine.IndexingStrategy indexingStrategyForOperation(final Index index) throws IOException {
        preFlight(index);
        /*
         * A NOTE ABOUT OPTIMIZATION USING SEQUENCE NUMBERS:
         *
         * 1. Indexing operations are processed concurrently in an engine. However, operations of the same docID are processed
         *    one by one under the docID lock.
         *
         * 2. An engine itself can resolve correctly if an operation is delivered multiple times. However, if an operation is
         *    optimized and delivered multiple, it will be appended into Lucene more than once. We void this issue by never
         *    optimizing an operation if it was processed in the engine (using LocalCheckpointTracker).
         *
         * 3. When replicating operations to replicas or followers, we also carry the max seq_no_of_updates_or_deletes on the
         *    leader to followers. This transfer guarantees the MUS on a follower when operation O is processed at least the
         *    MUS on the leader when it was executed.
         *
         * 4. The following proves that docID(O) does not exist on a follower when operation O is applied if MSU(O) <= LCP < seqno(O):
         *
         *    4.1) If such operation O' with docID(O’) = docID(O), and LCP < seqno(O’), then MSU(O) >= MSU(O') because O' was
         *         delivered to the follower before O. MUS(0') on the leader is at least seqno(O) or seqno(0') and both > LCP.
         *         This contradicts the assumption [MSU(O) <= LCP].
         *
         *    4.2) MSU(O) < seqno(O) then docID(O) does not exist when O is applied on a leader. This means docID(O) does not exist
         *         after we apply every operation with docID = docID(O) and seqno < seqno(O). On the follower, we have applied every
         *         operation with seqno <= LCP, and there is no such O' with docID(O’) = docID(O) and LCP < seqno(O’)[4.1].
         *         These mean the follower has applied every operation with docID = docID(O) and seqno < seqno(O).
         *         Thus docID(O) does not exist on the follower.
         */
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        if (containsOperation(index)) {
            return IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());

        } else if (maxSeqNoOfUpdatesOrDeletes <= getLocalCheckpoint()) {
            assert maxSeqNoOfUpdatesOrDeletes < index.seqNo() : "seq_no[" + index.seqNo() + "] <= msu[" + maxSeqNoOfUpdatesOrDeletes + "]";
            numOfOptimizedIndexing.inc();
            return InternalEngine.IndexingStrategy.optimizedAppendOnly(index.seqNo(), index.version());

        } else {
            return planIndexingAsNonPrimary(index);
        }
    }

    @Override
    protected InternalEngine.DeletionStrategy deletionStrategyForOperation(final Delete delete) throws IOException {
        preFlight(delete);
        return planDeletionAsNonPrimary(delete);
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        // a noop implementation, because follow shard does not own the history but the leader shard does.
        return 0;
    }

    @Override
    protected boolean assertPrimaryIncomingSequenceNumber(final Operation.Origin origin, final long seqNo) {
        // sequence number should be set when operation origin is primary
        assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations on a following index must have an assigned sequence number";
        return true;
    }

    @Override
    protected boolean assertNonPrimaryOrigin(final Operation operation) {
        return true;
    }

    @Override
    protected boolean assertPrimaryCanOptimizeAddDocument(final Index index) {
        assert index.version() == 1 && index.versionType() == VersionType.EXTERNAL
                : "version [" + index.version() + "], type [" + index.versionType() + "]";
        return true;
    }

    /**
     * Returns the number of indexing operations that have been optimized (bypass version lookup) using sequence numbers in this engine.
     * This metric is not persisted, and started from 0 when the engine is opened.
     */
    public long getNumberOfOptimizedIndexing() {
        return numOfOptimizedIndexing.count();
    }
}
