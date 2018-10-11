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
import java.util.Optional;

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
        // NOTES: refer Engine#getMaxSeqNoOfUpdatesOrDeletes for the explanation of the optimization using sequence numbers.
        final long maxSeqNoOfUpdatesOrDeletes = getMaxSeqNoOfUpdatesOrDeletes();
        assert maxSeqNoOfUpdatesOrDeletes != SequenceNumbers.UNASSIGNED_SEQ_NO : "max_seq_no_of_updates is not initialized";
        if (hasBeenProcessedBefore(index)) {
            if (index.origin() == Operation.Origin.PRIMARY) {
                /*
                 * The existing operation in this engine was probably assigned the term of the previous primary shard which is different
                 * from the term of the current operation. If the current operation arrives on replicas before the previous operation,
                 * then the Lucene content between the primary and replicas are not identical (primary terms are different). Since the
                 * existing operations are guaranteed to be replicated to replicas either via peer-recovery or primary-replica resync,
                 * we can safely skip this operation here and let the caller know the decision via AlreadyProcessedFollowingEngineException.
                 * The caller then waits for the global checkpoint to advance at least the seq_no of this operation to make sure that
                 * the existing operation was replicated to all replicas (see TransportBulkShardOperationsAction#shardOperationOnPrimary).
                 */
                final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(shardId, index.seqNo());
                return IndexingStrategy.skipDueToVersionConflict(error, false, index.version(), index.primaryTerm());
            } else {
                return IndexingStrategy.processButSkipLucene(false, index.seqNo(), index.version());
            }
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
        if (delete.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(delete)) {
            // See the comment in #indexingStrategyForOperation for the explanation why we can safely skip this operation.
            final AlreadyProcessedFollowingEngineException error = new AlreadyProcessedFollowingEngineException(shardId, delete.seqNo());
            return DeletionStrategy.skipDueToVersionConflict(error, delete.version(), delete.primaryTerm(), false);
        } else {
            return planDeletionAsNonPrimary(delete);
        }
    }

    @Override
    protected Optional<Exception> preFlightCheckForNoOp(NoOp noOp) {
        if (noOp.origin() == Operation.Origin.PRIMARY && hasBeenProcessedBefore(noOp)) {
            // See the comment in #indexingStrategyForOperation for the explanation why we can safely skip this operation.
            return Optional.of(new AlreadyProcessedFollowingEngineException(shardId, noOp.seqNo()));
        } else {
            return super.preFlightCheckForNoOp(noOp);
        }
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
