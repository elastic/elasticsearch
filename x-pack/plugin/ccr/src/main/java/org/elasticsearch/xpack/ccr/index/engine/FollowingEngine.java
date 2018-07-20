/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

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
        return planIndexingAsNonPrimary(index);
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

}
