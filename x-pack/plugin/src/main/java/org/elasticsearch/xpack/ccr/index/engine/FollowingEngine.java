/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.xpack.ccr.CcrSettings;

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

    @Override
    protected long doGenerateSeqNoForOperation(final Operation operation) {
        assert operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                : "primary operations on following indices must have an assigned sequence number";
        return operation.seqNo();
    }

    @Override
    protected boolean assertOriginPrimarySequenceNumber(final long seqNo) {
        assert seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO : "primary operations on following indices must have an assigned sequence number";
        return true;
    }

}
