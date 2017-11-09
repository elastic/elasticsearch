/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
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

    @Override
    public IndexResult index(final Index index) throws IOException {
        preFlight(index);
        return super.index(index);
    }

    @Override
    public DeleteResult delete(final Delete delete) throws IOException {
        preFlight(delete);
        return super.delete(delete);
    }

    private void preFlight(final Operation operation) {
        if (operation.origin() == Operation.Origin.PRIMARY) {
            throw new IllegalStateException("a following engine does not accept primary operations");
        }
    }

}
