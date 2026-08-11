/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Metrics for PIT (point-in-time) context relocation during stateless unpromotable shard recovery.
 * The five counters instrument the handoff from source to target node, making it possible
 * to detect how many contexts are offered, transferred, and ultimately re-created.
 */
public class PitRelocationMetrics {

    public static final String SOURCE_HANDOFF_COUNTER = "es.pit.relocation.source.handoff.total";
    public static final String SOURCE_CONTEXTS_COUNTER = "es.pit.relocation.source.contexts.total";
    public static final String TARGET_RESPONSE_COUNTER = "es.pit.relocation.target.response.total";
    public static final String TARGET_CONTEXTS_COUNTER = "es.pit.relocation.target.contexts.total";
    public static final String TARGET_READER_CONTEXT_COUNTER = "es.pit.relocation.target.reader_context.total";

    private final LongCounter sourceHandoffCounter;
    private final LongCounter sourceContextsCounter;
    private final LongCounter targetResponseCounter;
    private final LongCounter targetContextsCounter;
    private final LongCounter targetReaderContextCounter;

    public PitRelocationMetrics(MeterRegistry meterRegistry) {
        this.sourceHandoffCounter = meterRegistry.registerLongCounter(
            SOURCE_HANDOFF_COUNTER,
            "Number of times the handoff is initialed on the source node during PIT context relocation",
            "unit"
        );
        this.sourceContextsCounter = meterRegistry.registerLongCounter(
            SOURCE_CONTEXTS_COUNTER,
            "Number of pit context instances handed off by the source node during PIT context relocation",
            "unit"
        );
        this.targetResponseCounter = meterRegistry.registerLongCounter(
            TARGET_RESPONSE_COUNTER,
            "Number of times handoff messages are received on the target node during PIT context relocation",
            "unit"
        );
        this.targetContextsCounter = meterRegistry.registerLongCounter(
            TARGET_CONTEXTS_COUNTER,
            "Number of pit context instances handled by the target node during PIT context relocation",
            "unit"
        );
        this.targetReaderContextCounter = meterRegistry.registerLongCounter(
            TARGET_READER_CONTEXT_COUNTER,
            "Number of relocated ReaderContext instances created and added to the search service during PIT context relocation",
            "unit"
        );
    }

    /** Records a {@code doHandleStartHandoff} invocation on the source node. */
    public void recordSourceHandoff() {
        sourceHandoffCounter.incrementBy(1);
    }

    /** Records one {@code OpenPITContextInfo} handed off by the source node. */
    public void recordSourceContextCreated() {
        sourceContextsCounter.incrementBy(1);
    }

    /** Records a {@code handlePitHandoffResponse} invocation on the target node. */
    public void recordTargetResponseReceived() {
        targetResponseCounter.incrementBy(1);
    }

    /** Records one {@code OpenPITContextInfo} handled by the target node. */
    public void recordTargetContextHandled() {
        targetContextsCounter.incrementBy(1);
    }

    /** Records a relocated {@code ReaderContext} creation run on the target node. */
    public void recordTargetReaderContextCreated() {
        targetReaderContextCounter.incrementBy(1);
    }
}
