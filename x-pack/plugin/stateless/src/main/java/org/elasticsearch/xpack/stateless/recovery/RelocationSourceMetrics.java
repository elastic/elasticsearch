/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Per-phase durations measured on the source node during a stateless primary relocation: the initial best-effort flush,
 * acquiring all primary operation permits, the final flush, and the primary context handoff round-trip. These break down
 * the source-side portion of the relocation, which together with the target-side {@code es.recovery.shard.total.time}
 * metric describes the full handoff timeline.
 */
public record RelocationSourceMetrics(
    long initialFlushDurationInMillis,
    long acquirePermitsDurationInMillis,
    long secondFlushDurationInMillis,
    long handoffDurationInMillis
) implements Writeable {
    RelocationSourceMetrics(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(initialFlushDurationInMillis);
        out.writeVLong(acquirePermitsDurationInMillis);
        out.writeVLong(secondFlushDurationInMillis);
        out.writeVLong(handoffDurationInMillis);
    }

    static class Builder {
        long initialFlushDurationInMillis;
        long acquirePermitsDurationInMillis;
        long secondFlushDurationInMillis;
        long handoffDurationInMillis;

        void recordInitialFlushDuration(long durationInMillis) {
            this.initialFlushDurationInMillis = durationInMillis;
        }

        void recordAcquirePermitsDuration(long durationInMillis) {
            this.acquirePermitsDurationInMillis = durationInMillis;
        }

        void recordSecondFlushDuration(long durationInMillis) {
            this.secondFlushDurationInMillis = durationInMillis;
        }

        void recordHandoffDuration(long durationInMillis) {
            this.handoffDurationInMillis = durationInMillis;
        }

        RelocationSourceMetrics build() {
            return new RelocationSourceMetrics(
                initialFlushDurationInMillis,
                acquirePermitsDurationInMillis,
                secondFlushDurationInMillis,
                handoffDurationInMillis
            );
        }
    }
}
