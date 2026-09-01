/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents the health of the DLM (data stream lifecycle) frozen-tier transition feature, as evaluated on the
 * elected master node.
 *
 * @param transitionsEnabled          Whether the DLM frozen transition feature is enabled. When {@code false}, no new
 *                                    transitions will be submitted, though in-flight transitions continue to completion.
 * @param serviceRunning              Whether the DLM frozen transition service's periodic scheduler is running on the
 *                                    current master. Detected via the scheduler's {@link java.util.concurrent.ScheduledFuture}:
 *                                    {@code isDone()} becomes {@code true} if the task dies from an unhandled
 *                                    {@link Error}, which {@code isShutdown()} on the executor cannot detect.
 * @param defaultRepositoryConfigured Whether a default snapshot repository ({@code repositories.default_repository}) is
 *                                    configured. Without one, eligible indices cannot be marked for frozen conversion.
 * @param markedIndicesCount          Total number of indices currently marked for frozen-tier conversion across all
 *                                    projects, regardless of lifecycle configuration. Drives the "transitions disabled
 *                                    but pending work" YELLOW signal.
 * @param eligibleUnmarked            Indices past their {@code frozen_after} age for longer than the configured stall
 *                                    threshold, but not yet marked — typically because no default repository is
 *                                    configured. {@link StalledIndices#totalCount()} may exceed the sample size.
 * @param notStartedMarked            Marked indices that have not been submitted to the transition executor and whose
 *                                    stall duration exceeds the threshold. The stall reference point is
 *                                    {@code max(eligibleSince, masterTenureStart)}, so this count intentionally resets
 *                                    for one threshold period after a master failover.
 * @param queuedMarked                Marked indices that have been submitted to the transition executor and are waiting
 *                                    in its queue, but have not started, for longer than the stall threshold.
 * @param generatedAtMillis           Epoch-millisecond timestamp at which the master built this snapshot. Used to
 *                                    detect stale data (e.g. after a master failover before the new master has
 *                                    published its first snapshot).
 * @param publishIntervalMillis       The publisher's configured interval. The indicator treats the snapshot as stale
 *                                    when {@code now - generatedAtMillis > STALE_AFTER_PUBLISH_INTERVALS * publishIntervalMillis}.
 */
public record DlmFrozenTransitionsHealthInfo(
    boolean transitionsEnabled,
    boolean serviceRunning,
    boolean defaultRepositoryConfigured,
    int markedIndicesCount,
    StalledIndices eligibleUnmarked,
    StalledIndices notStartedMarked,
    StalledIndices queuedMarked,
    long generatedAtMillis,
    long publishIntervalMillis
) implements Writeable {

    public DlmFrozenTransitionsHealthInfo(StreamInput in) throws IOException {
        this(
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean(),
            in.readVInt(),
            new StalledIndices(in),
            new StalledIndices(in),
            new StalledIndices(in),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(transitionsEnabled);
        out.writeBoolean(serviceRunning);
        out.writeBoolean(defaultRepositoryConfigured);
        out.writeVInt(markedIndicesCount);
        eligibleUnmarked.writeTo(out);
        notStartedMarked.writeTo(out);
        queuedMarked.writeTo(out);
        out.writeVLong(generatedAtMillis);
        out.writeVLong(publishIntervalMillis);
    }
}
