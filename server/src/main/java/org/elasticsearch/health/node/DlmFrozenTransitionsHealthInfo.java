/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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
 * @param overdueIndices              A capped sample of indices, keyed by project then index name, that are past their
 *                                    {@code frozen_after} age by more than the configured stuck threshold and have not
 *                                    completed their frozen-tier transition, together with their current transition
 *                                    state. {@code totalOverdueIndicesCount} may exceed the number of entries here.
 * @param totalOverdueIndicesCount    The total number of overdue indices found across all projects, regardless of
 *                                    whether they fit in {@code overdueIndices}.
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
    Map<ProjectId, Map<String, TransitionState>> overdueIndices,
    int totalOverdueIndicesCount,
    long generatedAtMillis,
    long publishIntervalMillis
) implements Writeable {

    public DlmFrozenTransitionsHealthInfo {
        overdueIndices = overdueIndices.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue())));
    }

    public DlmFrozenTransitionsHealthInfo(StreamInput in) throws IOException {
        this(
            in.readBoolean(),
            in.readBoolean(),
            in.readBoolean(),
            in.readMap(ProjectId::readFrom, i -> i.readMap(v -> v.readEnum(TransitionState.class))),
            in.readVInt(),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(transitionsEnabled);
        out.writeBoolean(serviceRunning);
        out.writeBoolean(defaultRepositoryConfigured);
        out.writeMap(overdueIndices, (o, id) -> id.writeTo(o), (o, m) -> o.writeMap(m, StreamOutput::writeEnum));
        out.writeVInt(totalOverdueIndicesCount);
        out.writeVLong(generatedAtMillis);
        out.writeVLong(publishIntervalMillis);
    }

    /**
     * The transition state of an overdue index, as tracked by the transition executor on the current master node.
     * {@code UNMARKED} and {@code MARKED} are derived from durable cluster state; {@code QUEUED} and {@code RUNNING}
     * are best-effort and reset to {@code MARKED} across a master failover.
     */
    public enum TransitionState {
        UNMARKED,
        MARKED,
        QUEUED,
        RUNNING;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
