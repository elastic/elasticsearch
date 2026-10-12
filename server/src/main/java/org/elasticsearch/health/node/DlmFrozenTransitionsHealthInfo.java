/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the health of the DLM (data stream lifecycle) frozen-tier transition feature, as evaluated on the
 * elected master node.
 *
 * @param transitionsEnabled            Whether the DLM frozen transition feature is enabled. When {@code false}, no new
 *                                      transitions will be submitted, though in-flight transitions continue to completion.
 * @param serviceRunning                Whether the DLM frozen transition service's periodic scheduler is running on the
 *                                      current master. Detected via the scheduler's {@link java.util.concurrent.ScheduledFuture}:
 *                                      {@code isDone()} becomes {@code true} if the task dies from an unhandled
 *                                      {@link Error}, which {@code isShutdown()} on the executor cannot detect.
 * @param defaultRepositoryConfigured   Whether a default snapshot repository ({@code repositories.default_repository}) is
 *                                      configured. Without one, eligible indices cannot be marked for frozen conversion.
 * @param overdueIndices                A sample of overdue indices, keyed by project then index name, carrying their current
 *                                      transition state. The sample is informational only: up to
 *                                      {@code DLMFrozenTransitionHealthInfoPublisher.MAX_INDICES_TO_PUBLISH} entries are included
 *                                      per transition state, so every state that has a non-zero count in
 *                                      {@code overdueIndicesCountByState} will have example index names here. Diagnoses are driven
 *                                      by {@code overdueIndicesCountByState}, not by the presence of entries in this map.
 * @param totalOverdueIndicesCount      The total number of overdue indices found across all projects and states, regardless of
 *                                      whether they appear in the {@code overdueIndices} sample.
 * @param generatedAtMillis             Epoch-millisecond timestamp at which the master built this snapshot. Used to detect stale
 *                                      data (e.g. after a master failover before the new master has published its first snapshot).
 * @param publishIntervalMillis         The publisher's configured interval. The indicator treats the snapshot as stale when
 *                                      {@code now - generatedAtMillis > STALE_AFTER_PUBLISH_INTERVALS * publishIntervalMillis}.
 * @param overdueIndicesCountByState    The complete count of overdue indices per {@link TransitionState}, across all projects.
 *                                      Indices whose transition is already running are excluded from both this map and the sample.
 *                                      When this snapshot was read from a master running an older version (before transport version
 *                                      {@code dlm_frozen_transitions_health_state_counts}), counts are derived from the capped
 *                                      sample and may undercount during a rolling upgrade.
 */
public record DlmFrozenTransitionsHealthInfo(
    boolean transitionsEnabled,
    boolean serviceRunning,
    boolean defaultRepositoryConfigured,
    Map<ProjectId, Map<String, TransitionState>> overdueIndices,
    int totalOverdueIndicesCount,
    long generatedAtMillis,
    long publishIntervalMillis,
    Map<TransitionState, Integer> overdueIndicesCountByState
) implements Writeable {

    private static final TransportVersion DLM_FROZEN_TRANSITIONS_HEALTH_STATE_COUNTS = TransportVersion.fromName(
        "dlm_frozen_transitions_health_state_counts"
    );

    public DlmFrozenTransitionsHealthInfo {
        overdueIndices = overdueIndices.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue())));
        overdueIndicesCountByState = Map.copyOf(overdueIndicesCountByState);
    }

    /**
     * Reads a {@link DlmFrozenTransitionsHealthInfo} from the given stream. Use this as a method reference wherever
     * {@code readOptionalWriteable} or similar methods previously used {@code DlmFrozenTransitionsHealthInfo::new}.
     */
    public static DlmFrozenTransitionsHealthInfo readFrom(StreamInput in) throws IOException {
        boolean transitionsEnabled = in.readBoolean();
        boolean serviceRunning = in.readBoolean();
        boolean defaultRepositoryConfigured = in.readBoolean();
        Map<ProjectId, Map<String, TransitionState>> overdueIndices = in.readMap(
            ProjectId::readFrom,
            i -> i.readMap(v -> v.readEnum(TransitionState.class))
        );
        int totalOverdueIndicesCount = in.readVInt();
        long generatedAtMillis = in.readVLong();
        long publishIntervalMillis = in.readVLong();
        Map<TransitionState, Integer> overdueIndicesCountByState = in.getTransportVersion()
            .supports(DLM_FROZEN_TRANSITIONS_HEALTH_STATE_COUNTS)
                ? in.readMap(i -> i.readEnum(TransitionState.class), StreamInput::readVInt)
                : countByStateFromSample(overdueIndices);
        return new DlmFrozenTransitionsHealthInfo(
            transitionsEnabled,
            serviceRunning,
            defaultRepositoryConfigured,
            overdueIndices,
            totalOverdueIndicesCount,
            generatedAtMillis,
            publishIntervalMillis,
            overdueIndicesCountByState
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
        if (out.getTransportVersion().supports(DLM_FROZEN_TRANSITIONS_HEALTH_STATE_COUNTS)) {
            out.writeMap(overdueIndicesCountByState, StreamOutput::writeEnum, StreamOutput::writeVInt);
        }
    }

    private static Map<TransitionState, Integer> countByStateFromSample(Map<ProjectId, Map<String, TransitionState>> overdueIndices) {
        Map<TransitionState, Integer> counts = new EnumMap<>(TransitionState.class);
        overdueIndices.values().forEach(stateByIndex -> stateByIndex.values().forEach(state -> counts.merge(state, 1, Integer::sum)));
        return counts;
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
