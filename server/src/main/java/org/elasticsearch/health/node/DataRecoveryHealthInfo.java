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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;

/**
 * Recovery point summaries that the elected master, the only node that can load repository data, sends to the health node.
 * <ul>
 *   <li>A project without recovery points is present, with {@link ProjectSummary#NONE} start times and zero counts.
 *   <li>A project that is absent has no summary available: it has no recovery repository configured, or its repository could not be
 *       read. Readers tell these apart from the project's own settings, and treat a configured project without a summary as unknown.
 *   <li>An empty map means a snapshot was published but no project has a summary.
 *   <li>A {@code null} {@link HealthInfo#dataRecoveryHealthInfo()} means nothing has been published yet.
 *   <li>The cache ignores {@code null} updates, so the publisher must send a snapshot every cycle.
 * </ul>
 *
 * @param projects              the summary of each project that has one
 * @param generatedAtMillis     when the master built this information, which must not be negative
 * @param publishIntervalMillis how often the publisher refreshes the snapshot, which must be positive; readers can treat the snapshot as
 *                              stale once {@code now - generatedAtMillis} exceeds a few multiples of it
 */
public record DataRecoveryHealthInfo(Map<ProjectId, ProjectSummary> projects, long generatedAtMillis, long publishIntervalMillis)
    implements
        Writeable {

    public DataRecoveryHealthInfo {
        if (generatedAtMillis < 0) {
            throw new IllegalArgumentException("generation time must not be negative, got [" + generatedAtMillis + "]");
        }
        if (publishIntervalMillis <= 0) {
            throw new IllegalArgumentException("publish interval must be positive, got [" + publishIntervalMillis + "]");
        }
        projects = Map.copyOf(projects);
    }

    public DataRecoveryHealthInfo(StreamInput in) throws IOException {
        this(in.readImmutableMap(ProjectId::readFrom, ProjectSummary::new), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(projects, (o, projectId) -> projectId.writeTo(o), StreamOutput::writeWriteable);
        out.writeVLong(generatedAtMillis);
        out.writeVLong(publishIntervalMillis);
    }

    /**
     * The recovery points of one project: its completed snapshots, successful or partial, that are not being deleted.
     *
     * @param newestRecoveryPointStartMillis start time of the newest recovery point, or {@link #NONE}
     * @param oldestRecoveryPointStartMillis start time of the oldest recovery point, or {@link #NONE}
     * @param recoveryPointCount             number of recovery points
     * @param partialRecoveryPointCount      number of partial snapshots among them
     * @param incompleteSourceCount          number of indices and data streams not fully captured in the newest recovery point
     */
    public record ProjectSummary(
        long newestRecoveryPointStartMillis,
        long oldestRecoveryPointStartMillis,
        int recoveryPointCount,
        int partialRecoveryPointCount,
        int incompleteSourceCount
    ) implements Writeable {

        /** Marks a start time that does not exist because the project has no recovery points. */
        public static final long NONE = -1L;

        public ProjectSummary {
            final boolean consistent = recoveryPointCount == 0
                ? newestRecoveryPointStartMillis == NONE
                    && oldestRecoveryPointStartMillis == NONE
                    && partialRecoveryPointCount == 0
                    && incompleteSourceCount == 0
                : recoveryPointCount > 0
                    && oldestRecoveryPointStartMillis >= 0
                    && oldestRecoveryPointStartMillis <= newestRecoveryPointStartMillis
                    && partialRecoveryPointCount >= 0
                    && partialRecoveryPointCount <= recoveryPointCount
                    && incompleteSourceCount >= 0;
            if (consistent == false) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "inconsistent recovery point summary: newest start [%d], oldest start [%d], recovery points [%d], partial [%d], "
                            + "incomplete sources [%d]",
                        newestRecoveryPointStartMillis,
                        oldestRecoveryPointStartMillis,
                        recoveryPointCount,
                        partialRecoveryPointCount,
                        incompleteSourceCount
                    )
                );
            }
        }

        public ProjectSummary(StreamInput in) throws IOException {
            this(in.readLong(), in.readLong(), in.readVInt(), in.readVInt(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(newestRecoveryPointStartMillis);
            out.writeLong(oldestRecoveryPointStartMillis);
            out.writeVInt(recoveryPointCount);
            out.writeVInt(partialRecoveryPointCount);
            out.writeVInt(incompleteSourceCount);
        }

        public boolean hasRecoveryPoints() {
            return recoveryPointCount > 0;
        }
    }
}
