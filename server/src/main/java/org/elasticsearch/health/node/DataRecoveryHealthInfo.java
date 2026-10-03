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
import java.util.Map;

/**
 * Recovery point summaries computed on the elected master, the only node that can load repository data, and sent to the health node.
 *
 * @param projects          the summary of each project whose recovery repository could be read
 * @param generatedAtMillis when the master built this information, in epoch milliseconds
 */
public record DataRecoveryHealthInfo(Map<ProjectId, ProjectSummary> projects, long generatedAtMillis) implements Writeable {

    public DataRecoveryHealthInfo {
        projects = Map.copyOf(projects);
    }

    public DataRecoveryHealthInfo(StreamInput in) throws IOException {
        this(in.readImmutableMap(ProjectId::readFrom, ProjectSummary::new), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(projects, (o, projectId) -> projectId.writeTo(o), StreamOutput::writeWriteable);
        out.writeVLong(generatedAtMillis);
    }

    /**
     * The recovery points of one project, which are its completed snapshots, successful or partial, that are not being deleted.
     *
     * @param newestRecoveryPointStartMillis start time of the newest recovery point, or {@link #NONE}
     * @param oldestRecoveryPointStartMillis start time of the oldest recovery point, or {@link #NONE}
     * @param recoveryPointCount             number of recovery points
     * @param partialRecoveryPointCount      number of recovery points that are partial snapshots
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
