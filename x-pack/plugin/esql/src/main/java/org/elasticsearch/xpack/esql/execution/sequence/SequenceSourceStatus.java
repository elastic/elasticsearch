/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.io.IOException;

/**
 * A bounded snapshot of native sequence search, matching and stage execution work. Child stages create separate
 * drivers for candidate pages and selected payload; their cumulative CPU and iterations belong here because the
 * parent driver's CPU measures only its own work. Hits are counted before ordinary stage filtering or expansion,
 * including repeated reads of selected events. Server-reported search took values include distributed work and
 * should not be added to driver CPU. Pending candidates count live matcher prefixes, rather than distinct keys.
 */
public record SequenceSourceStatus(
    long candidateSearchRequests,
    long payloadSearchRequests,
    long candidateRowsFetched,
    long payloadRowsFetched,
    long completedMatches,
    long rowsEmitted,
    long stageDrivers,
    long stageDriverCpuNanos,
    long stageDriverIterations,
    long candidateSearchTookMillis,
    long payloadSearchTookMillis,
    long maxPendingCandidates
) implements Operator.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "sequence_source",
        SequenceSourceStatus::readFrom
    );

    /** Restores only the aggregate snapshot, avoiding per-page histories in task and response profiles. */
    public static SequenceSourceStatus readFrom(StreamInput in) throws IOException {
        Sequence.checkSupported(in.getTransportVersion());
        return new SequenceSourceStatus(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkSupported(out.getTransportVersion());
        out.writeVLong(candidateSearchRequests);
        out.writeVLong(payloadSearchRequests);
        out.writeVLong(candidateRowsFetched);
        out.writeVLong(payloadRowsFetched);
        out.writeVLong(completedMatches);
        out.writeVLong(rowsEmitted);
        out.writeVLong(stageDrivers);
        out.writeVLong(stageDriverCpuNanos);
        out.writeVLong(stageDriverIterations);
        out.writeVLong(candidateSearchTookMillis);
        out.writeVLong(payloadSearchTookMillis);
        out.writeVLong(maxPendingCandidates);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return Sequence.ESQL_SEQUENCE;
    }

    @Override
    public long documentsFound() {
        return candidateRowsFetched + payloadRowsFetched;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("candidate_search_requests", candidateSearchRequests);
        builder.field("payload_search_requests", payloadSearchRequests);
        builder.field("candidate_rows_fetched", candidateRowsFetched);
        builder.field("payload_rows_fetched", payloadRowsFetched);
        builder.field("completed_matches", completedMatches);
        builder.field("rows_emitted", rowsEmitted);
        builder.field("stage_drivers", stageDrivers);
        builder.field("stage_driver_cpu_nanos", stageDriverCpuNanos);
        if (builder.humanReadable()) {
            builder.field("stage_driver_cpu_time", TimeValue.timeValueNanos(stageDriverCpuNanos).toString());
        }
        builder.field("stage_driver_iterations", stageDriverIterations);
        builder.field("candidate_search_took_millis", candidateSearchTookMillis);
        builder.field("payload_search_took_millis", payloadSearchTookMillis);
        if (builder.humanReadable()) {
            builder.field("candidate_search_took", TimeValue.timeValueMillis(candidateSearchTookMillis).toString());
            builder.field("payload_search_took", TimeValue.timeValueMillis(payloadSearchTookMillis).toString());
        }
        builder.field("max_pending_candidates", maxPendingCandidates);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
