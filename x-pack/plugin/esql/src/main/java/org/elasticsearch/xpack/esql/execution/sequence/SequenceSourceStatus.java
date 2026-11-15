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
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.core.Nullable;
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
    long maxPendingCandidates,
    @Nullable LatestValidCounters latestValidCounters
) implements Operator.Status {
    /** The unmerged sequence feature has one transport definition; private intermediate checkpoints are not wire peers. */
    public static final TransportVersion ESQL_SEQUENCE_LATEST_VALID_STATUS = TransportVersion.fromName(
        "esql_sequence_latest_valid_language"
    );

    /** Preserves callers and the exact profile shape for today's pairing. */
    public SequenceSourceStatus(
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
    ) {
        this(
            candidateSearchRequests,
            payloadSearchRequests,
            candidateRowsFetched,
            payloadRowsFetched,
            completedMatches,
            rowsEmitted,
            stageDrivers,
            stageDriverCpuNanos,
            stageDriverIterations,
            candidateSearchTookMillis,
            payloadSearchTookMillis,
            maxPendingCandidates,
            null
        );
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "sequence_source",
        SequenceSourceStatus::readFrom
    );
    /** Describes the existing counters and optional human-readable times without changing their JSON or transport form. */
    public static final ProfileSchema<SequenceSourceStatus> PROFILE_SCHEMA = ProfileSchema.of(
        SequenceSourceStatus.class,
        (builder, status, params) -> status.writeProfile(builder, params)
    )
        .required("candidate_search_requests", ProfileSchema.integer())
        .required("payload_search_requests", ProfileSchema.integer())
        .required("candidate_rows_fetched", ProfileSchema.integer())
        .required("payload_rows_fetched", ProfileSchema.integer())
        .required("completed_matches", ProfileSchema.integer())
        .required("rows_emitted", ProfileSchema.integer())
        .required("stage_drivers", ProfileSchema.integer())
        .required("stage_driver_cpu_nanos", ProfileSchema.integer())
        .optional("stage_driver_cpu_time", ProfileSchema.string())
        .required("stage_driver_iterations", ProfileSchema.integer())
        .required("candidate_search_took_millis", ProfileSchema.integer())
        .required("payload_search_took_millis", ProfileSchema.integer())
        .optional("candidate_search_took", ProfileSchema.string())
        .optional("payload_search_took", ProfileSchema.string())
        .required("max_pending_candidates", ProfileSchema.integer())
        .optional("candidates_dropped_at_bound", ProfileSchema.integer())
        .optional("searches_abandoned_at_work_limit", ProfileSchema.integer())
        .optional("keys_that_hit_bound", ProfileSchema.integer())
        .optional("search_steps", ProfileSchema.integer())
        .optional("max_search_steps_per_final_row", ProfileSchema.integer())
        .optional("condition_evaluations", ProfileSchema.integer())
        .optional("condition_false_or_null", ProfileSchema.integer())
        .optional("largest_ring_fill", ProfileSchema.integer())
        .optional("candidates_used_without_report", ProfileSchema.integer())
        .build();

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
            in.readVLong(),
            in.getTransportVersion().supports(ESQL_SEQUENCE_LATEST_VALID_STATUS) && in.readBoolean()
                ? new LatestValidCounters(
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong(),
                    in.readVLong()
                )
                : null
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
        if (out.getTransportVersion().supports(ESQL_SEQUENCE_LATEST_VALID_STATUS)) {
            out.writeBoolean(latestValidCounters != null);
            if (latestValidCounters != null) {
                out.writeVLong(latestValidCounters.candidatesDroppedAtBound());
                out.writeVLong(latestValidCounters.searchesAbandonedAtWorkLimit());
                out.writeVLong(latestValidCounters.keysThatHitBound());
                out.writeVLong(latestValidCounters.searchSteps());
                out.writeVLong(latestValidCounters.maxSearchStepsPerFinalRow());
                out.writeVLong(latestValidCounters.conditionEvaluations());
                out.writeVLong(latestValidCounters.conditionFalseOrNull());
                out.writeVLong(latestValidCounters.largestRingFill());
                out.writeVLong(latestValidCounters.candidatesUsedWithoutReport());
            }
        }
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

    private XContentBuilder writeProfile(XContentBuilder builder, Params params) throws IOException {
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
        if (latestValidCounters != null) {
            builder.field("candidates_dropped_at_bound", latestValidCounters.candidatesDroppedAtBound());
            builder.field("searches_abandoned_at_work_limit", latestValidCounters.searchesAbandonedAtWorkLimit());
            builder.field("keys_that_hit_bound", latestValidCounters.keysThatHitBound());
            builder.field("search_steps", latestValidCounters.searchSteps());
            builder.field("max_search_steps_per_final_row", latestValidCounters.maxSearchStepsPerFinalRow());
            builder.field("condition_evaluations", latestValidCounters.conditionEvaluations());
            builder.field("condition_false_or_null", latestValidCounters.conditionFalseOrNull());
            builder.field("largest_ring_fill", latestValidCounters.largestRingFill());
            builder.field("candidates_used_without_report", latestValidCounters.candidatesUsedWithoutReport());
        }
        return builder.endObject();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return PROFILE_SCHEMA.write(builder, this, params);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Scalar-only latest-valid work. Used-without-report counts direct substitute rows, excluding rows marked
     * solely because another stage consumed their document. Bound-hit keys count lifetimes and fill is one ring.
     */
    public record LatestValidCounters(
        long candidatesDroppedAtBound,
        long searchesAbandonedAtWorkLimit,
        long keysThatHitBound,
        long searchSteps,
        long maxSearchStepsPerFinalRow,
        long conditionEvaluations,
        long conditionFalseOrNull,
        long largestRingFill,
        long candidatesUsedWithoutReport
    ) {}
}
