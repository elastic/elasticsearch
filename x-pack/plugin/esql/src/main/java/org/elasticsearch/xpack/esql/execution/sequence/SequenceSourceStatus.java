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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A bounded snapshot of sequence search, matching and stage execution work. Child stage drivers execute candidate
 * pages; their cumulative CPU and iterations belong here because the parent driver measures only its own CPU.
 * Hits are counted before ordinary stage filtering or expansion. Values are retained at first read, while separate
 * stages or branches may still read the same event. Server-reported search took values include distributed work
 * and should not be added to driver CPU. The historical max-pending counter counts retained candidate slots,
 * including used markers; it is not a key count.
 */
public record SequenceSourceStatus(
    long candidateSearchRequests,
    long candidateRowsFetched,
    long completedMatches,
    long rowsEmitted,
    long stageDrivers,
    long stageDriverCpuNanos,
    long stageDriverIterations,
    long candidateSearchTookMillis,
    long maxPendingCandidates,
    LatestValidCounters latestValidCounters,
    RuntimeProfile runtime
) implements Operator.Status {
    /** Counters are mandatory even before a source has processed its first row. */
    public SequenceSourceStatus {
        Objects.requireNonNull(latestValidCounters);
    }

    private static final ProfileSchema<RuntimeProfile> RUNTIME_SCHEMA = ProfileSchema.of(
        RuntimeProfile.class,
        (builder, runtime, params) -> runtime.writeProfile(builder)
    )
        .required("search_wait_nanos", ProfileSchema.integer())
        .required("stage_run_nanos", ProfileSchema.integer())
        .required("candidate_build_nanos", ProfileSchema.integer())
        .required("matcher_nanos", ProfileSchema.integer())
        .required("condition_nanos", ProfileSchema.integer())
        .required("result_build_nanos", ProfileSchema.integer())
        .required("rows_fetched", ProfileSchema.array(ProfileSchema.integer()))
        .required("candidates_admitted", ProfileSchema.array(ProfileSchema.integer()))
        .required("peak_keys", ProfileSchema.integer())
        .required("peak_bytes_held", ProfileSchema.integer())
        .build();

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Operator.Status.class,
        "sequence_source",
        SequenceSourceStatus::readFrom
    );
    /** Describes the candidate reader and its optional human-readable times. */
    public static final ProfileSchema<SequenceSourceStatus> PROFILE_SCHEMA = ProfileSchema.of(
        SequenceSourceStatus.class,
        (builder, status, params) -> status.writeProfile(builder, params)
    )
        .required("candidate_search_requests", ProfileSchema.integer())
        .required("candidate_rows_fetched", ProfileSchema.integer())
        .required("completed_matches", ProfileSchema.integer())
        .required("rows_emitted", ProfileSchema.integer())
        .required("stage_drivers", ProfileSchema.integer())
        .required("stage_driver_cpu_nanos", ProfileSchema.integer())
        .optional("stage_driver_cpu_time", ProfileSchema.string())
        .required("stage_driver_iterations", ProfileSchema.integer())
        .required("candidate_search_took_millis", ProfileSchema.integer())
        .optional("candidate_search_took", ProfileSchema.string())
        .required("max_pending_candidates", ProfileSchema.integer())
        .required("candidates_dropped_at_bound", ProfileSchema.integer())
        .required("searches_abandoned_at_work_limit", ProfileSchema.integer())
        .required("keys_that_hit_bound", ProfileSchema.integer())
        .required("search_steps", ProfileSchema.integer())
        .required("max_search_steps_per_final_row", ProfileSchema.integer())
        .required("condition_evaluations", ProfileSchema.integer())
        .required("largest_ring_fill", ProfileSchema.integer())
        .optional("runtime", RUNTIME_SCHEMA.schema())
        .build();

    /** Restores only the aggregate snapshot, avoiding per-page histories in task and response profiles. */
    public static SequenceSourceStatus readFrom(StreamInput in) throws IOException {
        Sequence.checkPlanSupported(in.getTransportVersion());
        return new SequenceSourceStatus(
            readBeforeRetiredSlot(in),
            readBeforeRetiredSlot(in),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            readBeforeRetiredSlot(in),
            in.readVLong(),
            new LatestValidCounters(
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            ),
            in.readOptionalWriteable(RuntimeProfile::new)
        );
    }

    /** Historical status records are unframed. Consume the old zero after its preceding candidate counter. */
    private static long readBeforeRetiredSlot(StreamInput in) throws IOException {
        long value = in.readVLong();
        in.readVLong();
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkPlanSupported(out.getTransportVersion());
        out.writeVLong(candidateSearchRequests);
        out.writeVLong(0); // Retired wire slot.
        out.writeVLong(candidateRowsFetched);
        out.writeVLong(0); // Retired wire slot.
        out.writeVLong(completedMatches);
        out.writeVLong(rowsEmitted);
        out.writeVLong(stageDrivers);
        out.writeVLong(stageDriverCpuNanos);
        out.writeVLong(stageDriverIterations);
        out.writeVLong(candidateSearchTookMillis);
        out.writeVLong(0); // Retired wire slot.
        out.writeVLong(maxPendingCandidates);
        out.writeVLong(latestValidCounters.candidatesDroppedAtBound());
        out.writeVLong(latestValidCounters.searchesAbandonedAtWorkLimit());
        out.writeVLong(latestValidCounters.keysThatHitBound());
        out.writeVLong(latestValidCounters.searchSteps());
        out.writeVLong(latestValidCounters.maxSearchStepsPerFinalRow());
        out.writeVLong(latestValidCounters.conditionEvaluations());
        out.writeVLong(latestValidCounters.largestRingFill());
        out.writeOptionalWriteable(runtime);
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
        return candidateRowsFetched;
    }

    private XContentBuilder writeProfile(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("candidate_search_requests", candidateSearchRequests);
        builder.field("candidate_rows_fetched", candidateRowsFetched);
        builder.field("completed_matches", completedMatches);
        builder.field("rows_emitted", rowsEmitted);
        builder.field("stage_drivers", stageDrivers);
        builder.field("stage_driver_cpu_nanos", stageDriverCpuNanos);
        if (builder.humanReadable()) {
            builder.field("stage_driver_cpu_time", TimeValue.timeValueNanos(stageDriverCpuNanos).toString());
        }
        builder.field("stage_driver_iterations", stageDriverIterations);
        builder.field("candidate_search_took_millis", candidateSearchTookMillis);
        if (builder.humanReadable()) {
            builder.field("candidate_search_took", TimeValue.timeValueMillis(candidateSearchTookMillis).toString());
        }
        builder.field("max_pending_candidates", maxPendingCandidates);
        builder.field("candidates_dropped_at_bound", latestValidCounters.candidatesDroppedAtBound());
        builder.field("searches_abandoned_at_work_limit", latestValidCounters.searchesAbandonedAtWorkLimit());
        builder.field("keys_that_hit_bound", latestValidCounters.keysThatHitBound());
        builder.field("search_steps", latestValidCounters.searchSteps());
        builder.field("max_search_steps_per_final_row", latestValidCounters.maxSearchStepsPerFinalRow());
        builder.field("condition_evaluations", latestValidCounters.conditionEvaluations());
        builder.field("largest_ring_fill", latestValidCounters.largestRingFill());
        if (runtime != null) {
            builder.field("runtime");
            RUNTIME_SCHEMA.write(builder, runtime, params);
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

    /** Aggregate work only. Bound-hit keys count lifetimes, and fill measures a single stage/key ring. */
    public record LatestValidCounters(
        long candidatesDroppedAtBound,
        long searchesAbandonedAtWorkLimit,
        long keysThatHitBound,
        long searchSteps,
        long maxSearchStepsPerFinalRow,
        long conditionEvaluations,
        long largestRingFill
    ) {}

    /** Detailed measurements captured only when profiling is enabled. Matcher time includes condition time. */
    public record RuntimeProfile(
        long searchWaitNanos,
        long stageRunNanos,
        long candidateBuildNanos,
        long matcherNanos,
        long conditionNanos,
        long resultBuildNanos,
        List<Long> rowsFetched,
        List<Long> candidatesAdmitted,
        long peakKeys,
        long peakBytesHeld
    ) implements Writeable {
        public RuntimeProfile {
            rowsFetched = List.copyOf(rowsFetched);
            candidatesAdmitted = List.copyOf(candidatesAdmitted);
            if (rowsFetched.size() != candidatesAdmitted.size()) {
                throw new IllegalArgumentException("runtime stage counts must have the same size");
            }
            if (searchWaitNanos < 0
                || stageRunNanos < 0
                || candidateBuildNanos < 0
                || matcherNanos < 0
                || conditionNanos < 0
                || resultBuildNanos < 0
                || peakKeys < 0
                || peakBytesHeld < 0
                || rowsFetched.stream().anyMatch(value -> value < 0)
                || candidatesAdmitted.stream().anyMatch(value -> value < 0)) {
                throw new IllegalArgumentException("runtime measurements must be nonnegative");
            }
        }

        private RuntimeProfile(StreamInput in) throws IOException {
            this(
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readCollectionAsList(StreamInput::readVLong),
                in.readCollectionAsList(StreamInput::readVLong),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(searchWaitNanos);
            out.writeVLong(stageRunNanos);
            out.writeVLong(candidateBuildNanos);
            out.writeVLong(matcherNanos);
            out.writeVLong(conditionNanos);
            out.writeVLong(resultBuildNanos);
            out.writeCollection(rowsFetched, StreamOutput::writeVLong);
            out.writeCollection(candidatesAdmitted, StreamOutput::writeVLong);
            out.writeVLong(peakKeys);
            out.writeVLong(peakBytesHeld);
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("search_wait_nanos", searchWaitNanos);
            builder.field("stage_run_nanos", stageRunNanos);
            builder.field("candidate_build_nanos", candidateBuildNanos);
            builder.field("matcher_nanos", matcherNanos);
            builder.field("condition_nanos", conditionNanos);
            builder.field("result_build_nanos", resultBuildNanos);
            builder.field("rows_fetched", rowsFetched);
            builder.field("candidates_admitted", candidatesAdmitted);
            builder.field("peak_keys", peakKeys);
            builder.field("peak_bytes_held", peakBytesHeld);
            return builder.endObject();
        }
    }

}
