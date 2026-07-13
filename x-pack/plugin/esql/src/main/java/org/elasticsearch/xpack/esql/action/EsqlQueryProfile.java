/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks profiling for the planning phase
 */
public class EsqlQueryProfile implements Writeable, ToXContentFragment {

    public static final String QUERY = "query";
    public static final String PLANNING = "planning";
    public static final String PARSING = "parsing";
    public static final String VIEW_RESOLUTION = "view_resolution";
    public static final String DATASET_RESOLUTION = "dataset_resolution";
    public static final String PRE_ANALYSIS = "preanalysis";
    public static final String INDICES_RESOLUTION = "indices_resolution";
    public static final String ENRICH_RESOLUTION = "enrich_resolution";
    public static final String INFERENCE_RESOLUTION = "inference_resolution";
    public static final String ANALYSIS = "analysis";

    /** Time elapsed since start of query till the final result rendering */
    private final TimeSpanMarker totalMarker;
    /** Time elapsed since start of query to calling ComputeService.execute */
    private final TimeSpanMarker planningMarker;
    /** Time elapsed for query parsing */
    private final TimeSpanMarker parsingMarker;
    /** Time elapsed for resolving views in the logical plan */
    private final TimeSpanMarker viewResolutionMarker;
    /** Time elapsed for rewriting datasets in the logical plan */
    private final TimeSpanMarker datasetResolutionMarker;
    /** Time elapsed for index preanalysis, including lookup indices */
    private final TimeSpanMarker preAnalysisMarker;
    /** Time elapsed for resolving indices dependencies */
    private final TimeSpanMarker indicesResolutionMarker;
    /** Time elapsed for resolving enrich dependencies */
    private final TimeSpanMarker enrichResolutionMarker;
    /** Time elapsed for resolving inference dependencies */
    private final TimeSpanMarker inferenceResolutionMarker;
    /** Time elapsed for plan analysis */
    private final TimeSpanMarker analysisMarker;
    private final AtomicInteger fieldCapsCalls;
    /** Distinct external files scanned after coordinator-side pruning (file-based sources only). */
    private final AtomicInteger filesScanned;
    /** Total external splits scanned across all external sources. */
    private final AtomicInteger splitsScanned;
    /** Estimated bytes scanned across the discovered external splits. */
    private final AtomicLong bytesScanned;
    /** The query-level unmapped field resolution mode. */
    private volatile UnmappedResolution unmappedResolution;
    /**
     * Number of external relations whose ungrouped aggregate was served <em>warm</em> — answered purely
     * from canonical-stripe / whole-file statistics with the data scan short-circuited away (split
     * discovery skipped, {@code AggregateExec -> ExternalSourceExec} rewritten to a constant
     * {@code LocalSourceExec}). A positive value is the affirmative "served from stripes" profiling
     * signal: it lets a profile reader distinguish a warm short-circuit (this counter {@code > 0},
     * scan counters zero) from a cold scan (scan counters {@code > 0}) without inferring from latency.
     */
    private final AtomicInteger externalWarmAggregates;

    private static final TransportVersion ESQL_QUERY_PLANNING_PROFILE = TransportVersion.fromName("esql_query_planning_profile");
    private static final TransportVersion ESQL_QUERY_PROFILE_VIEW_RESOLUTION = TransportVersion.fromName(
        "esql_query_profile_view_resolution"
    );
    private static final TransportVersion ESQL_EXTERNAL_SOURCE_PROFILE = TransportVersion.fromName("esql_external_source_profile");
    private static final TransportVersion ESQL_SEPARATE_DEPENDENCY_RESOLUTION = TransportVersion.fromName(
        "esql_separate_dependency_resolution"
    );
    private static final TransportVersion ESQL_EXTERNAL_SCAN_PROFILE = TransportVersion.fromName("esql_external_scan_profile");
    private static final TransportVersion ESQL_PROFILE_UNMAPPED_FIELDS_MODE = TransportVersion.fromName("esql_vsr_source_load_profile");
    private static final TransportVersion ESQL_EXTERNAL_WARM_AGGREGATE_PROFILE = TransportVersion.fromName(
        "esql_external_warm_aggregate_profile"
    );

    public EsqlQueryProfile() {
        this(null, null, null, null, null, null, null, null, null, null, 0, 0, 0, 0L, UnmappedResolution.DEFAULT, 0);
    }

    // For testing
    public EsqlQueryProfile(
        TimeSpan query,
        TimeSpan planning,
        TimeSpan parsing,
        TimeSpan viewResolution,
        TimeSpan datasetResolution,
        TimeSpan preAnalysis,
        TimeSpan indicesResolution,
        TimeSpan enrichResolution,
        TimeSpan inferenceResolution,
        TimeSpan analysis,
        int fieldCapsCalls,
        int filesScanned,
        int splitsScanned,
        long bytesScanned,
        UnmappedResolution unmappedResolution,
        int externalWarmAggregates
    ) {
        this.totalMarker = new TimeSpanMarker(QUERY, true, query);
        this.planningMarker = new TimeSpanMarker(PLANNING, false, planning);
        this.parsingMarker = new TimeSpanMarker(PARSING, false, parsing);
        this.viewResolutionMarker = new TimeSpanMarker(VIEW_RESOLUTION, false, viewResolution);
        this.datasetResolutionMarker = new TimeSpanMarker(DATASET_RESOLUTION, false, datasetResolution);
        this.preAnalysisMarker = new TimeSpanMarker(PRE_ANALYSIS, false, preAnalysis);
        this.indicesResolutionMarker = new TimeSpanMarker(INDICES_RESOLUTION, true, indicesResolution);
        this.enrichResolutionMarker = new TimeSpanMarker(ENRICH_RESOLUTION, true, enrichResolution);
        this.inferenceResolutionMarker = new TimeSpanMarker(INFERENCE_RESOLUTION, true, inferenceResolution);
        this.analysisMarker = new TimeSpanMarker(ANALYSIS, true, analysis);
        this.fieldCapsCalls = new AtomicInteger(fieldCapsCalls);
        this.filesScanned = new AtomicInteger(filesScanned);
        this.splitsScanned = new AtomicInteger(splitsScanned);
        this.bytesScanned = new AtomicLong(bytesScanned);
        this.unmappedResolution = unmappedResolution;
        this.externalWarmAggregates = new AtomicInteger(externalWarmAggregates);
    }

    public static EsqlQueryProfile readFrom(StreamInput in) throws IOException {
        TimeSpan query = in.readOptionalWriteable(TimeSpan::readFrom);
        TimeSpan planning = in.readOptionalWriteable(TimeSpan::readFrom);
        TimeSpan parsing = null;
        TimeSpan viewResolution = null;
        TimeSpan datasetResolution = null;
        TimeSpan preAnalysis = null;
        TimeSpan indicesResolution = null;
        TimeSpan enrichResolution = null;
        TimeSpan inferenceResolution = null;
        TimeSpan analysis = null;
        int fieldCapsCalls = 0;
        if (in.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            parsing = in.readOptionalWriteable(TimeSpan::readFrom);
            if (in.getTransportVersion().supports(ESQL_QUERY_PROFILE_VIEW_RESOLUTION)) {
                viewResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            }
            if (in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                datasetResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            }
            preAnalysis = in.readOptionalWriteable(TimeSpan::readFrom);
            indicesResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            if (in.getTransportVersion().supports(ESQL_SEPARATE_DEPENDENCY_RESOLUTION)) {
                enrichResolution = in.readOptionalWriteable(TimeSpan::readFrom);
                inferenceResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            }
            analysis = in.readOptionalWriteable(TimeSpan::readFrom);
        }
        if (in.getTransportVersion().supports(EsqlExecutionInfo.EXECUTION_PROFILE_FORMAT_VERSION)) {
            fieldCapsCalls = in.readVInt();
        }
        int filesScanned = 0;
        int splitsScanned = 0;
        long bytesScanned = 0L;
        if (in.getTransportVersion().supports(ESQL_EXTERNAL_SCAN_PROFILE)) {
            filesScanned = in.readVInt();
            splitsScanned = in.readVInt();
            bytesScanned = in.readVLong();
        }
        UnmappedResolution unmappedResolution = UnmappedResolution.DEFAULT;
        if (in.getTransportVersion().supports(ESQL_PROFILE_UNMAPPED_FIELDS_MODE)) {
            unmappedResolution = in.readEnum(UnmappedResolution.class);
        }
        int externalWarmAggregates = 0;
        if (in.getTransportVersion().supports(ESQL_EXTERNAL_WARM_AGGREGATE_PROFILE)) {
            externalWarmAggregates = in.readVInt();
        }
        return new EsqlQueryProfile(
            query,
            planning,
            parsing,
            viewResolution,
            datasetResolution,
            preAnalysis,
            indicesResolution,
            enrichResolution,
            inferenceResolution,
            analysis,
            fieldCapsCalls,
            filesScanned,
            splitsScanned,
            bytesScanned,
            unmappedResolution,
            externalWarmAggregates
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(totalMarker.timeSpan());
        out.writeOptionalWriteable(planningMarker.timeSpan());
        if (out.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            out.writeOptionalWriteable(parsingMarker.timeSpan());
            if (out.getTransportVersion().supports(ESQL_QUERY_PROFILE_VIEW_RESOLUTION)) {
                out.writeOptionalWriteable(viewResolutionMarker.timeSpan());
            }
            if (out.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_PROFILE)) {
                out.writeOptionalWriteable(datasetResolutionMarker.timeSpan());
            }
            out.writeOptionalWriteable(preAnalysisMarker.timeSpan());
            if (out.getTransportVersion().supports(ESQL_SEPARATE_DEPENDENCY_RESOLUTION)) {
                out.writeOptionalWriteable(indicesResolutionMarker.timeSpan());
                out.writeOptionalWriteable(enrichResolutionMarker.timeSpan());
                out.writeOptionalWriteable(inferenceResolutionMarker.timeSpan());
            } else {
                out.writeOptionalWriteable(
                    TimeSpan.combine(
                        indicesResolutionMarker.timeSpan(),
                        enrichResolutionMarker.timeSpan(),
                        inferenceResolutionMarker.timeSpan()
                    )
                );
            }
            out.writeOptionalWriteable(analysisMarker.timeSpan());
        }
        if (out.getTransportVersion().supports(EsqlExecutionInfo.EXECUTION_PROFILE_FORMAT_VERSION)) {
            out.writeVInt(fieldCapsCalls.get());
        }
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_SCAN_PROFILE)) {
            out.writeVInt(filesScanned.get());
            out.writeVInt(splitsScanned.get());
            out.writeVLong(bytesScanned.get());
        }
        if (out.getTransportVersion().supports(ESQL_PROFILE_UNMAPPED_FIELDS_MODE)) {
            out.writeEnum(unmappedResolution);
        }
        if (out.getTransportVersion().supports(ESQL_EXTERNAL_WARM_AGGREGATE_PROFILE)) {
            out.writeVInt(externalWarmAggregates.get());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        EsqlQueryProfile that = (EsqlQueryProfile) o;
        return Objects.equals(totalMarker, that.totalMarker)
            && Objects.equals(planningMarker, that.planningMarker)
            && Objects.equals(parsingMarker, that.parsingMarker)
            && Objects.equals(viewResolutionMarker, that.viewResolutionMarker)
            && Objects.equals(datasetResolutionMarker, that.datasetResolutionMarker)
            && Objects.equals(preAnalysisMarker, that.preAnalysisMarker)
            && Objects.equals(indicesResolutionMarker, that.indicesResolutionMarker)
            && Objects.equals(enrichResolutionMarker, that.enrichResolutionMarker)
            && Objects.equals(inferenceResolutionMarker, that.inferenceResolutionMarker)
            && Objects.equals(analysisMarker, that.analysisMarker)
            && Objects.equals(fieldCapsCalls.get(), that.fieldCapsCalls.get())
            && filesScanned.get() == that.filesScanned.get()
            && splitsScanned.get() == that.splitsScanned.get()
            && bytesScanned.get() == that.bytesScanned.get()
            && unmappedResolution == that.unmappedResolution
            && externalWarmAggregates.get() == that.externalWarmAggregates.get();
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalMarker,
            planningMarker,
            parsingMarker,
            viewResolutionMarker,
            datasetResolutionMarker,
            preAnalysisMarker,
            indicesResolutionMarker,
            enrichResolutionMarker,
            inferenceResolutionMarker,
            analysisMarker,
            fieldCapsCalls.get(),
            filesScanned.get(),
            splitsScanned.get(),
            bytesScanned.get(),
            unmappedResolution,
            externalWarmAggregates.get()
        );
    }

    @Override
    public String toString() {
        return "EsqlQueryProfile{"
            + "totalMarker="
            + totalMarker
            + ", planningMarker="
            + planningMarker
            + ", parsingMarker="
            + parsingMarker
            + ", viewResolutionMarker="
            + viewResolutionMarker
            + ", datasetResolutionMarker="
            + datasetResolutionMarker
            + ", preAnalysisMarker="
            + preAnalysisMarker
            + ", indicesResolutionMarker="
            + indicesResolutionMarker
            + ", enrichResolutionMarker="
            + enrichResolutionMarker
            + ", inferenceResolutionMarker="
            + inferenceResolutionMarker
            + ", analysisMarker="
            + analysisMarker
            + ", fieldCapsCalls="
            + fieldCapsCalls.get()
            + ", filesScanned="
            + filesScanned.get()
            + ", splitsScanned="
            + splitsScanned.get()
            + ", bytesScanned="
            + bytesScanned.get()
            + ", unmappedResolution="
            + unmappedResolution
            + ", externalWarmAggregates="
            + externalWarmAggregates.get()
            + '}';
    }

    public EsqlQueryProfile start() {
        totalMarker.start();
        return this;
    }

    public EsqlQueryProfile stop() {
        totalMarker.stop();
        return this;
    }

    public TimeSpanMarker total() {
        return totalMarker;
    }

    /**
     * Span for the ES|QL "planning" phase - when it is complete, query execution (in ComputeService) is about to start.
     * Note this is currently only built for a single phase planning/execution model. When INLINE STATS
     * moves towards GA we may need to revisit this model. Currently, it should never be called more than once.
     */
    public TimeSpanMarker planning() {
        return planningMarker;
    }

    /**
     * Span for the parsing phase, that covers the ES|QL query parsing
     */
    public TimeSpanMarker parsing() {
        return parsingMarker;
    }

    /**
     * Span for resolving views in the logical plan (between parsing and pre-analysis).
     */
    public TimeSpanMarker viewResolution() {
        return viewResolutionMarker;
    }

    /**
     * Span for rewriting datasets in the logical plan (between view resolution and pre-analysis).
     */
    public TimeSpanMarker datasetResolution() {
        return datasetResolutionMarker;
    }

    /**
     * Span for the preanalysis phase
     */
    public TimeSpanMarker preAnalysis() {
        return preAnalysisMarker;
    }

    public TimeSpanMarker indicesResolutionMarker() {
        return indicesResolutionMarker;
    }

    public TimeSpanMarker enrichResolutionMarker() {
        return enrichResolutionMarker;
    }

    public TimeSpanMarker inferenceResolutionMarker() {
        return inferenceResolutionMarker;
    }

    /**
     * Span for the plan analysis phase - this does not include plan optimizations, which come later and are part of each individual
     * plan profiling
     */
    public TimeSpanMarker analysis() {
        return analysisMarker;
    }

    public int fieldCapsCalls() {
        return fieldCapsCalls.get();
    }

    public void incFieldCapsCalls() {
        fieldCapsCalls.incrementAndGet();
    }

    public int filesScanned() {
        return filesScanned.get();
    }

    public int splitsScanned() {
        return splitsScanned.get();
    }

    public long bytesScanned() {
        return bytesScanned.get();
    }

    public int externalWarmAggregates() {
        return externalWarmAggregates.get();
    }

    /**
     * Records the post-prune external scan accounting discovered for the query. Adds to any
     * previously recorded counts so multiple split-discovery paths can contribute.
     */
    public void addExternalScanStats(int files, int splits, long bytes) {
        filesScanned.addAndGet(files);
        splitsScanned.addAndGet(splits);
        bytesScanned.addAndGet(bytes);
    }

    /**
     * Records that {@code count} external relations were served warm — their ungrouped aggregate was
     * answered from canonical-stripe / whole-file statistics with the data scan short-circuited away.
     * Recorded at split-discovery time on the coordinator, where the short-circuit decision is made
     * (see {@code ComputeService.canSkipSplitDiscovery}); no scan operator runs for a warm relation, so
     * this is the only place the "served from stripes" signal is observable.
     */
    public void addExternalWarmAggregates(int count) {
        externalWarmAggregates.addAndGet(count);
    }

    public Collection<TimeSpanMarker> timeSpanMarkers() {
        return List.of(
            totalMarker,
            planningMarker,
            parsingMarker,
            viewResolutionMarker,
            datasetResolutionMarker,
            preAnalysisMarker,
            indicesResolutionMarker,
            enrichResolutionMarker,
            inferenceResolutionMarker,
            analysisMarker
        );
    }

    public void setUnmappedResolution(UnmappedResolution unmappedResolution) {
        this.unmappedResolution = unmappedResolution;
    }

    public UnmappedResolution unmappedResolution() {
        return unmappedResolution;
    }

    /**
     * Safely stops all markers that were started but not yet stopped.
     * This is useful in error paths where we need to ensure all timing data is captured.
     */
    public void stopAllStartedMarkers() {
        for (TimeSpanMarker marker : timeSpanMarkers()) {
            marker.stopIfStarted();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimeSpanMarker timeSpanMarker : timeSpanMarkers()) {
            builder.field(timeSpanMarker.name(), timeSpanMarker.timeSpan());
        }
        builder.field("field_caps_calls", fieldCapsCalls.get());
        // Only emit external scan accounting for queries that actually scanned an external source.
        // files_scanned and bytes_scanned are source-specific; omit them when the source cannot
        // report them (e.g. connector sources like Arrow Flight have no file or byte accounting).
        int splits = splitsScanned.get();
        if (splits > 0) {
            int files = filesScanned.get();
            if (files > 0) {
                builder.field("files_scanned", files);
            }
            builder.field("splits_scanned", splits);
            long bytes = bytesScanned.get();
            if (bytes > 0) {
                builder.field("bytes_scanned", bytes);
            }
        }
        builder.field("unmapped_fields", unmappedResolution.name().toLowerCase(Locale.ROOT));
        // The affirmative warm signal: emitted only when at least one external aggregate was served from
        // statistics with the scan short-circuited away. Its presence (with the scan counters above
        // absent/zero) is what distinguishes a warm short-circuit from a cold scan without inferring from
        // latency.
        int warm = externalWarmAggregates.get();
        if (warm > 0) {
            builder.field("external_warm_aggregates", warm);
        }
        return builder;
    }

}
