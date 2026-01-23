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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks profiling for the planning phase
 */
public class EsqlQueryProfile implements Writeable, ToXContentFragment {

    public static final String QUERY = "query";
    public static final String PLANNING = "planning";
    public static final String PARSING = "parsing";
    public static final String PRE_ANALYSIS = "preanalysis";
    public static final String DEPENDENCY_RESOLUTION = "dependency_resolution";
    public static final String ANALYSIS = "analysis";

    /** Time elapsed since start of query till the final result rendering */
    private final TimeSpanMarker totalMarker;
    /** Time elapsed since start of query to calling ComputeService.execute */
    private final TimeSpanMarker planningMarker;
    /** Time elapsed for query parsing */
    private final TimeSpanMarker parsingMarker;
    /** Time elapsed for index preanalysis, including lookup indices */
    private final TimeSpanMarker preAnalysisMarker;
    /** Time elapsed for checking dependencies (field_caps, enrich policies, inference ids) */
    private final TimeSpanMarker dependencyResolutionMarker;
    /** Time elapsed for plan analysis */
    private final TimeSpanMarker analysisMarker;
    private final AtomicInteger fieldCapsCalls;

    private static final TransportVersion ESQL_QUERY_PLANNING_PROFILE = TransportVersion.fromName("esql_query_planning_profile");

    public EsqlQueryProfile() {
        this(null, null, null, null, null, null, 0);
    }

    // For testing
    public EsqlQueryProfile(
        TimeSpan query,
        TimeSpan planning,
        TimeSpan parsing,
        TimeSpan preAnalysis,
        TimeSpan dependencyResolution,
        TimeSpan analysis,
        int fieldCapsCalls
    ) {
        this.totalMarker = new TimeSpanMarker(QUERY, true, query);
        this.planningMarker = new TimeSpanMarker(PLANNING, false, planning);
        this.parsingMarker = new TimeSpanMarker(PARSING, false, parsing);
        this.preAnalysisMarker = new TimeSpanMarker(PRE_ANALYSIS, false, preAnalysis);
        this.dependencyResolutionMarker = new TimeSpanMarker(DEPENDENCY_RESOLUTION, true, dependencyResolution);
        this.analysisMarker = new TimeSpanMarker(ANALYSIS, true, analysis);
        this.fieldCapsCalls = new AtomicInteger(fieldCapsCalls);
    }

    public static EsqlQueryProfile readFrom(StreamInput in) throws IOException {
        TimeSpan query = in.readOptionalWriteable(TimeSpan::readFrom);
        TimeSpan planning = in.readOptionalWriteable(TimeSpan::readFrom);
        TimeSpan parsing = null, preAnalysis = null, dependencyResolution = null, analysis = null;
        int fieldCapsCalls = 0;
        if (in.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            parsing = in.readOptionalWriteable(TimeSpan::readFrom);
            preAnalysis = in.readOptionalWriteable(TimeSpan::readFrom);
            dependencyResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            analysis = in.readOptionalWriteable(TimeSpan::readFrom);
        }
        if (in.getTransportVersion().supports(EsqlExecutionInfo.EXECUTION_PROFILE_FORMAT_VERSION)) {
            fieldCapsCalls = in.readVInt();
        }
        return new EsqlQueryProfile(query, planning, parsing, preAnalysis, dependencyResolution, analysis, fieldCapsCalls);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(totalMarker.timeSpan());
        out.writeOptionalWriteable(planningMarker.timeSpan());
        if (out.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            out.writeOptionalWriteable(parsingMarker == null ? null : parsingMarker.timeSpan());
            out.writeOptionalWriteable(preAnalysisMarker == null ? null : preAnalysisMarker.timeSpan());
            out.writeOptionalWriteable(dependencyResolutionMarker == null ? null : dependencyResolutionMarker.timeSpan());
            out.writeOptionalWriteable(analysisMarker == null ? null : analysisMarker.timeSpan());
        }
        if (out.getTransportVersion().supports(EsqlExecutionInfo.EXECUTION_PROFILE_FORMAT_VERSION)) {
            out.writeVInt(fieldCapsCalls.get());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        EsqlQueryProfile that = (EsqlQueryProfile) o;
        return Objects.equals(totalMarker, that.totalMarker)
            && Objects.equals(planningMarker, that.planningMarker)
            && Objects.equals(parsingMarker, that.parsingMarker)
            && Objects.equals(preAnalysisMarker, that.preAnalysisMarker)
            && Objects.equals(dependencyResolutionMarker, that.dependencyResolutionMarker)
            && Objects.equals(analysisMarker, that.analysisMarker)
            && Objects.equals(fieldCapsCalls.get(), that.fieldCapsCalls.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalMarker,
            planningMarker,
            parsingMarker,
            preAnalysisMarker,
            dependencyResolutionMarker,
            analysisMarker,
            fieldCapsCalls.get()
        );
    }

    @Override
    public String toString() {
        return "PlanningProfile{"
            + "totalMarker="
            + totalMarker
            + "planningMarker="
            + planningMarker
            + ", parsingMarker="
            + parsingMarker
            + ", preAnalysisMarker="
            + preAnalysisMarker
            + ", dependencyResolutionMarker="
            + dependencyResolutionMarker
            + ", analysisMarker="
            + analysisMarker
            + ", fieldCapsCalls="
            + fieldCapsCalls.get()
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
     * Span for the preanalysis phase
     */
    public TimeSpanMarker preAnalysis() {
        return preAnalysisMarker;
    }

    /**
     * Span for the dependency resolution phase - this includes field_caps, enrich policies and inference resolution IDs
     */
    public TimeSpanMarker dependencyResolution() {
        return dependencyResolutionMarker;
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

    public Collection<TimeSpanMarker> timeSpanMarkers() {
        return List.of(totalMarker, planningMarker, parsingMarker, preAnalysisMarker, dependencyResolutionMarker, analysisMarker);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimeSpanMarker timeSpanMarker : timeSpanMarkers()) {
            builder.field(timeSpanMarker.name(), timeSpanMarker.timeSpan());
        }
        builder.field("field_caps_calls", fieldCapsCalls.get());
        return builder;
    }

}
