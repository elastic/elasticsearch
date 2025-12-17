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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Tracks profiling for the planning phase
 */
public class PlanningProfile implements Writeable, ToXContentFragment {

    public static final String PLANNING = "planning";
    public static final String PARSING = "parsing";
    public static final String PRE_ANALYSIS = "preanalysis";
    public static final String DEPENDENCY_RESOLUTION = "dependency_resolution";
    public static final String ANALYSIS = "analysis";

    /** Time elapsed since start of query to calling ComputeService.execute */
    private final transient TimeSpanMarker planningMarker;

    /** Time elapsed for query parsing */
    private final transient TimeSpanMarker parsingMarker;

    /** Time elapsed for index preanalysis, including lookup indices */
    private final transient TimeSpanMarker preAnalysisMarker;

    /** Time elapsed for checking dependencies (field_caps, enrich policies, inference ids) */
    private final transient TimeSpanMarker dependencyResolutionMarker;

    /** Time elapsed for plan analysis */
    private final transient TimeSpanMarker analysisMarker;

    private static final TransportVersion ESQL_QUERY_PLANNING_PROFILE = TransportVersion.fromName("esql_query_planning_profile");

    public PlanningProfile() {
        this(null, null, null, null, null);
    }

    // For testing
    PlanningProfile(TimeSpan planning, TimeSpan parsing, TimeSpan preAnalysis, TimeSpan dependencyResolution, TimeSpan analysis) {
        planningMarker = new TimeSpanMarker(PLANNING, false, planning);
        parsingMarker = new TimeSpanMarker(PARSING, false, parsing);
        preAnalysisMarker = new TimeSpanMarker(PRE_ANALYSIS, false, preAnalysis);
        dependencyResolutionMarker = new TimeSpanMarker(DEPENDENCY_RESOLUTION, true, dependencyResolution);
        analysisMarker = new TimeSpanMarker(ANALYSIS, true, analysis);
    }

    public static PlanningProfile readFrom(StreamInput in) throws IOException {
        TimeSpan planning = in.readOptionalWriteable(TimeSpan::readFrom);
        TimeSpan parsing = null, preAnalysis = null, dependencyResolution = null, analysis = null;
        if (in.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            parsing = in.readOptionalWriteable(TimeSpan::readFrom);
            preAnalysis = in.readOptionalWriteable(TimeSpan::readFrom);
            dependencyResolution = in.readOptionalWriteable(TimeSpan::readFrom);
            analysis = in.readOptionalWriteable(TimeSpan::readFrom);
        }
        return new PlanningProfile(planning, parsing, preAnalysis, dependencyResolution, analysis);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(planningMarker.timeSpan);
        if (out.getTransportVersion().supports(ESQL_QUERY_PLANNING_PROFILE)) {
            out.writeOptionalWriteable(parsingMarker == null ? null : parsingMarker.timeSpan);
            out.writeOptionalWriteable(preAnalysisMarker == null ? null : preAnalysisMarker.timeSpan);
            out.writeOptionalWriteable(dependencyResolutionMarker == null ? null : dependencyResolutionMarker.timeSpan);
            out.writeOptionalWriteable(analysisMarker == null ? null : analysisMarker.timeSpan);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PlanningProfile that = (PlanningProfile) o;
        return Objects.equals(planningMarker, that.planningMarker)
            && Objects.equals(parsingMarker, that.parsingMarker)
            && Objects.equals(preAnalysisMarker, that.preAnalysisMarker)
            && Objects.equals(dependencyResolutionMarker, that.dependencyResolutionMarker)
            && Objects.equals(analysisMarker, that.analysisMarker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planningMarker, parsingMarker, preAnalysisMarker, dependencyResolutionMarker, analysisMarker);
    }

    @Override
    public String toString() {
        return "PlanningProfile{"
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
            + '}';
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

    public Collection<TimeSpanMarker> timeSpanMarkers() {
        return List.of(planningMarker, parsingMarker, preAnalysisMarker, dependencyResolutionMarker, analysisMarker);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (TimeSpanMarker timeSpanMarker : timeSpanMarkers()) {
            builder.field(timeSpanMarker.name(), timeSpanMarker.timeSpan);
        }

        return builder;
    }

    public static class TimeSpanMarker {
        private TimeSpan timeSpan;
        private transient TimeSpan.Builder timeSpanBuilder;

        private final String name;
        private final boolean allowMultipleCalls;

        // Package private for testing
        TimeSpanMarker(String name, boolean allowMultipleCalls, TimeSpan timeSpan) {
            this.name = name;
            this.allowMultipleCalls = allowMultipleCalls;
            this.timeSpan = timeSpan;
        }

        public String name() {
            return name;
        }

        TimeSpan timeSpan() {
            return timeSpan;
        }

        public void start() {
            assert allowMultipleCalls || timeSpanBuilder == null : "start() should only be called once for " + name;
            if (timeSpanBuilder == null) {
                timeSpanBuilder = TimeSpan.start();
            }
        }

        public void stop() {
            assert timeSpanBuilder != null : "start() should have be called for " + name;
            assert allowMultipleCalls || timeSpan == null : "start() should only be called once for " + name;
            timeSpan = timeSpanBuilder.stop();
        }

        public TimeValue timeTook() {
            assert timeSpan != null : "start() should have been called for " + name;
            return timeSpan.toTimeValue();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            TimeSpanMarker that = (TimeSpanMarker) o;
            return allowMultipleCalls == that.allowMultipleCalls
                && Objects.equals(timeSpan, that.timeSpan)
                && Objects.equals(name, that.name);
            // Don't consider timeStampBuilders for equality
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeSpan, name, allowMultipleCalls);
        }

        @Override
        public String toString() {
            return "TimeSpanMarker{"
                + "name='"
                + name
                + '\''
                + ", timeSpan="
                + timeSpan
                + ", allowMultipleCalls="
                + allowMultipleCalls
                + '}';
        }
    }
}
