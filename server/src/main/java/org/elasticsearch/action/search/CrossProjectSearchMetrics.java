/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public class CrossProjectSearchMetrics implements Writeable, ToXContentFragment {
    private long planningPhaseTookTime;
    private long mergingPhaseTookTime;
    /*
     * Tracks the time taken from dispatching a request to a linked project to receiving a response (meaning, also includes the
     * network time).
     */
    private final Map<String, Long> perProjectRoundtripTime;

    public static final String CPS_PROFILE_FIELD = "cps_profile";
    public static final ParseField PLANNING_PHASE_TOOK_TIME_FIELD = new ParseField("planning_phase_took_time");
    public static final ParseField MERGING_PHASE_TOOK_TIME_FIELD = new ParseField("merging_phase_took_time");
    public static final String PROJECTS_ROUND_TRIP_TIME = "projects_round_trip_time";
    public static final String PROJECTS_NAME = "projects";

    public CrossProjectSearchMetrics() {
        this.planningPhaseTookTime = 0L;
        this.mergingPhaseTookTime = 0L;
        this.perProjectRoundtripTime = new HashMap<>();
    }

    public CrossProjectSearchMetrics(StreamInput in) throws IOException {
        this.planningPhaseTookTime = in.readLong();
        this.perProjectRoundtripTime = in.readMap(StreamInput::readLong);
        this.mergingPhaseTookTime = in.readLong();
    }

    public void trackPlanningPhaseTookTime(long planningPhaseTookTime) {
        this.planningPhaseTookTime = planningPhaseTookTime;
    }

    public void trackProjectRoundtripTime(String projectName, long projectTookTime) {
        if (projectName.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
            projectName = "_origin";
        }

        this.perProjectRoundtripTime.put(projectName, projectTookTime);
    }

    public void trackMergingPhaseTookTime(long mergingPhaseTookTime) {
        this.mergingPhaseTookTime = mergingPhaseTookTime;
    }

    public long getPlanningPhaseTookTime() {
        return planningPhaseTookTime;
    }

    public Map<String, Long> getProjectsRoundtripTime() {
        return perProjectRoundtripTime;
    }

    public long getMergingPhaseTookTime() {
        return mergingPhaseTookTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(CPS_PROFILE_FIELD);

        builder.field(PLANNING_PHASE_TOOK_TIME_FIELD.getPreferredName(), planningPhaseTookTime);
        builder.field(MERGING_PHASE_TOOK_TIME_FIELD.getPreferredName(), mergingPhaseTookTime);

        builder.startObject(PROJECTS_ROUND_TRIP_TIME).startArray(PROJECTS_NAME);

        TreeSet<String> sorted = new TreeSet<>(perProjectRoundtripTime.keySet());
        for (String projectName : sorted) {
            long projectTookTime = perProjectRoundtripTime.get(projectName);

            builder.startObject();
            builder.field(projectName, projectTookTime);
            builder.endObject();
        }

        builder.endArray().endObject();

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CrossProjectSearchMetrics other
            && other.planningPhaseTookTime == this.planningPhaseTookTime
            && other.mergingPhaseTookTime == this.mergingPhaseTookTime
            && other.perProjectRoundtripTime.equals(this.perProjectRoundtripTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planningPhaseTookTime, mergingPhaseTookTime, perProjectRoundtripTime);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(planningPhaseTookTime);
        out.writeMap(perProjectRoundtripTime, StreamOutput::writeLong);
        out.writeLong(mergingPhaseTookTime);
    }
}
