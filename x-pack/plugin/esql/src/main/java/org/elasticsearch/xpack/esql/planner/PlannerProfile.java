/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stores profiling information about the query plan.  This can be the top level planning on the coordinating node, or the local
 * planning on the data nodes.
 */
public class PlannerProfile implements Writeable, ChunkedToXContentObject {

    public static final class RuleProfile implements Writeable, ChunkedToXContentObject {
        private final List<Long> runDurations;
        private int runsWithChanges;

        public RuleProfile() {
            this.runDurations = new ArrayList<>();
            this.runsWithChanges = 0;
        }

        public RuleProfile(List<Long> durationNanos, int runsWithChanges) {
            this.runDurations = durationNanos;
            this.runsWithChanges = runsWithChanges;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(runDurations, StreamOutput::writeLong);
            out.writeInt(runsWithChanges);
        }

        public RuleProfile readFrom(StreamInput in) throws IOException {
            List<Long> runDurations = in.readCollectionAsImmutableList(StreamInput::readLong);
            int runsWithChanges = in.readInt();
            return new RuleProfile(runDurations, runsWithChanges);
        }

        public void addRun(long runDuration, boolean hasChanges) {
            runDurations.add(runDuration);
            if (hasChanges) {
                runsWithChanges++;
            }
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            // NOCOMMIT
            return null;
        }

        // NOCOMMIT equals and hashcode, once the fields are stable
    }

    public static final PlannerProfile EMPTY = new PlannerProfile(false, "");

    private final boolean isLocalPlanning;
    private final String nodeName;

    private Map<String, Map<String, RuleProfile>> ruleProfiles;

    public PlannerProfile(boolean isLocalPlanning, String nodeName) {
        this.isLocalPlanning = isLocalPlanning;
        this.nodeName = nodeName;
        ruleProfiles = new HashMap<>();
    }

    public static PlannerProfile readFrom(StreamInput in) throws IOException {
        // NOCOMMIT - need to read back the profiles
        return new PlannerProfile(in.readBoolean(), in.readString());
    }

    public void recordRuleProfile(String batchName, String ruleName, long runDuration, boolean hasChanges) {
        ruleProfiles.computeIfAbsent(batchName, k -> new HashMap<>())
            .computeIfAbsent(ruleName, k -> new RuleProfile())
            .addRun(runDuration, hasChanges);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(isLocalPlanning);
        out.writeString(nodeName);
        // NOCOMMIT - need to write out the profile map
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // NOCOMMIT
        throw new UnsupportedOperationException();
    }
    // NOCOMMIT equals and hashcode, once the fields are stable
}
