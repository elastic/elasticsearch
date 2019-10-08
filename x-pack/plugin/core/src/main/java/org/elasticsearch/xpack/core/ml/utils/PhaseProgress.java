/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A class that describes a phase and its progress as a percentage
 */
public class PhaseProgress implements ToXContentObject, Writeable {

    public static final ParseField PHASE = new ParseField("phase");
    public static final ParseField PROGRESS_PERCENT = new ParseField("progress_percent");

    public static final ConstructingObjectParser<PhaseProgress, Void> PARSER = new ConstructingObjectParser<>("phase_progress",
        true, a -> new PhaseProgress((String) a[0], (int) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), PROGRESS_PERCENT);
    }

    private final String phase;
    private final int progressPercent;

    public PhaseProgress(String phase, int progressPercent) {
        this.phase = Objects.requireNonNull(phase);
        this.progressPercent = progressPercent;
    }

    public PhaseProgress(StreamInput in) throws IOException {
        phase = in.readString();
        progressPercent = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(phase);
        out.writeVInt(progressPercent);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PHASE.getPreferredName(), phase);
        builder.field(PROGRESS_PERCENT.getPreferredName(), progressPercent);
        builder.endObject();
        return builder;
    }

    public String getPhase() {
        return phase;
    }

    public int getProgressPercent() {
        return progressPercent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, progressPercent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhaseProgress that = (PhaseProgress) o;
        return Objects.equals(phase, that.phase) && progressPercent == that.progressPercent;
    }
}
