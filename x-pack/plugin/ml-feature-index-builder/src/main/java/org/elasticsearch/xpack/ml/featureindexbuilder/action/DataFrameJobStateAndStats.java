/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobState;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.DataFrameIndexerJobStats;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;

import java.io.IOException;
import java.util.Objects;

public class DataFrameJobStateAndStats implements Writeable, ToXContentObject {

    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField STATS_FIELD = new ParseField("stats");

    private final String id;
    private final FeatureIndexBuilderJobState jobState;
    private final DataFrameIndexerJobStats jobStats;

    public static final ConstructingObjectParser<DataFrameJobStateAndStats, Void> PARSER = new ConstructingObjectParser<>(
            GetDataFrameJobsAction.NAME,
            a -> new DataFrameJobStateAndStats((String) a[0], (FeatureIndexBuilderJobState) a[1], (DataFrameIndexerJobStats) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FeatureIndexBuilderJob.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), FeatureIndexBuilderJobState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerJobStats.fromXContent(p), STATS_FIELD);
    }

    public DataFrameJobStateAndStats(String id, FeatureIndexBuilderJobState state, DataFrameIndexerJobStats stats) {
        this.id = Objects.requireNonNull(id);
        this.jobState = Objects.requireNonNull(state);
        this.jobStats = Objects.requireNonNull(stats);
    }

    public DataFrameJobStateAndStats(StreamInput in) throws IOException {
        this.id = in.readString();
        this.jobState = new FeatureIndexBuilderJobState(in);
        this.jobStats = new DataFrameIndexerJobStats(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FeatureIndexBuilderJob.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), jobState);
        builder.field(STATS_FIELD.getPreferredName(), jobStats);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        jobState.writeTo(out);
        jobStats.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, jobState, jobStats);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameJobStateAndStats that = (DataFrameJobStateAndStats) other;

        return Objects.equals(this.id, that.id) && Objects.equals(this.jobState, that.jobState)
                && Objects.equals(this.jobStats, that.jobStats);
    }

    public String getId() {
        return id;
    }

    public DataFrameIndexerJobStats getJobStats() {
        return jobStats;
    }

    public FeatureIndexBuilderJobState getJobState() {
        return jobState;
    }
}
