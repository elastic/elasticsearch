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
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobState;

import java.io.IOException;
import java.util.Objects;

public class GetDataFrameJobSingleResponse implements Writeable, ToXContentObject {

    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField CONFIG_FIELD = new ParseField("config");

    private final FeatureIndexBuilderJobState jobState;
    private final FeatureIndexBuilderJobConfig jobConfig;

    public static final ConstructingObjectParser<GetDataFrameJobSingleResponse, Void> PARSER = new ConstructingObjectParser<>(
            GetDataFrameJobsAction.NAME,
            a -> new GetDataFrameJobSingleResponse((FeatureIndexBuilderJobState) a[0], (FeatureIndexBuilderJobConfig) a[1]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), FeatureIndexBuilderJobState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> FeatureIndexBuilderJobConfig.fromXContent(p, null),
                CONFIG_FIELD);
    }

    public GetDataFrameJobSingleResponse(FeatureIndexBuilderJobState state, FeatureIndexBuilderJobConfig config) {
        this.jobState = state;
        this.jobConfig = config;
    }

    public GetDataFrameJobSingleResponse(StreamInput in) throws IOException {
        this.jobState = new FeatureIndexBuilderJobState(in);
        this.jobConfig = new FeatureIndexBuilderJobConfig(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE_FIELD.getPreferredName(), jobState);
        builder.field(CONFIG_FIELD.getPreferredName(), jobConfig);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        jobState.writeTo(out);
        jobConfig.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobState, jobConfig);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        GetDataFrameJobSingleResponse that = (GetDataFrameJobSingleResponse) other;

        return Objects.equals(this.jobState, that.jobState) && Objects.equals(this.jobConfig, that.jobConfig);
    }

    public FeatureIndexBuilderJobConfig getJobConfig() {
        return jobConfig;
    }

    public FeatureIndexBuilderJobState getJobState() {
        return jobState;
    }
}
