/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderConfiguration;

import java.io.IOException;
import java.util.Objects;

public class MlAutoscalingDeciderConfiguration implements AutoscalingDeciderConfiguration {
    static final String NAME = "ml";

    private static final int DEFAULT_ANOMALY_JOBS_IN_QUEUE = 0;
    private static final int DEFAULT_ANALYTICS_JOBS_IN_QUEUE = 0;

    private static final ParseField NUM_ANOMALY_JOBS_IN_QUEUE = new ParseField("num_anomaly_jobs_in_queue");
    private static final ParseField NUM_ANALYTICS_JOBS_IN_QUEUE = new ParseField("num_analytics_jobs_in_queue");

    private static final ObjectParser<MlAutoscalingDeciderConfiguration.Builder, Void> PARSER = new ObjectParser<>(NAME,
        MlAutoscalingDeciderConfiguration.Builder::new);

    static {
        PARSER.declareInt(MlAutoscalingDeciderConfiguration.Builder::setNumAnomalyJobsInQueue, NUM_ANOMALY_JOBS_IN_QUEUE);
        PARSER.declareInt(MlAutoscalingDeciderConfiguration.Builder::setNumAnalyticsJobsInQueue, NUM_ANALYTICS_JOBS_IN_QUEUE);
    }

    public static MlAutoscalingDeciderConfiguration parse(final XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    private final int numAnomalyJobsInQueue;
    private final int numAnalyticsJobsInQueue;

    MlAutoscalingDeciderConfiguration(int numAnomalyJobsInQueue, int numAnalyticsJobsInQueue) {
        if (numAnomalyJobsInQueue < 0) {
            throw new IllegalArgumentException("[" + NUM_ANOMALY_JOBS_IN_QUEUE.getPreferredName() + "] must be non-negative");
        }
        if (numAnalyticsJobsInQueue < 0) {
            throw new IllegalArgumentException("[" + NUM_ANALYTICS_JOBS_IN_QUEUE.getPreferredName() + "] must be non-negative");
        }
        this.numAnalyticsJobsInQueue = numAnalyticsJobsInQueue;
        this.numAnomalyJobsInQueue = numAnomalyJobsInQueue;
    }

    public MlAutoscalingDeciderConfiguration(StreamInput in) throws IOException {
        numAnomalyJobsInQueue = in.readVInt();
        numAnalyticsJobsInQueue = in.readVInt();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numAnomalyJobsInQueue);
        out.writeVInt(numAnalyticsJobsInQueue);
    }

    public int getNumAnomalyJobsInQueue() {
        return numAnomalyJobsInQueue;
    }

    public int getNumAnalyticsJobsInQueue() {
        return numAnalyticsJobsInQueue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MlAutoscalingDeciderConfiguration that = (MlAutoscalingDeciderConfiguration) o;
        return numAnomalyJobsInQueue == that.numAnomalyJobsInQueue &&
            numAnalyticsJobsInQueue == that.numAnalyticsJobsInQueue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAnomalyJobsInQueue, numAnalyticsJobsInQueue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_ANOMALY_JOBS_IN_QUEUE .getPreferredName(), numAnomalyJobsInQueue);
        builder.field(NUM_ANALYTICS_JOBS_IN_QUEUE.getPreferredName(), numAnalyticsJobsInQueue);
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private int numAnomalyJobsInQueue = DEFAULT_ANOMALY_JOBS_IN_QUEUE;
        private int numAnalyticsJobsInQueue = DEFAULT_ANALYTICS_JOBS_IN_QUEUE;

        public Builder setNumAnomalyJobsInQueue(int numAnomalyJobsInQueue) {
            this.numAnomalyJobsInQueue = numAnomalyJobsInQueue;
            return this;
        }

        public Builder setNumAnalyticsJobsInQueue(int numAnalyticsJobsInQueue) {
            this.numAnalyticsJobsInQueue = numAnalyticsJobsInQueue;
            return this;
        }

        public MlAutoscalingDeciderConfiguration build() {
            return new MlAutoscalingDeciderConfiguration(numAnomalyJobsInQueue, numAnalyticsJobsInQueue);
        }
    }

}
