/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderConfiguration;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class MlAutoscalingDeciderConfiguration implements AutoscalingDeciderConfiguration {
    private static final String NAME = "ml";

    private static final int DEFAULT_MIN_NUM_NODES = 1;

    private static final TimeValue DEFAULT_ANOMALY_JOB_TIME_IN_QUEUE = TimeValue.ZERO;
    private static final TimeValue DEFAULT_ANALYSIS_JOB_TIME_IN_QUEUE = TimeValue.timeValueMinutes(5);

    private static final ParseField MIN_NUM_NODES = new ParseField("min_num_nodes");
    private static final ParseField ANOMALY_JOB_TIME_IN_QUEUE = new ParseField("anomaly_job_time_in_queue");
    private static final ParseField ANALYSIS_JOB_TIME_IN_QUEUE = new ParseField("analysis_job_time_in_queue");

    private static final ObjectParser<MlAutoscalingDeciderConfiguration.Builder, Void> PARSER = new ObjectParser<>(NAME,
        MlAutoscalingDeciderConfiguration.Builder::new);

    static {
        PARSER.declareInt(MlAutoscalingDeciderConfiguration.Builder::setMinNumNodes, MIN_NUM_NODES);
        PARSER.declareString(MlAutoscalingDeciderConfiguration.Builder::setAnomalyJobTimeInQueue, ANOMALY_JOB_TIME_IN_QUEUE);
        PARSER.declareString(MlAutoscalingDeciderConfiguration.Builder::setAnalysisJobTimeInQueue, ANALYSIS_JOB_TIME_IN_QUEUE);
    }

    public static MlAutoscalingDeciderConfiguration parse(final XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    private final int minNumNodes;
    private final TimeValue anomalyJobTimeInQueue;
    private final TimeValue analysisJobTimeInQueue;

    MlAutoscalingDeciderConfiguration(int minNumNodes, TimeValue anomalyJobTimeInQueue, TimeValue analysisJobTimeInQueue) {
        if (minNumNodes < 0) {
            throw new IllegalArgumentException("[" + MIN_NUM_NODES.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.minNumNodes = minNumNodes;
        this.anomalyJobTimeInQueue = ExceptionsHelper.requireNonNull(anomalyJobTimeInQueue, ANOMALY_JOB_TIME_IN_QUEUE.getPreferredName());
        this.analysisJobTimeInQueue = ExceptionsHelper.requireNonNull(analysisJobTimeInQueue,
            ANALYSIS_JOB_TIME_IN_QUEUE.getPreferredName());
    }

    public MlAutoscalingDeciderConfiguration(StreamInput in) throws IOException {
        minNumNodes = in.readVInt();
        anomalyJobTimeInQueue = in.readTimeValue();
        analysisJobTimeInQueue = in.readTimeValue();
    }

    public int getMinNumNodes() {
        return minNumNodes;
    }

    public TimeValue getAnomalyJobTimeInQueue() {
        return anomalyJobTimeInQueue;
    }

    public TimeValue getAnalysisJobTimeInQueue() {
        return analysisJobTimeInQueue;
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
        out.writeVInt(minNumNodes);
        out.writeTimeValue(anomalyJobTimeInQueue);
        out.writeTimeValue(analysisJobTimeInQueue);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MIN_NUM_NODES.getPreferredName(), minNumNodes);
        builder.field(ANOMALY_JOB_TIME_IN_QUEUE .getPreferredName(), anomalyJobTimeInQueue.getStringRep());
        builder.field(ANALYSIS_JOB_TIME_IN_QUEUE.getPreferredName(), analysisJobTimeInQueue.getStringRep());
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MlAutoscalingDeciderConfiguration that = (MlAutoscalingDeciderConfiguration) o;
        return minNumNodes == that.minNumNodes &&
            Objects.equals(anomalyJobTimeInQueue, that.anomalyJobTimeInQueue) &&
            Objects.equals(analysisJobTimeInQueue, that.analysisJobTimeInQueue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minNumNodes, anomalyJobTimeInQueue, analysisJobTimeInQueue);
    }

    public static class Builder {

        private int minNumNodes = DEFAULT_MIN_NUM_NODES;
        private TimeValue anomalyJobTimeInQueue = DEFAULT_ANOMALY_JOB_TIME_IN_QUEUE;
        private TimeValue analysisJobTimeInQueue = DEFAULT_ANALYSIS_JOB_TIME_IN_QUEUE;

        public Builder setMinNumNodes(int minNodes) {
            this.minNumNodes = minNodes;
            return this;
        }

        public Builder setAnomalyJobTimeInQueue(TimeValue anomalyJobTimeInQueue) {
            this.anomalyJobTimeInQueue = anomalyJobTimeInQueue;
            return this;
        }

        void setAnomalyJobTimeInQueue(String anomalyJobTimeInQueue) {
            this.setAnomalyJobTimeInQueue(TimeValue.parseTimeValue(anomalyJobTimeInQueue,
                DEFAULT_ANOMALY_JOB_TIME_IN_QUEUE,
                ANOMALY_JOB_TIME_IN_QUEUE.getPreferredName()));
        }

        public Builder setAnalysisJobTimeInQueue(TimeValue analysisJobTimeInQueue) {
            this.analysisJobTimeInQueue = analysisJobTimeInQueue;
            return this;
        }

        void setAnalysisJobTimeInQueue(String analysisJobTimeInQueue) {
            this.setAnalysisJobTimeInQueue(TimeValue.parseTimeValue(analysisJobTimeInQueue,
                DEFAULT_ANALYSIS_JOB_TIME_IN_QUEUE,
                ANALYSIS_JOB_TIME_IN_QUEUE.getPreferredName()));
        }

        public MlAutoscalingDeciderConfiguration build() {
            return new MlAutoscalingDeciderConfiguration(minNumNodes, anomalyJobTimeInQueue, analysisJobTimeInQueue);
        }
    }

}
