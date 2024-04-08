/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class MachineLearningFeatureSetUsage extends XPackFeatureSet.Usage {

    public static final String ALL = "_all";
    public static final String JOBS_FIELD = "jobs";
    public static final String DATAFEEDS_FIELD = "datafeeds";
    public static final String COUNT = "count";
    public static final String DETECTORS = "detectors";
    public static final String FORECASTS = "forecasts";
    public static final String MODEL_SIZE = "model_size";
    public static final String CREATED_BY = "created_by";
    public static final String NODE_COUNT = "node_count";
    public static final String DATA_FRAME_ANALYTICS_JOBS_FIELD = "data_frame_analytics_jobs";
    public static final String INFERENCE_FIELD = "inference";

    private final Map<String, Object> jobsUsage;
    private final Map<String, Object> datafeedsUsage;
    private final Map<String, Object> analyticsUsage;
    private final Map<String, Object> inferenceUsage;
    private final int nodeCount;

    public MachineLearningFeatureSetUsage(
        boolean available,
        boolean enabled,
        Map<String, Object> jobsUsage,
        Map<String, Object> datafeedsUsage,
        Map<String, Object> analyticsUsage,
        Map<String, Object> inferenceUsage,
        int nodeCount
    ) {
        super(XPackField.MACHINE_LEARNING, available, enabled);
        this.jobsUsage = Objects.requireNonNull(jobsUsage);
        this.datafeedsUsage = Objects.requireNonNull(datafeedsUsage);
        this.analyticsUsage = Objects.requireNonNull(analyticsUsage);
        this.inferenceUsage = Objects.requireNonNull(inferenceUsage);
        this.nodeCount = nodeCount;
    }

    public MachineLearningFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.jobsUsage = in.readGenericMap();
        this.datafeedsUsage = in.readGenericMap();
        this.analyticsUsage = in.readGenericMap();
        this.inferenceUsage = in.readGenericMap();
        this.nodeCount = in.readInt();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(jobsUsage);
        out.writeGenericMap(datafeedsUsage);
        out.writeGenericMap(analyticsUsage);
        out.writeGenericMap(inferenceUsage);
        out.writeInt(nodeCount);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field(JOBS_FIELD, jobsUsage);
        builder.field(DATAFEEDS_FIELD, datafeedsUsage);
        builder.field(DATA_FRAME_ANALYTICS_JOBS_FIELD, analyticsUsage);
        builder.field(INFERENCE_FIELD, inferenceUsage);
        if (nodeCount >= 0) {
            builder.field(NODE_COUNT, nodeCount);
        }
    }

}
