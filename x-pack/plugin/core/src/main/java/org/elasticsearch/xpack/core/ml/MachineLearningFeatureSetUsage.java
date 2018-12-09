/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
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
    public static final String NODE_COUNT = "node_count";

    private final Map<String, Object> jobsUsage;
    private final Map<String, Object> datafeedsUsage;
    private final int nodeCount;

    public MachineLearningFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> jobsUsage,
                                          Map<String, Object> datafeedsUsage, int nodeCount) {
        super(XPackField.MACHINE_LEARNING, available, enabled);
        this.jobsUsage = Objects.requireNonNull(jobsUsage);
        this.datafeedsUsage = Objects.requireNonNull(datafeedsUsage);
        this.nodeCount = nodeCount;
    }

    public MachineLearningFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.jobsUsage = in.readMap();
        this.datafeedsUsage = in.readMap();
        if (in.getVersion().onOrAfter(Version.V_6_5_0)) {
            this.nodeCount = in.readInt();
        } else {
            this.nodeCount = -1;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(jobsUsage);
        out.writeMap(datafeedsUsage);
        if (out.getVersion().onOrAfter(Version.V_6_5_0)) {
            out.writeInt(nodeCount);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (jobsUsage != null) {
            builder.field(JOBS_FIELD, jobsUsage);
        }
        if (datafeedsUsage != null) {
            builder.field(DATAFEEDS_FIELD, datafeedsUsage);
        }
        if (nodeCount >= 0) {
            builder.field(NODE_COUNT, nodeCount);
        }
    }

}
