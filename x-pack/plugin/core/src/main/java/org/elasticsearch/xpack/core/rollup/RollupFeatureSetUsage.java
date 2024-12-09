/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

public class RollupFeatureSetUsage extends XPackFeatureUsage {

    private final int numberOfRollupJobs;

    public RollupFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.numberOfRollupJobs = input.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? input.readVInt() : 0;
    }

    public RollupFeatureSetUsage(int numberOfRollupJobs) {
        super(XPackField.ROLLUP, true, true);
        this.numberOfRollupJobs = numberOfRollupJobs;
    }

    public int getNumberOfRollupJobs() {
        return numberOfRollupJobs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeVInt(numberOfRollupJobs);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("number_of_rollup_jobs", numberOfRollupJobs);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

}
