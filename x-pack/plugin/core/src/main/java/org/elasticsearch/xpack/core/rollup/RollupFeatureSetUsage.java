/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.TransportVersion;
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
        this.numberOfRollupJobs = input.readVInt();
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
        out.writeVInt(numberOfRollupJobs);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("number_of_rollup_jobs", numberOfRollupJobs);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }

}
