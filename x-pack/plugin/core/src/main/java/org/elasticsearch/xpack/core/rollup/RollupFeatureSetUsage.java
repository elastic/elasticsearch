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
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.ROLLUP_USAGE;

public class RollupFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numberOfRollupJobs;
    private final int numberOfRollupIndices;

    public RollupFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.numberOfRollupJobs = input.getTransportVersion().onOrAfter(ROLLUP_USAGE) ? input.readVInt() : 0;
        this.numberOfRollupIndices = input.getTransportVersion().onOrAfter(ROLLUP_USAGE) ? input.readVInt() : 0;
    }

    public RollupFeatureSetUsage(int numberOfRollupIndices, int numberOfRollupJobs) {
        super(XPackField.ROLLUP, true, true);
        this.numberOfRollupJobs = numberOfRollupJobs;
        this.numberOfRollupIndices = numberOfRollupIndices;
    }

    public int getNumberOfRollupJobs() {
        return numberOfRollupJobs;
    }

    public int getNumberOfRollupIndices() {
        return numberOfRollupIndices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ROLLUP_USAGE)) {
            out.writeVInt(numberOfRollupJobs);
        }
        if (out.getTransportVersion().onOrAfter(ROLLUP_USAGE)) {
            out.writeVInt(numberOfRollupIndices);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("number_of_rollup_jobs", numberOfRollupJobs);
        builder.field("number_of_rollup_indices", numberOfRollupIndices);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_0_0;
    }

}
