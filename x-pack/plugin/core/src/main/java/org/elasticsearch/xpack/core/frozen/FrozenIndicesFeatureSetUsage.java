/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.frozen;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

@UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // Remove this: it is unused in v9 but needed for mixed v8/v9 clusters
public class FrozenIndicesFeatureSetUsage extends XPackFeatureUsage {

    private final int numberOfFrozenIndices;

    public FrozenIndicesFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfFrozenIndices = input.readVInt();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numberOfFrozenIndices);
    }

    public FrozenIndicesFeatureSetUsage(boolean available, boolean enabled, int numberOfFrozenIndices) {
        super(XPackField.FROZEN_INDICES, available, enabled);
        this.numberOfFrozenIndices = numberOfFrozenIndices;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", numberOfFrozenIndices);
    }

    public int getNumberOfFrozenIndices() {
        return numberOfFrozenIndices;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, numberOfFrozenIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FrozenIndicesFeatureSetUsage other = (FrozenIndicesFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(numberOfFrozenIndices, other.numberOfFrozenIndices);
    }
}
