/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.frozen;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class FrozenIndicesFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numberOfFrozenIndices;

    public FrozenIndicesFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numberOfFrozenIndices = input.readVInt();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_4_0;
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
        return Objects.equals(available, other.available) &&
            Objects.equals(enabled, other.enabled) &&
            Objects.equals(numberOfFrozenIndices, other.numberOfFrozenIndices);
    }
}
