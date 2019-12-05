/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.aggregatemetric;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class AggregateMetricFeatureSetUsage extends XPackFeatureSet.Usage {
    private final int fieldCount;

    public AggregateMetricFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.fieldCount = input.getVersion().onOrAfter(Version.V_8_0_0) ? input.readInt() : 0;
    }

    public AggregateMetricFeatureSetUsage(boolean available, boolean enabled, int fieldCount) {
        super(XPackField.AGGREGATE_METRIC, available, enabled);
        this.fieldCount = fieldCount;
    }

    int fieldCount() {
        return fieldCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeInt(fieldCount);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("field_count", fieldCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateMetricFeatureSetUsage that = (AggregateMetricFeatureSetUsage) o;
        return available == that.available && enabled == that.enabled && fieldCount == that.fieldCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, fieldCount);
    }
}
