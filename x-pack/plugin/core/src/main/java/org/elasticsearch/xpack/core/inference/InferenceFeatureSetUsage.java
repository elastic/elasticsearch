/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.inference.usage.ModelStats;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class InferenceFeatureSetUsage extends XPackFeatureUsage {

    public static final InferenceFeatureSetUsage EMPTY = new InferenceFeatureSetUsage(List.of());

    private final Collection<ModelStats> modelStats;

    public InferenceFeatureSetUsage(Collection<ModelStats> modelStats) {
        super(XPackField.INFERENCE, true, true);
        this.modelStats = modelStats;
    }

    public InferenceFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.modelStats = in.readCollectionAsList(ModelStats::new);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.xContentList("models", modelStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(modelStats);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        InferenceFeatureSetUsage that = (InferenceFeatureSetUsage) o;
        return Objects.equals(modelStats, that.modelStats);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(modelStats);
    }

    Collection<ModelStats> modelStats() {
        return modelStats;
    }
}
