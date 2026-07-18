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
import java.util.Map;
import java.util.Objects;

public class InferenceFeatureSetUsage extends XPackFeatureUsage {

    public static final InferenceFeatureSetUsage EMPTY = new InferenceFeatureSetUsage(List.of(), Map.of());
    // Public so tests can access it
    public static final String MODELS_FIELD = "models";
    public static final String CONFIG_SIZES_FIELD = "config_sizes";

    private final Collection<ModelStats> modelStats;
    private final Map<String, Object> configSizes;

    public InferenceFeatureSetUsage(Collection<ModelStats> modelStats) {
        this(modelStats, Map.of());
    }

    public InferenceFeatureSetUsage(Collection<ModelStats> modelStats, Map<String, Object> configSizes) {
        super(XPackField.INFERENCE, true, true);
        this.modelStats = modelStats;
        this.configSizes = configSizes;
    }

    public InferenceFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.modelStats = in.readCollectionAsList(ModelStats::new);
        this.configSizes = Map.of();
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.xContentList(MODELS_FIELD, modelStats);
        if (configSizes.isEmpty() == false) {
            builder.field(CONFIG_SIZES_FIELD, configSizes);
        }
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
        return Objects.equals(modelStats, that.modelStats) && Objects.equals(configSizes, that.configSizes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelStats, configSizes);
    }

    Collection<ModelStats> modelStats() {
        return modelStats;
    }

    Map<String, Object> configSizes() {
        return configSizes;
    }
}
