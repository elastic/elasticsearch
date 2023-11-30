/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Map;

public class InferenceFeatureSetUsage extends XPackFeatureSet.Usage {

    public static final String MODELS_FIELD = "models";

    private final Map<String, Object> stats;

    public InferenceFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> stats) {
        super(XPackField.INFERENCE, available, enabled);
        this.stats = stats;
    }

    public InferenceFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.stats = input.readMap();
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            builder.field(MODELS_FIELD, stats);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(stats);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_8_1;
    }
}
