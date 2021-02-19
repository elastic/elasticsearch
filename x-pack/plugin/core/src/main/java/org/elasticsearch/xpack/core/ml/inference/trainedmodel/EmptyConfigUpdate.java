/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class EmptyConfigUpdate implements InferenceConfigUpdate {

    public static final String NAME = "empty";

    public static Version minimumSupportedVersion() {
        return Version.V_7_9_0;
    }

    public EmptyConfigUpdate() {
    }

    public EmptyConfigUpdate(StreamInput in) {
    }

    @Override
    public String getResultsField() {
        return null;
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        return originalConfig;
    }

    @Override
    public InferenceConfig toConfig() {
        throw new UnsupportedOperationException("the empty config update cannot be rewritten");
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return true;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public boolean equals(Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return EmptyConfigUpdate.class.hashCode();
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, EmptyConfigUpdate> {

        @Override
        public Builder setResultsField(String resultsField) {
            return this;
        }

        public EmptyConfigUpdate build() {
            return new EmptyConfigUpdate();
        }
    }
}
