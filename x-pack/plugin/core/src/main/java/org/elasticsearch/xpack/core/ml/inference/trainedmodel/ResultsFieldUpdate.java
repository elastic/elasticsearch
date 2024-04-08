/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A config update that sets the results field only.
 * Supports any type of {@link InferenceConfig}
 */
public class ResultsFieldUpdate implements InferenceConfigUpdate {

    public static final String NAME = "field_update";

    private final String resultsField;

    public ResultsFieldUpdate(String resultsField) {
        this.resultsField = Objects.requireNonNull(resultsField);
    }

    public ResultsFieldUpdate(StreamInput in) throws IOException {
        resultsField = in.readString();
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return true;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate.Builder<? extends InferenceConfigUpdate.Builder<?, ?>, ? extends InferenceConfigUpdate> newBuilder() {
        return new Builder().setResultsField(resultsField);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_9_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultsFieldUpdate that = (ResultsFieldUpdate) o;
        return Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(resultsField);
    }

    public static class Builder implements InferenceConfigUpdate.Builder<Builder, ResultsFieldUpdate> {
        private String resultsField;

        @Override
        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        @Override
        public ResultsFieldUpdate build() {
            return new ResultsFieldUpdate(resultsField);
        }
    }
}
