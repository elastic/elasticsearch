/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class RegressionInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "regression";

    private final String resultsField;

    public RegressionInferenceResults(double value, InferenceConfig config) {
        super(value);
        assert config instanceof RegressionConfig;
        RegressionConfig regressionConfig = (RegressionConfig)config;
        this.resultsField = regressionConfig.getResultsField();
    }

    public RegressionInferenceResults(StreamInput in) throws IOException {
        super(in.readDouble());
        this.resultsField = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(resultsField);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        RegressionInferenceResults that = (RegressionInferenceResults) object;
        return Objects.equals(value(), that.value()) && Objects.equals(this.resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value(), resultsField);
    }

    @Override
    public void writeResult(IngestDocument document, String parentResultField) {
        ExceptionsHelper.requireNonNull(document, "document");
        ExceptionsHelper.requireNonNull(parentResultField, "parentResultField");
        document.setFieldValue(parentResultField + "." + this.resultsField, value());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
