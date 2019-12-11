/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class RegressionInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "regression";

    public RegressionInferenceResults(double value) {
        super(value);
    }

    public RegressionInferenceResults(StreamInput in) throws IOException {
        super(in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    XContentBuilder innerToXContent(XContentBuilder builder, Params params) {
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        RegressionInferenceResults that = (RegressionInferenceResults) object;
        return Objects.equals(value(), that.value());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value());
    }

    @Override
    public void writeResult(IngestDocument document, String resultField, InferenceConfig config) {
        ExceptionsHelper.requireNonNull(document, "document");
        ExceptionsHelper.requireNonNull(resultField, "resultField");
        document.setFieldValue(resultField, value());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
