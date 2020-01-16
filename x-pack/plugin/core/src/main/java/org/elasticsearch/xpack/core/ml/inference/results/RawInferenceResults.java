/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.IngestDocument;

import java.io.IOException;
import java.util.Objects;

public class RawInferenceResults extends SingleValueInferenceResults {

    public static final String NAME = "raw";

    public RawInferenceResults(double value) {
        super(value);
    }

    public RawInferenceResults(StreamInput in) throws IOException {
        super(in.readDouble());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) { return true; }
        if (object == null || getClass() != object.getClass()) { return false; }
        RawInferenceResults that = (RawInferenceResults) object;
        return Objects.equals(value(), that.value());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value());
    }

    @Override
    public void writeResult(IngestDocument document, String parentResultField) {
        throw new UnsupportedOperationException("[raw] does not support writing inference results");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

}
